(async () => {
    const config = require('./config.json');
    const md5 = require('md5');
    const cron = require('node-cron');
    const { spawn, exec } = require("child_process");
    const fs = require('fs');
    const path = require('path');
    const chokidar = require('chokidar');
    const fileType = require('detect-file-type');
    const sharp = require('sharp');
    const request = require('request').defaults({ encoding: null, jar: true });
    const {sqlPromiseSafe, sqlPromiseSimple} = require("./utils/sqlClient");
    const Discord_CDN_Accepted_Files = ['jpg','jpeg','jfif','png','webp','gif'];

    console.log("Reading tags from database...");
    let exsitingTags = new Map();
    (await sqlPromiseSafe(`SELECT id, name FROM sequenzia_index_tags`)).rows.map(e => exsitingTags.set(e.name, e.id));
    console.log("Reading tags from model...");
    let modelTags = new Map();
    const _modelTags = (fs.readFileSync(path.join(config.deepbooru_model_path, './tags.txt'))).toString().trim().split('\n').map(line => line.trim());
    const _modelCategories = JSON.parse(fs.readFileSync(path.join(config.deepbooru_model_path, './categories.json')).toString());
    Object.values(_modelCategories).map((e,i,a) => {
        const c = ((n) => {
            switch (n) {
                case 'General':
                    return 1;
                case 'Character':
                    return 2;
                case 'System':
                    return 3;
                default:
                    return 0;
            }
        })(e.name);
        _modelTags.slice(e.start_index, ((i+1 !== a.length) ? a[i+1].start_index - 1 : undefined)).map(t => {
            modelTags.set(t, c)
        })
    })
    console.log(`Loaded ${modelTags.size} tags from model`);
    const badFiles = new Map();
    const skippedEid = [];


    async function clearFolder(folderPath) {
        try {
            const files = await fs.promises.readdir(folderPath);
            for (const file of files) {
                await fs.promises.unlink(path.resolve(folderPath, file));
                console.log(`${folderPath}/${file} has been removed successfully`);
            }
        } catch (err){
            console.log(err);
        }
    }
    //await clearFolder(config.deepbooru_input_path);
    // On-The-Fly Tagging System (aka no wasted table space)
    async function addTagForEid(eid, name, rating = 0) {
        let tagId = 0;
        const type = modelTags.get(name) || 0;
        if (!exsitingTags.has(name)) {
            await sqlPromiseSafe(`INSERT INTO sequenzia_index_tags SET name = ?, type = ?`, [name, type]);
            const newId = (await sqlPromiseSafe(`SELECT id, name FROM sequenzia_index_tags WHERE name = ?`, [name])).rows[0]
            tagId = newId.id;
            exsitingTags.set(name, tagId);
        } else {
            tagId = exsitingTags.get(name);
        }
        await sqlPromiseSafe(`INSERT INTO sequenzia_index_matches SET tag_pair = ?, eid = ?, tag = ?, rating = ? ON DUPLICATE KEY UPDATE rating = ?`, [
            parseInt(eid.toString() + tagId.toString()),
            eid, tagId, rating, rating
        ])
    }
    async function updateTagsPairs(eid, tags) {
        await sqlPromiseSafe(`UPDATE kanmi_records SET tags = ? WHERE eid = ?`, [ tags.map(t => `${modelTags.get(name) || 0}/${t.rating}/${t.name}`).join('; '), eid ])
        return await tags.map(async tag => await addTagForEid(eid, tag.name, tag.rating))
    }
    async function queryImageTags() {
        console.log('Processing images for tags...')
        return new Promise(async (resolve) => {
            const startTime = Date.now();
            (fs.readdirSync(config.deepbooru_input_path))
                .filter(e => fs.existsSync(path.join(config.deepbooru_output_path, `${e.split('.')[0]}.json`)))
                .map(e => fs.unlinkSync(path.resolve(config.deepbooru_input_path, e)))
            let ddOptions = ['evaluate', config.deepbooru_input_path, '--project-path', config.deepbooru_model_path, '--allow-folder', '--save-json', '--save-path', config.deepbooru_output_path, '--no-tag-output']
            if (config.deepbooru_gpu)
                ddOptions.push('--allow-gpu')
            console.log(ddOptions.join(' '))
            const muginoMeltdown = spawn(((config.deepbooru_exec) ? config.deepbooru_exec : 'deepbooru'), ddOptions, { encoding: 'utf8' })

            if (!config.deepbooru_no_log)
                muginoMeltdown.stdout.on('data', (data) => console.log(data.toString().trim().split('\n').filter(e => e.trim().length > 1 && !e.trim().includes('===] ')).join('\n')))
            muginoMeltdown.stderr.on('data', (data) => console.error(data.toString()));
            muginoMeltdown.on('close', (code, signal) => {
                (fs.readdirSync(config.deepbooru_input_path))
                    .filter(e => fs.existsSync(path.join(config.deepbooru_output_path, `${e.split('.')[0]}.json`)))
                    .map(e => fs.unlinkSync(path.resolve(config.deepbooru_input_path, e)))
                if (code !== 0) {
                    console.error(`Mugino Meltdown! MIITS reported a error!`);
                    resolve(false)
                } else {
                    console.log(`Tagging Completed in ${((Date.now() - startTime) / 1000).toFixed(0)} sec!`);
                    resolve(true)
                }
            })
        })
    }
    async function queryForTags(analyzerGroup) {
        const sqlFields = [
            'kanmi_records.eid',
            'kanmi_records.channel',
            'kanmi_records.attachment_name',
            'kanmi_records.attachment_hash',
            'kanmi_records.cache_proxy',
            'kanmi_records.sizeH',
            'kanmi_records.sizeW',
        ]
        const sqlTables = [
            'kanmi_records',
            'kanmi_channels',
        ]
        const sqlWhereBase = [
            'kanmi_records.channel = kanmi_channels.channelid',
            'kanmi_records.attachment_hash IS NOT NULL',
            'kanmi_records.attachment_name IS NOT NULL',
            'kanmi_records.attachment_extra IS NULL',
            'eid NOT IN (SELECT eid FROM sequenzia_index_matches)',
        ]
        const sqlWhereFiletypes = [
            "kanmi_records.attachment_name LIKE '%.jp%_'",
            "kanmi_records.attachment_name LIKE '%.jfif'",
            "kanmi_records.attachment_name LIKE '%.png'",
            "kanmi_records.attachment_name LIKE '%.gif'",
        ]
        let sqlWhereFilter = [];
        if (analyzerGroup && analyzerGroup.query) {
            sqlWhereFilter.push('analyzerGroup.query')
        } else {
            if (analyzerGroup && analyzerGroup.channels) {
                sqlWhereFilter.push('(' + analyzerGroup.channels.map(h => `kanmi_records.channel = ${h}`).join(' OR ') + ')');
            }
            if (analyzerGroup && analyzerGroup.servers) {
                sqlWhereFilter.push('(' + analyzerGroup.servers.map(h => `kanmi_records.server = ${h}`).join(' OR ') + ')');
            }
            if (analyzerGroup && analyzerGroup.content) {
                sqlWhereFilter.push('(' + analyzerGroup.content.map(h => `kanmi_records.content_full LIKE '%${h}%'`).join(' OR ') + ')');
            }

            if (analyzerGroup && analyzerGroup.parent) {
                sqlWhereFilter.push('(' + analyzerGroup.parent.map(h => `kanmi_channels.parent = ${h}`).join(' OR ') + ')');
            }
            if (analyzerGroup && analyzerGroup.class) {
                sqlWhereFilter.push('(' + analyzerGroup.class.map(h => `kanmi_channels.classification = ${h}`).join(' OR ') + ')');
            }
            if (analyzerGroup && analyzerGroup.vcid) {
                sqlWhereFilter.push('(' + analyzerGroup.vcid.map(h => `kanmi_channels.virtual_cid = ${h}`).join(' OR ') + ')');
            }
        }
        const sqlOrderBy = (analyzerGroup && analyzerGroup.order) ? analyzerGroup.order :'eid DESC'
        const query = `SELECT ${sqlFields.join(', ')} FROM ${sqlTables.join(', ')} WHERE (${sqlWhereBase.join(' AND ')} AND (${sqlWhereFiletypes.join(' OR ')}))${(sqlWhereFilter.length > 0) ? ' AND (' + sqlWhereFilter.join(' AND ') + ')' : ''}${(skippedEid.length > 0) ? ' AND (' + skippedEid.map(e => 'eid != ' + e).join(' AND ') + ')' : ''} ORDER BY ${sqlOrderBy} LIMIT ${(analyzerGroup && analyzerGroup.limit) ? analyzerGroup.limit : 100}`
        console.log(query);

        const messages = (await sqlPromiseSafe(query, true)).rows.map(e => {
            const url = (( e.cache_proxy) ? e.cache_proxy.startsWith('http') ? e.cache_proxy : `https://media.discordapp.net/attachments${e.cache_proxy}` : (e.attachment_hash && e.attachment_name) ? `https://media.discordapp.net/attachments/` + ((e.attachment_hash.includes('/')) ? e.attachment_hash : `${e.channel}/${e.attachment_hash}/${e.attachment_name}`) : undefined) + '';
            return {
                url,
                ...e
            }
        })
        console.log(messages.length + ' attachments to tag!')
        let downlaods = {}
        const existingFiles = [
            ...new Set([
                ...fs.readdirSync(config.deepbooru_input_path).map(e => e.split('.')[0]),
                ...fs.readdirSync(config.deepbooru_output_path).map(e => e.split('.')[0])
            ])
        ]
        messages.filter(e => existingFiles.indexOf(e.eid.toString()) === -1).map((e,i) => {
            downlaods[i] = e
        })
        if (messages.length === 0)
            return true;
        while (Object.keys(downlaods).length !== 0) {
            let downloadKeys = Object.keys(downlaods).slice(0,config.parallel_downloads || 25)
            console.log(`${Object.keys(downlaods).length} Left to download`)
            await Promise.all(downloadKeys.map(async k => {
                const e = downlaods[k];
                const results = await new Promise(ok => {
                    function getimageSizeParam() {
                        if (e.sizeH && e.sizeW && Discord_CDN_Accepted_Files.indexOf(e.attachment_name.split('.').pop().toLowerCase()) !== -1 && (e.sizeH > 512 || e.sizeW > 512)) {
                            let ih = 512;
                            let iw = 512;
                            if (e.sizeW >= e.sizeH) {
                                iw = (e.sizeW * (512 / e.sizeH)).toFixed(0)
                            } else {
                                ih = (e.sizeH * (512 / e.sizeW)).toFixed(0)
                            }
                            return `?width=${iw}&height=${ih}`
                        } else {
                            return ''
                        }
                    }
                    request.get({
                        url: e.url + getimageSizeParam(),
                        headers: {
                            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                            'accept-language': 'en-US,en;q=0.9',
                            'cache-control': 'max-age=0',
                            'sec-ch-ua': '"Chromium";v="92", " Not A;Brand";v="99", "Microsoft Edge";v="92"',
                            'sec-ch-ua-mobile': '?0',
                            'sec-fetch-dest': 'document',
                            'sec-fetch-mode': 'navigate',
                            'sec-fetch-site': 'none',
                            'sec-fetch-user': '?1',
                            'upgrade-insecure-requests': '1',
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36 Edg/92.0.902.73'
                        },
                    }, async function (err, res, body) {
                        if (err) {
                            ok(null)
                        } else {
                            try {
                                if (body && body.length > 100) {
                                    const mime = await new Promise(ext => {
                                        fileType.fromBuffer(body,function(err, result) {
                                            if (err) {
                                                console.log(err);
                                                ext(null);
                                            } else {
                                                ext(result);
                                            }
                                        });
                                    })
                                    if (mime.ext && ['png', 'jpg'].indexOf(mime.ext) !== -1) {
                                        fs.writeFileSync(path.join(config.deepbooru_input_path, `${e.eid}.${mime.ext}`), body);
                                        console.log(`Downloaded ${e.url}`)
                                        ok(true);
                                    } else if (mime.ext && ['gif', 'tiff', 'webp'].indexOf(mime.ext) !== -1) {
                                        await sharp(body)
                                            .toFormat('png')
                                            .toFile(path.join(config.deepbooru_input_path, `${e.eid}.png`), (err, info) => {
                                                if (err) {
                                                    console.error(err);
                                                    ok(false);
                                                } else {
                                                    console.log(`Downloaded as PNG ${e.url}`)
                                                    ok(true);
                                                }
                                            })
                                    } else {
                                        console.error('Unsupported file, will be discarded! Please consider correcting file name');
                                        ok(false);
                                    }
                                } else {
                                    console.error(`Download failed, File size to small: ${e.url}`);
                                    ok(false);
                                }
                            } catch (err) {
                                console.error(err);
                                ok(false);
                            }
                        }
                        delete downlaods[k];
                    })
                })
                if (!results) {
                    if (badFiles.has(e.eid)) {
                        let prev = badFiles.get(e.eid)
                        if (prev <= 5) {
                            prev++;
                            badFiles.set(e.eid, prev);
                        } else {
                            skippedEid.push(e.eid);
                        }
                    } else {
                        badFiles.set(e.eid, 0);
                    }
                }
                return results;
            }))
        }
        return false;
    }
    const resultsWatcher = chokidar.watch(config.deepbooru_output_path, {
        ignored: /[\/\\]\./,
        persistent: true,
        usePolling: true,
        awaitWriteFinish: {
            stabilityThreshold: 2000,
            pollInterval: 100
        },
        depth: 1,
        ignoreInitial: false
    });
    resultsWatcher
        .on('add', async function (filePath) {
            const eid = path.basename(filePath).split('.')[0];
            const jsonFilePath = path.resolve(filePath)
            const tagResults = JSON.parse(fs.readFileSync(jsonFilePath).toString());
            console.error(`Entity ${eid} has ${Object.keys(tagResults).length} tags!`);
            await sqlPromiseSafe(`UPDATE kanmi_records SET tags = ? WHERE eid = ?`, [ Object.keys(tagResults).map(k => `${modelTags.get(k) || 0}/${parseFloat(tagResults[k]).toFixed(4)}/${k}`).join('; '), eid ])
            Object.keys(tagResults).map(async k => {
                const r = tagResults[k];
                await addTagForEid(eid, k, r);
            });
            fs.unlinkSync(jsonFilePath);
            const imageFile = fs.readdirSync(config.deepbooru_input_path).filter(k => k.split('.')[0] === eid).pop();
            if (imageFile)
                fs.unlinkSync(path.join(config.deepbooru_input_path, (imageFile)));
        })
        .on('error', function (error) {
            console.error(error);
        })
        .on('ready', function () {
            console.log("MIITS Results Watcher Ready!")
        });

    let runTimer = null;
    async function parseUntilDone(analyzerGroups) {
        while (true) {
            let noResults = 0;
            if (analyzerGroups) {
                await new Promise(completed => {
                    let requests = analyzerGroups.reduce((promiseChain, w) => {
                        return promiseChain.then(() => new Promise(async (resolve) => {
                            console.log(`Searching for "${w}"...`)
                            const _r = await queryForTags(w);
                            if (_r)
                                noResults++;
                            resolve(true);
                        }))
                    }, Promise.resolve());
                    requests.then(async () => {
                        if (noResults !== analyzerGroups.length) {
                            console.log('Search Jobs Completed!, Starting MIITS Tagger...');
                            await queryImageTags();
                            console.log('MIITS Tagger finished!');
                        }
                        completed();
                    })
                })
            } else {
                const _r = await queryForTags();
                if (_r)
                    noResults++;
                console.log('Search Jobs Completed!, Starting MIITS Tagger...');
                await queryImageTags();
                console.log('MIITS Tagger finished!');
            }
            if ((analyzerGroups && noResults === analyzerGroups.length) || (!analyzerGroups && noResults === 1))
                break;
            console.log('More work to be done, no sleep!');
        }
        console.log('Waiting for next run... Zzzzz')
        runTimer = setTimeout(parseUntilDone, 300000);
    }

    await parseUntilDone(config.search);
    console.log("First pass completed!")
})()
