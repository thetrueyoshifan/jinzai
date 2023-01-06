(async () => {
    const config = require('./config.json');
    const md5 = require('md5');
    const cron = require('node-cron');
    const cheerio = require('cheerio');
    const fs = require('fs');
    const request = require('request').defaults({ encoding: null, jar: true });
    const {sqlPromiseSafe, sqlPromiseSimple} = require("./utils/sqlClient");
    const {sendData} = require("./utils/mqAccess");
    const Discord_CDN_Accepted_Files = ['jpg','jpeg','jfif','png','webp','gif'];

    let exsitingTags = new Map();
    (await sqlPromiseSafe(`SELECT id, name FROM sequenzia_index_tags`)).rows.map(e => exsitingTags.set(e.name, e.id));

    async function addTagForEid(eid, type = 0, name, rating = 0) {
        let tagId = 0;
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
    function updateTagsPairs(eid, tags) {
        return tags.map(tg => {
            const tag_type = ((t) => {
                switch (t) {
                    case 'General Tags':
                        return 1;
                    case 'Character Tags':
                        return 2;
                    case 'System Tags':
                        return 3;
                    default:
                        return 0;
                }
            })(tg.text)
            tg.tags.map(async tag => await addTagForEid(eid, tag_type, tag.name, tag.rating))
        })
    }
    async function parseResponse(eid, data) {
        const $ = cheerio.load(data);
        return Array.from($('.card-body > .row > div > table').children()).map((e,i,a) => {
            //console.log(e)
            if (e.name === 'thead') {
                let innerTags = [];
                $(a[i+1]).find('tr').each((fi, f) => {
                    let rating = 0;
                    let name = '';
                    $(f.children).each((gi, g) => {
                        if (g.children) {
                            if (g.children[0].type === 'text') {
                                rating = parseFloat(g.children[0].data.trim());
                            } else {
                                name = g.children[0].children[0].data.trim();
                            }
                        }
                    })
                    innerTags.push({ name, rating })
                })
                //console.log(innerTags)
                return {
                    text: $(e).find('th')[0].children[0].data.trim(),
                    tags: innerTags
                }
            }
        }).filter(e => !!e);
    }
    async function queryForTags() {
        const messages = (await sqlPromiseSafe(`SELECT attachment_name, channel, attachment_hash, eid, cache_proxy, sizeH, sizeW
                                                FROM kanmi_records
                                                WHERE attachment_hash IS NOT NULL
                                                  AND channel = ?
                                                  AND eid NOT IN (SELECT eid FROM sequenzia_index_matches)
                                                ORDER BY eid DESC
                                                LIMIT 5000`, [config.channel], true)).rows.map(e => {
            const url = (( e.cache_proxy) ? e.cache_proxy.startsWith('http') ? e.cache_proxy : `https://media.discordapp.net/attachments${e.cache_proxy}` : (e.attachment_hash && e.attachment_name) ? `https://media.discordapp.net/attachments/` + ((e.attachment_hash.includes('/')) ? e.attachment_hash : `${e.channel}/${e.attachment_hash}/${e.attachment_name}`) : undefined) + '';
            return {
                url,
                ...e
            }
        })
        //.\Downloads\DanbooruDownloader\DanbooruDownloader.exe dump --username konata_fan337 --api-key SFZ4391wUixCmx6EMovAQEyN
        console.log(messages)
        let msgRequests = messages.reduce((promiseChain, e, i, a) => {
            return promiseChain.then(() => new Promise(async(completed) => {
                const fileName = e.url.split('/').pop()
                const imageData = await new Promise(ok => {
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
                    }, function (err, res, body) {
                        if (err) {
                            ok(null)
                        } else {
                            console.log('Data!')
                            fs.writeFileSync(`./${fileName}`, body)
                            ok(true);
                        }
                    })
                })
                if (imageData) {
                    const requestResults = await new Promise(ok => {
                        const url = new URL(config.deepbooru_host);
                        console.log('Request Started...')
                        request.post({
                            url: `${config.deepbooru_host}/deepdanbooru/upload`,
                            headers: {
                                'Host': url.host,
                                'Origin': url.origin,
                                'Referer': url.origin + '/deepdanbooru',
                                "Content-Type": "multipart/form-data",
                                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                                'accept-language': 'en-US,en;q=0.9',
                                'cache-control': 'max-age=0',
                                'dnt': '1',
                                'upgrade-insecure-requests': '1',
                                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36 Edg/92.0.902.73'
                            },
                            formData : {
                                "network_type": "general",
                                "file" : fs.createReadStream(`./${fileName}`)
                            }
                        }, function (err, res, body) {
                            if (err) {
                                console.error(err)
                                ok(null)
                            } else {
                                if (res.statusCode === 302) {
                                    ok(res.rawHeaders.filter(e => e.startsWith(config.deepbooru_host))[0])
                                } else {
                                    console.log(body.toString())
                                    console.error("Did not get a redirect! Failed request!")
                                    ok(null)
                                }
                            }
                        })
                    })
                    if (requestResults) {
                        setTimeout(() => {completed(true);}, 750);
                        try { fs.unlinkSync(`./${fileName}`); }
                        catch (e) { console.error("Failed to delete temp file") }
                        const getResults = await new Promise(ok => {
                            const url = new URL(requestResults);
                            console.log('Request Completed, Getting Results')
                            request.get({
                                url: requestResults,
                                headers: {
                                    'Host': url.host,
                                    'Origin': url.origin,
                                    'Referer': url.origin + '/deepdanbooru/upload',
                                    "Content-Type": "multipart/form-data",
                                    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                                    'accept-language': 'en-US,en;q=0.9',
                                    'cache-control': 'max-age=0',
                                    'dnt': '1',
                                    'upgrade-insecure-requests': '1',
                                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36 Edg/92.0.902.73'
                                }
                            }, function (err, res, body) {
                                if (err) {
                                    console.error(err)
                                    ok(null)
                                } else {
                                    if (res.statusCode < 300) {
                                        ok(body)
                                    } else {
                                        console.log(body)
                                        console.error("Did not get a body! => " + requestResults)
                                        ok(null)
                                    }
                                }
                            })
                        })
                        if (getResults) {
                            //console.log("Saving last results!")
                            //fs.writeFileSync(`./results-${fileName}.html`, getResults);
                            const tags = await parseResponse(e.eid, getResults);
                            await updateTagsPairs(e.eid, tags);
                            console.log("Tags updated for " + e.eid)
                        } else {
                            console.error('Failed to get results from deepbooru')
                            completed(false);
                        }
                    } else {
                        console.error('Failed to request results from deepbooru')
                        completed(false);
                    }
                } else {
                    console.error('Failed to image data from discord server')
                    completed(false);
                }
            }))
        }, Promise.resolve());
        msgRequests.then(() => {
            console.log('Parsed Pending Messages!')
        })
    }
    //cron.schedule('45 * * * *', async () => { queryForTags(); });
    await queryForTags()
})()
