(async () => {
    const systemglobal = require('./config.json');
    if (process.env.SYSTEM_NAME && process.env.SYSTEM_NAME.trim().length > 0)
        systemglobal.system_name = process.env.SYSTEM_NAME.trim()
    const facilityName = 'Mugino-Ctrl';

    const md5 = require('md5');
    const cron = require('node-cron');
    const { spawn, exec } = require("child_process");
    const fs = require('fs');
    const path = require('path');
    const chokidar = require('chokidar');
    const fileType = require('detect-file-type');
    const sharp = require('sharp');
    const storageHandler = require('node-persist');
    const request = require('request').defaults({ encoding: null, jar: true });
    const {sqlPromiseSafe, sqlPromiseSimple} = require("./utils/sqlClient");
    const Logger = require('./utils/logSystem');
    const mqClient = require('./utils/mqAccess');
    const { DiscordSnowflake } = require('@sapphire/snowflake');
    const crypto = require('crypto');
    const globalRunKey = crypto.randomBytes(5).toString("hex");
    const Discord_CDN_Accepted_Files = ['jpg','jpeg','jfif','png','webp','gif'];

    console.log("Reading tags from database...");
    let exsitingTags = new Map();
    (await sqlPromiseSafe(`SELECT id, name FROM sequenzia_index_tags`)).rows.map(e => exsitingTags.set(e.name, e.id));
    console.log("Reading tags from model...");
    let modelTags = new Map();
    const _modelTags = (fs.readFileSync(path.join(systemglobal.deepbooru_model_path, './tags.txt'))).toString().trim().split('\n').map(line => line.trim());
    const _modelCategories = JSON.parse(fs.readFileSync(path.join(systemglobal.deepbooru_model_path, './categories.json')).toString());
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
    const activeFiles = new Map();
    let init = false;

    Logger.printLine("Init", "Mugino Orchestrator Server", "debug");
    const baseKeyName = `mugino.${systemglobal.system_name}.`

    const LocalQueue = storageHandler.create({
        dir: 'data/LocalQueue',
        stringify: JSON.stringify,
        parse: JSON.parse,
        encoding: 'utf8',
        logging: false,
        ttl: false,
        expiredInterval: 2 * 60 * 1000, // every 2 minutes the process will clean-up the expired cache
        forgiveParseErrors: false
    });
    LocalQueue.init((err) => {
        if (err) {
            Logger.printLine("LocalQueue", "Failed to initialize the local request storage", "error", err)
        } else {
            Logger.printLine("LocalQueue", "Initialized successfully the local request storage", "debug", err)
        }
    });

    const ruleSets = new Map();

    if (systemglobal.mq_mugino_in) {
        //const RateLimiter = require('limiter').RateLimiter;
        //const limiter = new RateLimiter(5, 5000);
        //const limiterlocal = new RateLimiter(1, 1000);
        //const limiterbacklog = new RateLimiter(5, 5000);
        const amqp = require('amqplib/callback_api');
        let amqpConn = null;

        if (process.env.MQ_HOST && process.env.MQ_HOST.trim().length > 0)
            systemglobal.mq_host = process.env.MQ_HOST.trim()
        if (process.env.RABBITMQ_DEFAULT_USER && process.env.RABBITMQ_DEFAULT_USER.trim().length > 0)
            systemglobal.mq_user = process.env.RABBITMQ_DEFAULT_USER.trim()
        if (process.env.RABBITMQ_DEFAULT_PASS && process.env.RABBITMQ_DEFAULT_PASS.trim().length > 0)
            systemglobal.mq_pass = process.env.RABBITMQ_DEFAULT_PASS.trim()

        const mq_host = `amqp://${systemglobal.mq_user}:${systemglobal.mq_pass}@${systemglobal.mq_host}/?heartbeat=60`
        const MQWorker1 = `${systemglobal.mq_mugino_in}`
        const MQWorker2 = `${MQWorker1}.priority`
        const MQWorker3 = `${MQWorker1}.backlog`

        if (systemglobal.mq_rules)
            systemglobal.mq_rules.map(async rule => { rule.channels.map(ch => { ruleSets.set(ch, rule) }) })

        function startWorker() {
            amqpConn.createChannel(function(err, ch) {
                if (closeOnErr(err)) return;
                ch.on("error", function(err) {
                    Logger.printLine("KanmiMQ", "Channel 1 Error (Remote)", "error", err)
                });
                ch.on("close", function() {
                    Logger.printLine("KanmiMQ", "Channel 1 Closed (Remote)", "critical")
                    start();
                });
                ch.prefetch(10);
                ch.assertQueue(MQWorker1, { durable: true }, function(err, _ok) {
                    if (closeOnErr(err)) return;
                    ch.consume(MQWorker1, processMsg, { noAck: false });
                    Logger.printLine("KanmiMQ", "Channel 1 Worker Ready (Remote)", "debug")
                });
                ch.assertExchange("kanmi.exchange", "direct", {}, function(err, _ok) {
                    if (closeOnErr(err)) return;
                    ch.bindQueue(MQWorker1, "kanmi.exchange", MQWorker1, [], function(err, _ok) {
                        if (closeOnErr(err)) return;
                        Logger.printLine("KanmiMQ", "Channel 1 Worker Bound to Exchange (Remote)", "debug")
                    })
                })
                function processMsg(msg) {
                    work(msg, 'normal', function(ok) {
                        try {
                            if (ok)
                                ch.ack(msg);
                            else
                                ch.reject(msg, true);
                        } catch (e) {
                            closeOnErr(e);
                        }
                    });
                }
            });
        }
        // Priority Requests
        function startWorker2() {
            amqpConn.createChannel(function(err, ch) {
                if (closeOnErr(err)) return;
                ch.on("error", function(err) {
                    Logger.printLine("KanmiMQ", "Channel 2 Error (Local)", "error", err)
                });
                ch.on("close", function() {
                    Logger.printLine("KanmiMQ", "Channel 2 Closed (Local)", "critical")
                    start();
                });
                ch.prefetch(1);
                ch.assertQueue(MQWorker2, { durable: true }, function(err, _ok) {
                    if (closeOnErr(err)) return;
                    ch.consume(MQWorker2, processMsg, { noAck: false });
                    Logger.printLine("KanmiMQ", "Channel 2 Worker Ready (Local)", "debug")
                });
                ch.assertExchange("kanmi.exchange", "direct", {}, function(err, _ok) {
                    if (closeOnErr(err)) return;
                    ch.bindQueue(MQWorker2, "kanmi.exchange", MQWorker2, [], function(err, _ok) {
                        if (closeOnErr(err)) return;
                        Logger.printLine("KanmiMQ", "Channel 2 Worker Bound to Exchange (Local)", "debug")
                    })
                })

                function processMsg(msg) {
                    work(msg, 'priority', function(ok) {
                        try {
                            if (ok)
                                ch.ack(msg);
                            else
                                ch.reject(msg, true);
                        } catch (e) {
                            closeOnErr(e);
                        }
                    });
                }
            });
        }
        // Backlog Requests
        function startWorker3() {
            amqpConn.createChannel(function(err, ch) {
                if (closeOnErr(err)) return;
                ch.on("error", function(err) {
                    Logger.printLine("KanmiMQ", "Channel 3 Error (Backlog)", "error", err)
                });
                ch.on("close", function() {
                    Logger.printLine("KanmiMQ", "Channel 3 Closed (Backlog)", "critical")
                    start();
                });
                ch.prefetch(5);
                ch.assertQueue(MQWorker3, { durable: true, queueMode: 'lazy'  }, function(err, _ok) {
                    if (closeOnErr(err)) return;
                    ch.consume(MQWorker3, processMsg, { noAck: false });
                    Logger.printLine("KanmiMQ", "Channel 3 Worker Ready (Backlog)", "debug")
                });
                ch.assertExchange("kanmi.exchange", "direct", {}, function(err, _ok) {
                    if (closeOnErr(err)) return;
                    ch.bindQueue(MQWorker3, "kanmi.exchange", MQWorker3, [], function(err, _ok) {
                        if (closeOnErr(err)) return;
                        Logger.printLine("KanmiMQ", "Channel 3 Worker Bound to Exchange (Backlog)", "debug")
                    })
                })
                function processMsg(msg) {
                    work(msg, 'backlog', function(ok) {
                        try {
                            if (ok)
                                ch.ack(msg);
                            else
                                ch.reject(msg, true);
                        } catch (e) {
                            closeOnErr(e);
                        }
                    });
                }
            });
        }
        async function work(raw, queue, cb) {
            try {
                let msg = JSON.parse(Buffer.from(raw.content).toString('utf-8'));
                const fileId = 'message-' + globalRunKey + '-' + DiscordSnowflake.generate();
                if (ruleSets.has(msg.messageChannelID) && msg.messageType === 'sfile' && msg.itemFileData && msg.itemFileName && ['jpg', 'jpeg', 'jfif', 'png'].indexOf(msg.itemFileName.split('.').pop().toLowerCase()) !== -1) {
                    Logger.printLine(`MessageProcessor`, `Process Message: (${queue}) From: ${msg.fromClient}, To Channel: ${msg.messageChannelID}`, "info");
                    LocalQueue.setItem(fileId, { id: fileId, queue, message: msg })
                        .then(async function () {
                            await sharp(new Buffer.from(msg.itemFileData))
                                .toFormat('png')
                                .toFile(path.join(systemglobal.deepbooru_input_path, `${fileId}.png`), (err, info) => {
                                    if (err) {
                                        Logger.printLine("SaveFile", `Error when saving the file ${fileId}`, "error", err)
                                        mqClient.sendData( `${systemglobal.mq_discord_out}${(queue !== 'normal') ? '.' + queue : ''}`, msg, function (ok) {
                                            cb(ok);
                                        });
                                    } else {
                                        cb(true);
                                    }
                                })
                        })
                        .catch(function (err) {
                            Logger.printLine(`MessageProcessor`, `Failed to set save message`, `error`, err)
                            cb(false);
                        })
                } else {
                    Logger.printLine(`MessageProcessor`, `Bypass Message: (${queue}) From: ${msg.fromClient}, To Channel: ${msg.messageChannelID}`, "debug");
                    mqClient.sendData( `${systemglobal.mq_discord_out}${(queue !== 'normal') ? '.' + queue : ''}`, msg, function (ok) {
                        cb(ok);
                    });
                }
            } catch (err) {
                Logger.printLine("JobParser", "Error Parsing Job - " + err.message, "critical")
                cb(false);
            }
        }
        function start() {
            amqp.connect(mq_host, function(err, conn) {
                if (err) {
                    Logger.printLine("KanmiMQ", "Initialization Error", "critical", err)
                    return setTimeout(start, 1000);
                }
                conn.on("error", function(err) {
                    if (err.message !== "Connection closing") {
                        Logger.printLine("KanmiMQ", "Initialization Connection Error", "emergency", err)
                    }
                });
                conn.on("close", function() {
                    Logger.printLine("KanmiMQ", "Attempting to Reconnect...", "debug")
                    return setTimeout(start, 1000);
                });
                Logger.printLine("KanmiMQ", `Connected to Kanmi Exchange as ${systemglobal.system_name}!`, "info")
                amqpConn = conn;
                whenConnected();
            });
        }
        function closeOnErr(err) {
            if (!err) return false;
            console.error(err)
            Logger.printLine("KanmiMQ", "Connection Closed due to error", "error", err)
            amqpConn.close();
            return true;
        }
        async function whenConnected() {
            if (systemglobal.Watchdog_Host && systemglobal.Watchdog_ID) {
                request.get(`http://${systemglobal.Watchdog_Host}/watchdog/init?id=${systemglobal.Watchdog_ID}&entity=${facilityName}-${systemglobal.system_name}`, async (err, res) => {
                    if (err || res && res.statusCode !== undefined && res.statusCode !== 200) {
                        console.error(`Failed to init watchdog server ${systemglobal.Watchdog_Host} as ${facilityName}:${systemglobal.Watchdog_ID}`);
                    }
                })
            }
            startWorker();
            startWorker2();
            startWorker3();
            if (process.send && typeof process.send === 'function')
                process.send('ready');
            init = true
        }

        start();
    }

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
    async function queryImageTags() {
        console.log('Processing image tags via MIITS Client...')
        return new Promise(async (resolve) => {
            const startTime = Date.now();
            (fs.readdirSync(systemglobal.deepbooru_input_path))
                .filter(e => fs.existsSync(path.join(systemglobal.deepbooru_output_path, `${e.split('.')[0]}.json`)) || (fs.statSync(path.resolve(systemglobal.deepbooru_input_path, e))).size <= 16 )
                .map(e => fs.unlinkSync(path.resolve(systemglobal.deepbooru_input_path, e)))
            if ((fs.readdirSync(systemglobal.deepbooru_input_path)).length > 0) {
                let ddOptions = ['evaluate', systemglobal.deepbooru_input_path, '--project-path', systemglobal.deepbooru_model_path, '--allow-folder', '--save-json', '--save-path', systemglobal.deepbooru_output_path, '--no-tag-output']
                if (systemglobal.deepbooru_gpu)
                    ddOptions.push('--allow-gpu')
                console.log(ddOptions.join(' '))
                const muginoMeltdown = spawn(((systemglobal.deepbooru_exec) ? systemglobal.deepbooru_exec : 'deepbooru'), ddOptions, { encoding: 'utf8' })

                if (!systemglobal.deepbooru_no_log)
                    muginoMeltdown.stdout.on('data', (data) => console.log(data.toString().trim().split('\n').filter(e => e.trim().length > 1 && !e.trim().includes('===] ')).join('\n')))
                muginoMeltdown.stderr.on('data', (data) => console.error(data.toString()));
                muginoMeltdown.on('close', (code, signal) => {
                    (fs.readdirSync(systemglobal.deepbooru_input_path))
                        .filter(e => fs.existsSync(path.join(systemglobal.deepbooru_output_path, `${e.split('.')[0]}.json`)))
                        .map(e => fs.unlinkSync(path.resolve(systemglobal.deepbooru_input_path, e)))
                    if (code !== 0) {
                        console.error(`Mugino Meltdown! MIITS reported a error during tagging operation!`);
                        resolve(false)
                    } else {
                        console.log(`Tagging Completed in ${((Date.now() - startTime) / 1000).toFixed(0)} sec!`);
                        resolve(true)
                    }
                })
            } else {
                console.info(`There are no file that need to be tagged!`);
                resolve(true)
            }
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
            'kanmi_records.tags IS NULL',
        ]
        const sqlWhereFiletypes = [
            "kanmi_records.attachment_name LIKE '%.jp%_'",
            "kanmi_records.attachment_name LIKE '%.jfif'",
            "kanmi_records.attachment_name LIKE '%.png'",
            "kanmi_records.attachment_name LIKE '%.gif'",
        ]
        let sqlWhereFilter = [];

        if (analyzerGroup && analyzerGroup.query) {
            sqlWhereFilter.push(analyzerGroup.query)
        } else {
            if (analyzerGroup && analyzerGroup.channels) {
                sqlWhereFilter.push('(' + analyzerGroup.channels.map(h => `kanmi_records.channel = '${h}'`).join(' OR ') + ')');
            }
            if (analyzerGroup && analyzerGroup.servers) {
                sqlWhereFilter.push('(' + analyzerGroup.servers.map(h => `kanmi_records.server = '${h}'`).join(' OR ') + ')');
            }
            if (analyzerGroup && analyzerGroup.content) {
                sqlWhereFilter.push('(' + analyzerGroup.content.map(h => `kanmi_records.content_full LIKE '%${h}%'`).join(' OR ') + ')');
            }

            if (analyzerGroup && analyzerGroup.parent) {
                sqlWhereFilter.push('(' + analyzerGroup.parent.map(h => `kanmi_channels.parent = '${h}'`).join(' OR ') + ')');
            }
            if (analyzerGroup && analyzerGroup.class) {
                sqlWhereFilter.push('(' + analyzerGroup.class.map(h => `kanmi_channels.classification = '${h}'`).join(' OR ') + ')');
            }
            if (analyzerGroup && analyzerGroup.vcid) {
                sqlWhereFilter.push('(' + analyzerGroup.vcid.map(h => `kanmi_channels.virtual_cid = '${h}'`).join(' OR ') + ')');
            }
        }

        const sqlOrderBy = (analyzerGroup && analyzerGroup.order) ? analyzerGroup.order :'eid DESC'
        const query = `SELECT ${sqlFields.join(', ')} FROM ${sqlTables.join(', ')} WHERE (${sqlWhereBase.join(' AND ')} AND (${sqlWhereFiletypes.join(' OR ')}))${(sqlWhereFilter.length > 0) ? ' AND (' + sqlWhereFilter.join(' AND ') + ')' : ''} ORDER BY ${sqlOrderBy} LIMIT ${(analyzerGroup && analyzerGroup.limit) ? analyzerGroup.limit : 100}`
        console.log(`Selecting data for analyzer group...`, analyzerGroup);

        const messages = (await sqlPromiseSafe(query, true)).rows.map(e => {
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
            const url = (( e.cache_proxy) ? e.cache_proxy.startsWith('http') ? e.cache_proxy : `https://${(systemglobal.no_media_cdn || (activeFiles.has(e.eid) && (activeFiles.get(e.eid)) >= 2)) ? 'cdn.discordapp.com' : 'media.discordapp.net'}/attachments${e.cache_proxy}${(systemglobal.no_media_cdn || (activeFiles.has(e.eid) && (activeFiles.get(e.eid)) >= 2)) ? '' : getimageSizeParam()}` : (e.attachment_hash && e.attachment_name) ? `https://${(systemglobal.no_media_cdn || (activeFiles.has(e.eid) && (activeFiles.get(e.eid)) >= 2)) ? 'cdn.discordapp.com' : 'media.discordapp.net'}/attachments/` + ((e.attachment_hash.includes('/')) ? e.attachment_hash : `${e.channel}/${e.attachment_hash}/${e.attachment_name}${(systemglobal.no_media_cdn || (activeFiles.has(e.eid) && (activeFiles.get(e.eid)) >= 2)) ? '' : getimageSizeParam()}`) : undefined) + '';
            return { url, ...e };
        })
        console.log(messages.length + ' items need to be tagged!')
        let downlaods = {}
        const existingFiles = [
            ...new Set([
                ...fs.readdirSync(systemglobal.deepbooru_input_path).map(e => e.split('.')[0]),
                ...fs.readdirSync(systemglobal.deepbooru_output_path).map(e => e.split('.')[0])
            ])
        ]
        messages.filter(e => existingFiles.indexOf(e.eid.toString()) === -1).map((e,i) => { downlaods[i] = e });
        if (messages.length === 0)
            return true;
        while (Object.keys(downlaods).length !== 0) {
            let downloadKeys = Object.keys(downlaods).slice(0,systemglobal.parallel_downloads || 25)
            console.log(`${Object.keys(downlaods).length} Left to download`)
            await Promise.all(downloadKeys.map(async k => {
                const e = downlaods[k];
                const results = await new Promise(ok => {

                    const url = e.url;
                    request.get({
                        url,
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
                            console.error(`Download failed: ${url}`, err);
                            ok(false)
                        } else {
                            try {
                                if (body && body.length > 100) {
                                    const mime = await new Promise(ext => {
                                        fileType.fromBuffer(body,function(err, result) {
                                            if (err) {
                                                console.error(`Failed to get MIME type for ${e.eid}`, err);
                                                ext(null);
                                            } else {
                                                ext(result);
                                            }
                                        });
                                    })
                                    if (systemglobal.allow_direct_write && mime.ext && ['png', 'jpg'].indexOf(mime.ext) !== -1) {
                                        fs.writeFileSync(path.join(systemglobal.deepbooru_input_path, `${e.eid}.${mime.ext}`), body);
                                        ok(true);
                                    } else if ((!systemglobal.allow_direct_write && mime.ext && ['png', 'jpg', 'gif', 'tiff', 'webp'].indexOf(mime.ext) !== -1) || (mime.ext && ['gif', 'tiff', 'webp'].indexOf(mime.ext) !== -1)) {
                                        await sharp(body)
                                            .toFormat('png')
                                            .toFile(path.join(systemglobal.deepbooru_input_path, `query-${e.eid}.png`), (err, info) => {
                                                if (err) {
                                                    console.error(`Failed to convert ${e.eid} to PNG file`, err);
                                                    ok(false);
                                                } else {
                                                    //console.log(`Downloaded as PNG ${e.url}`)
                                                    ok(true);
                                                }
                                            })
                                    } else {
                                        console.error('Unsupported file, will be discarded! Please consider correcting file name');
                                        ok(false);
                                    }
                                } else {
                                    console.error(`Download failed, File size to small: ${url}`);
                                    ok(false);
                                }
                            } catch (err) {
                                console.error(`Unexpected Error processing ${e.eid}!`, err);
                                console.error(e)
                                ok(false);
                            }
                        }
                        delete downlaods[k];
                    })
                })
                if (!results) {
                    if (activeFiles.has(e.eid)) {
                        let prev = activeFiles.get(e.eid)
                        if (prev <= 5) {
                            prev++;
                            activeFiles.set(e.eid, prev);
                        } else {
                            await sqlPromiseSafe(`UPDATE kanmi_records SET tags = ? WHERE eid = ?`, [ '3/1/cant_tag; ', e.eid ]);
                            console.error(`Failed to get data for ${e.eid} multiple times, it will be permanently skipped!`)
                        }
                    } else {
                        activeFiles.set(e.eid, 0);
                    }
                }
                return results;
            }))
        }
        return false;
    }
    async function parseResultsForMessage(key, results) {
        if (key && results) {
            return await new Promise(ok => {
                LocalQueue.getItem(key)
                    .then(data => {
                        if (data) {
                            const tags = Object.keys(results);
                            const rules = ruleSets.get(data.message.messageChannelID);
                            const result = (() => {
                                if (rules && rules.accept && tags.filter(t => (rules.accept.indexOf(t) !== -1)).length === 0) {
                                    console.error(`Did not find a approved tags "${tags.filter(t => rules.block.indexOf(t) !== -1).join(' ')}"`)
                                    return false;
                                }
                                if (rules && rules.block && tags.filter(t => (rules.block.indexOf(t) !== -1)).length > 0) {
                                    console.error(`Found a blocked tags "${tags.filter(t => rules.block.indexOf(t) !== -1).join(' ')}"`)
                                    return false;
                                }
                                return true;
                            })()
                            if (result) {
                                ok({
                                    destination: `${systemglobal.mq_discord_out}${(data.queue !== 'normal') ? '.' + data.queue : ''}`,
                                    message: {
                                        fromDPS: `return.${facilityName}.${systemglobal.system_name}`,
                                       ...data.message,
                                       messageTags: Object.keys(results).map(k => `${modelTags.get(k) || 0}/${parseFloat(results[k]).toFixed(4)}/${k}`).join('; ')
                                   }
                                });
                            } else {
                                ok(false);
                            }
                        } else {
                            console.error(`Unexpectedly Failed to get message data for key ${key}`)
                            ok(false);
                        }
                    })
                    .catch(err => {
                        console.error(`Unexpectedly Failed to get message for key ${key}`, err)
                        ok(false);
                    })
            })
        }
        return false;
    }
    const resultsWatcher = chokidar.watch(systemglobal.deepbooru_output_path, {
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
            if (filePath.split('/').pop().split('\\').pop().endsWith('.json') && filePath.split('/').pop().split('\\').pop().startsWith('query-')) {
                const eid = path.basename(filePath).split('query-').pop().split('.')[0];
                const jsonFilePath = path.resolve(filePath);
                const tagResults = JSON.parse(fs.readFileSync(jsonFilePath).toString());
                console.error(`Entity ${eid} has ${Object.keys(tagResults).length} tags!`);
                await sqlPromiseSafe(`UPDATE kanmi_records SET tags = ? WHERE eid = ?`, [ Object.keys(tagResults).map(k => `${modelTags.get(k) || 0}/${parseFloat(tagResults[k]).toFixed(4)}/${k}`).join('; '), eid ])
                Object.keys(tagResults).map(async k => {
                    const r = tagResults[k];
                    await addTagForEid(eid, k, r);
                });
                fs.unlinkSync(jsonFilePath);
                const imageFile = fs.readdirSync(systemglobal.deepbooru_input_path).filter(k => k.split('.')[0] === eid).pop();
                if (imageFile)
                    fs.unlinkSync(path.join(systemglobal.deepbooru_input_path, (imageFile)));
                activeFiles.delete(eid);
            } else if (filePath.split('/').pop().split('\\').pop().endsWith('.json') && filePath.split('/').pop().split('\\').pop().startsWith('message-')) {
                const key = path.basename(filePath).split('message-').pop().split('.')[0];
                const jsonFilePath = path.resolve(filePath);
                const tagResults = JSON.parse(fs.readFileSync(jsonFilePath).toString());
                console.error(`Message ${key} has ${Object.keys(tagResults).length} tags!`);
                const approved = await parseResultsForMessage(key, tagResults);
                if (approved) {
                    mqClient.sendData( `${approved.destination}`, approved.message, function (ok) { });
                    console.error(`Message ${key} was approved!`);
                } else { console.error(`Message ${key} was denied!`); }
                fs.unlinkSync(jsonFilePath);
                const imageFile = fs.readdirSync(systemglobal.deepbooru_input_path)
                    .filter(k => k.split('.')[0] === path.basename(filePath).split('.')[0]).pop();
                if (imageFile)
                    fs.unlinkSync(path.join(systemglobal.deepbooru_input_path, (imageFile)));
            }
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
                            console.log(`Searching for "${JSON.stringify(w)}"...`)
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

    await parseUntilDone(systemglobal.search);
    console.log("First pass completed!")
})()
