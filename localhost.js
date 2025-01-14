(async () => {
    const systemglobal = require('./config.json');
    if (process.env.SYSTEM_NAME && process.env.SYSTEM_NAME.trim().length > 0)
        systemglobal.system_name = process.env.SYSTEM_NAME.trim()
    const facilityName = 'MuginoMIITS';
    const sleep = (waitTimeInMs) => new Promise(resolve => setTimeout(resolve, waitTimeInMs));
    const md5 = require('md5');
    const cron = require('node-cron');
    const { spawn, exec } = require("child_process");
    const fs = require('fs');
    const path = require('path');
    const chokidar = require('chokidar');
    const fileType = require('detect-file-type');
    const sharp = require('sharp');
    const splitFile = require('split-file');
    const rimraf = require('rimraf');
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
    const UpscaleQueue = storageHandler.create({
        dir: 'data/UpscaleQueue',
        stringify: JSON.stringify,
        parse: JSON.parse,
        encoding: 'utf8',
        logging: false,
        ttl: false,
        expiredInterval: 2 * 60 * 1000, // every 2 minutes the process will clean-up the expired cache
        forgiveParseErrors: false
    });
    UpscaleQueue.init((err) => {
        if (err) {
            Logger.printLine("UpscaleQueue", "Failed to initialize the upscale request storage", "error", err)
        } else {
            Logger.printLine("UpscaleQueue", "Initialized successfully the upscale request storage", "debug", err)
        }
    });

    const ruleSets = new Map();

    let startEvaluating = null;
    let startUpscaleing = null;
    let gpuLocked = false;
    let upscaleIsActive = false;
    let mittsIsActive = false;
    let runTimer = null;

    if (systemglobal.Watchdog_Host && systemglobal.Watchdog_ID) {
        console.log(`Registering with Watchdog with ID "${systemglobal.Watchdog_ID}" as Entity "${facilityName}-${systemglobal.system_name}"`)
        request.get(`http://${systemglobal.Watchdog_Host}/watchdog/init?id=${systemglobal.Watchdog_ID}&entity=${facilityName}-${systemglobal.system_name}`, async (err, res) => {
            if (err || res && res.statusCode !== undefined && res.statusCode !== 200) {
                console.error(`Failed to init watchdog server ${systemglobal.Watchdog_Host} as ${facilityName}:${systemglobal.Watchdog_ID}`);
            }
        })
        setInterval(() => {
            request.get(`http://${systemglobal.Watchdog_Host}/watchdog/ping?id=${systemglobal.Watchdog_ID}&entity=${facilityName}-${systemglobal.system_name}`, async (err, res) => {
                if (err || res && res.statusCode !== undefined && res.statusCode !== 200) {
                    console.error(`Failed to ping watchdog server ${systemglobal.Watchdog_Host} as ${facilityName}:${systemglobal.Watchdog_ID}`);
                }
            })
        }, 60000)
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
                const imageFile = fs.readdirSync(systemglobal.deepbooru_input_path)
                    .filter(k => k.split('.')[0] === path.basename(filePath).split('.')[0]).pop();
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
                LocalQueue.removeItem(key);
            } else if ((filePath.split('/').pop().split('\\').pop().endsWith('.jpg') || filePath.split('/').pop().split('\\').pop().endsWith('.png')) && filePath.split('/').pop().split('\\').pop().startsWith('upscale-')) {
                const key = path.basename(filePath).split('upscale-').pop().split('.')[0];
                console.error(`Message ${key} has been upscaled!`);

                mqClient.sendData( `${approved.destination}`, approved.message, function (ok) { });
                fs.unlinkSync(filePath);
                const imageFile = fs.readdirSync(systemglobal.waifu2x_input_path)
                    .filter(k => k.split('.')[0] === path.basename(filePath).split('.')[0]).pop();
                if (imageFile)
                    fs.unlinkSync(path.join(systemglobal.waifu2x_input_path, (imageFile)));
                UpscaleQueue.removeItem(key);
            }
        })
        .on('error', function (error) {
            console.error(error);
        })
        .on('ready', function () {
            console.log("MIITS Results Watcher Ready!")
        });
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

        if (systemglobal.rules)
            systemglobal.rules.map(async rule => { rule.channels.map(ch => { ruleSets.set(ch, rule) }) })

        console.log(ruleSets.size + ' configured rules')

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
                const msg = JSON.parse(Buffer.from(raw.content).toString('utf-8'));
                const fileId = globalRunKey + '-' + DiscordSnowflake.generate();
                /*console.log({
                    ...msg,
                    itemFileData: (msg.itemFileData) ? 'true' : 'false'
                })*/

                if (msg.messageType === 'command' && msg.messageEID) {
                    Logger.printLine(`MessageProcessor`, `Command Message: (${queue}) Action: ${msg.messageAction}, From: ${msg.fromClient}, To Channel: ${msg.messageChannelID}`, "info");
                    switch (msg.messageAction) {
                        case 'Upscale':
                            db.safe(`SELECT x.*, y.data FROM (SELECT r.*, m.url, m.valid FROM (SELECT kanmi_records.* FROM kanmi_records WHERE kanmi_records.eid = ? AND kanmi_records.source = 0) r LEFT JOIN (SELECT url, valid, fileid FROM discord_multipart_files) m ON r.fileid = m.fileid) x LEFT OUTER JOIN (SELECT * FROM kanmi_records_extended) y ON (x.eid = y.eid)`, [MessageContents.messageEID], function (err, cacheresponse) {
                                if (err || cacheresponse.length === 0) {
                                    Logger.printLine("MPFDownload", `File not found!`, "error")
                                    cb(true)
                                } else if (cacheresponse[0].fileid && cacheresponse.filter(e => e.valid === 0 && !(!e.url)).length !== 0) {
                                    Logger.printLine("MPFDownload", `Failed to proccess the MultiPart File ${cacheresponse.real_filename} (${MessageContents.fileUUID})\nSome files are not valid and will need to be revalidated or repaired!`, "error")
                                    cb(true)
                                } else if (cacheresponse[0].fileid && cacheresponse.filter(e => e.valid === 1 && !(!e.url)).length !== cacheresponse[0].paritycount) {
                                    Logger.printLine("MPFDownload", `Failed to proccess the MultiPart File ${cacheresponse.real_filename} (${MessageContents.fileUUID})\nThe expected number of parity files were not available. \nTry to repair the parity cache \`juzo jfs repair parts\``, "error")
                                    cb(true)
                                } else if (cacheresponse[0].fileid && ['jpg', 'jpeg', 'jfif', 'png'].indexOf(cacheresponse[0].real_filename.split('.').pop().toLowerCase()) !== -1) {
                                    let itemsCompleted = [];
                                    const fileName = `upscale-${fileId}.${cacheresponse[0].real_filename.split('.').pop().toLowerCase()}`
                                    const CompleteFilename = path.join(systemglobal.waifu2x_input_path, fileName);
                                    const PartsFilePath = path.join(systemglobal.mpf_temp, `PARITY-${cacheresponse[0].fileid}`);
                                    fs.mkdirSync(PartsFilePath, {recursive: true})
                                    let requests = cacheresponse.filter(e => e.valid === 1 && !(!e.url)).map(e => e.url).sort((x, y) => (x.split('.').pop() < y.split('.').pop()) ? -1 : (y.split('.').pop() > x.split('.').pop()) ? 1 : 0).reduce((promiseChain, URLtoGet, URLIndex) => {
                                        return promiseChain.then(() => new Promise((resolve) => {
                                            const DestFilename = path.join(PartsFilePath, `${URLIndex}.par`)
                                            const stream = request.get({
                                                url: URLtoGet,
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
                                            }).pipe(fs.createWriteStream(DestFilename))
                                            // Write File to Temp Filesystem
                                            stream.on('finish', function () {
                                                Logger.printLine("MPFDownload", `Downloaded Part #${URLIndex} : ${DestFilename}`, "debug", {
                                                    URL: URLtoGet,
                                                    DestFilename: DestFilename,
                                                    CompleteFilename: fileName
                                                })
                                                itemsCompleted.push(DestFilename);
                                                resolve()
                                            });
                                            stream.on("error", function (err) {
                                                Logger.printLine("MPFDownload", `Part of the multipart file failed to download! ${URLtoGet}`, "err", "MPFDownload", "error", err)
                                                resolve()
                                            })
                                        }))
                                    }, Promise.resolve());
                                    requests.then(async () => {
                                        if (itemsCompleted.length === cacheresponse[0].paritycount) {
                                            await new Promise((deleted) => {
                                                rimraf(CompleteFilename, function (err) { deleted(!err) });
                                            })
                                            try {
                                                await splitFile.mergeFiles(itemsCompleted.sort(function (a, b) {
                                                    return a - b
                                                }), CompleteFilename)
                                                Logger.printLine("MPFDownload", `File "${fileName.replace(/[/\\?%*:|"<> ]/g, '_')}" was build successfully!`, "info")
                                                await new Promise((deleted) => {
                                                    rimraf(PartsFilePath, function (err) { deleted(!err) });
                                                })

                                                cb(true)
                                            } catch (err) {
                                                Logger.printLine("MPFDownload", `File ${cacheresponse[0].real_filename} failed to rebuild!`, "err", err)
                                                console.error(err)
                                                for (let part of itemsCompleted) {
                                                    fs.unlink(part, function (err) {
                                                        if (err && (err.code === 'EBUSY' || err.code === 'ENOENT')) {
                                                            //mqClient.sendMessage(`Error removing file part from temporary folder! - ${err.message}`, "err", "MPFDownload", err)
                                                        }
                                                    })
                                                }
                                                cb(true)
                                            }
                                        } else {
                                            Logger.printLine("MPFDownload", `Failed to proccess the MultiPart File ${fileId} (${MessageContents.fileUUID})\nThe expected number of parity files did not all download or save.`, "error")
                                            cb(true)
                                        }
                                    })
                                } else if (!cacheresponse[0].fileid && ['jpg', 'jpeg', 'jfif', 'png'].indexOf(cacheresponse[0].real_filename.split('.').pop().toLowerCase()) !== -1) {
                                    UpscaleQueue.setItem(fileId, { id: fileId, queue, message: msg })
                                        .then(async function () {
                                            const URLtoGet = (( cacheresponse[0].cache_proxy) ? cacheresponse[0].cache_proxy.startsWith('http') ? cacheresponse[0].cache_proxy : `https://cdn.discordapp.com/attachments${cacheresponse[0].cache_proxy}` : (cacheresponse[0].attachment_hash && cacheresponse[0].attachment_name) ? `https://cdn.discordapp.com/attachments/` + ((cacheresponse[0].attachment_hash.includes('/')) ? cacheresponse[0].attachment_hash : `${cacheresponse[0].channel}/${cacheresponse[0].attachment_hash}/${cacheresponse[0].attachment_name}`) : undefined) + ''
                                            const filePath = path.join(systemglobal.waifu2x_input_path, `upscale-${fileId}.${cacheresponse[0].real_filename.split('.').pop().toLowerCase()}`)
                                            const stream = request.get({
                                                url: URLtoGet,
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
                                            }).pipe(fs.createWriteStream(filePath))
                                            // Write File to Temp Filesystem
                                            stream.on('finish', async function () {
                                                cb(true);
                                                clearTimeout(startEvaluating);
                                                startEvaluating = null;
                                                startEvaluating = setTimeout(processGPUWorkloads, 60000)
                                            });
                                            stream.on("error", function (err) {
                                                Logger.printLine("MPFDownload", `File failed to download! ${URLtoGet}`, "error", err)
                                                cb(true)
                                            })
                                        })
                                        .catch(function (err) {
                                            Logger.printLine(`MessageProcessor`, `Failed to set save message`, `error`, err)
                                            cb(false);
                                        })
                                } else {
                                    Logger.printLine("MPFDownload", `File format is not supported!`, "error")
                                    cb(true)
                                }
                            })
                            break;
                        default:
                            cb(true);
                            break;
                    }
                } else if (msg.messageType === 'sfile' && msg.itemFileData && msg.itemFileName && ['jpg', 'jpeg', 'jfif', 'png'].indexOf(msg.itemFileName.split('.').pop().toLowerCase()) !== -1) {
                    Logger.printLine(`MessageProcessor`, `Process Message: (${queue}) From: ${msg.fromClient}, To Channel: ${msg.messageChannelID}`, "info");
                    LocalQueue.setItem(fileId, { id: fileId, queue, message: msg })
                        .then(async function () {
                            await sharp(new Buffer.from(msg.itemFileData, 'base64'))
                                .toFormat('png')
                                .toFile(path.join(systemglobal.deepbooru_input_path, `message-${fileId}.png`), (err, info) => {
                                    if (err) {
                                        Logger.printLine("SaveFile", `Error when saving the file ${fileId}`, "error")
                                        console.error(err);
                                        mqClient.sendData( `${systemglobal.mq_discord_out}${(queue !== 'normal') ? '.' + queue : ''}`, msg, function (ok) {
                                            cb(ok);
                                        });
                                    } else {
                                        cb(true);
                                        clearTimeout(startEvaluating);
                                        startEvaluating = null;
                                        startEvaluating = setTimeout(processGPUWorkloads, 60000)
                                    }
                                })
                        })
                        .catch(function (err) {
                            mqClient.sendData( `${systemglobal.mq_discord_out}${(queue !== 'normal') ? '.' + queue : ''}`, msg, function (ok) {
                                cb(ok);
                            });
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
            startWorker();
            startWorker2();
            startWorker3();
            if (process.send && typeof process.send === 'function')
                process.send('ready');
            init = true
        }

        await validateImageInputs();
        await processGPUWorkloads();
        start();
        if (systemglobal.search)
            await parseUntilDone(systemglobal.search);
        console.log("First pass completed!")
    } else {
        await validateImageInputs();
        await processGPUWorkloads();
        if (systemglobal.search)
            await parseUntilDone(systemglobal.search);
        console.log("First pass completed!")
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
    async function processGPUWorkloads() {
        if (startEvaluating) {
            clearTimeout(startEvaluating);
            startEvaluating = null;
        }
        if (!gpuLocked) {
            if (systemglobal.deepbooru_exec)
                await queryImageTags();
            if (systemglobal.waifu2x_exec)
                await upscaleImages();
        } else {
            return false;
        }
    }
    async function upscaleImages() {
        if (!upscaleIsActive) {
            upscaleIsActive = true;
            console.log('Processing image upscale via MIITS Client...')
            return new Promise(async (resolve) => {
                const startTime = Date.now();
                (fs.readdirSync(systemglobal.waifu2x_input_path))
                    .filter(e => fs.existsSync(path.join(systemglobal.deepbooru_output_path, `${e.split('.')[0]}.png`)) || fs.existsSync(path.join(systemglobal.deepbooru_output_path, `${e.split('.')[0]}.jpg`)) || (fs.statSync(path.resolve(systemglobal.waifu2x_input_path, e))).size <= 16)
                    .map(e => fs.unlinkSync(path.resolve(systemglobal.waifu2x_input_path, e)))
                if ((fs.readdirSync(systemglobal.waifu2x_input_path)).length > 0) {
                    await new Promise(completed => {
                        let requests = (fs.readdirSync(systemglobal.waifu2x_input_path)).reduce((promiseChain, e) => {
                            return promiseChain.then(() => new Promise(async (resolve) => {
                                const key = e.split('.')[0].split('upscale-').pop();
                                UpscaleQueue.getItem(key)
                                    .then(data => {
                                        if (data.parameters) {
                                            let w2xOptions = [
                                                (systemglobal.waifu2x_exec_input_parameter || '-i'),
                                                path.join(systemglobal.waifu2x_input_path, e),
                                                (systemglobal.waifu2x_exec_output_parameter || '-o'),
                                                path.join(systemglobal.deepbooru_output_path, `${e.split('.')[0]}.${systemglobal.waifu2x_exec_format_options[data.parameters.image_format || 0]}`),
                                                (systemglobal.waifu2x_exec_noise_parameter || '-n'),
                                                systemglobal.waifu2x_exec_noise_options[data.parameters.noise_level || 0],
                                                (systemglobal.waifu2x_exec_size_parameter || '-s'),
                                                systemglobal.waifu2x_exec_size_options[data.parameters.scale_level || 0],
                                                (systemglobal.waifu2x_exec_format_parameter || '-f'),
                                                systemglobal.waifu2x_exec_format_options[data.parameters.image_format || 0],
                                                ...(systemglobal.waifu2x_exec_additional_options || [])
                                            ]
                                            console.log(w2xOptions.join(' '))
                                            const muginoMeltdown = spawn(((systemglobal.waifu2x_exec) ? systemglobal.waifu2x_exec : 'waifu2x'), w2xOptions, {encoding: 'utf8'})
                                            if (!systemglobal.waifu2x_no_log)
                                                muginoMeltdown.stdout.on('data', (data) => console.log(data.toString().trim().split('\n').filter(e => e.trim().length > 1 && !e.trim().includes('===] ')).join('\n')))
                                            muginoMeltdown.stderr.on('data', (data) => console.error(data.toString()));
                                            muginoMeltdown.on('close', (code, signal) => {
                                                (fs.readdirSync(systemglobal.waifu2x_input_path))
                                                    .filter(e => fs.existsSync(path.join(systemglobal.deepbooru_output_path, `${e.split('.')[0]}.jpg`)) || fs.existsSync(path.join(systemglobal.deepbooru_output_path, `${e.split('.')[0]}.png`)))
                                                    .map(e => fs.unlinkSync(path.resolve(systemglobal.waifu2x_input_path, e)))
                                                if (code !== 0) {
                                                    console.error(`Mugino Meltdown! MIITS Upscaler reported a error during upscale operation!`);
                                                    upscaleIsActive = false;
                                                    resolve(false)
                                                } else {
                                                    console.log(`Tagging Completed in ${((Date.now() - startTime) / 1000).toFixed(0)} sec!`);
                                                    upscaleIsActive = false;
                                                    resolve(true)
                                                }
                                            })
                                        } else {
                                            console.error(`Unexpectedly Failed to get message data for key ${key}`)
                                            upscaleIsActive = false;
                                            resolve(true);
                                        }
                                    })
                                    .catch(err => {
                                        console.error(`Unexpectedly Failed to get message for key ${key}`, err)
                                        upscaleIsActive = false;
                                        resolve(true);
                                    })
                            }))
                        }, Promise.resolve());
                        requests.then(async () => {
                            completed();
                        })
                    })
                } else {
                    console.info(`There are no file that need to be upscaled!`);
                    mittsIsActive = false;
                    resolve(true)
                }
            })
        } else {
            return true;
        }
    }
    async function queryImageTags() {
        if (!mittsIsActive) {
            mittsIsActive = true;
            console.log('Processing image tags via MIITS Client...')
            return new Promise(async (resolve) => {
                const startTime = Date.now();
                (fs.readdirSync(systemglobal.deepbooru_input_path))
                    .filter(e => fs.existsSync(path.join(systemglobal.deepbooru_output_path, `${e.split('.')[0]}.json`)) || (fs.statSync(path.resolve(systemglobal.deepbooru_input_path, e))).size <= 16)
                    .map(e => fs.unlinkSync(path.resolve(systemglobal.deepbooru_input_path, e)))
                if ((fs.readdirSync(systemglobal.deepbooru_input_path)).length > 0) {
                    let ddOptions = ['evaluate', systemglobal.deepbooru_input_path, '--project-path', systemglobal.deepbooru_model_path, '--allow-folder', '--save-json', '--save-path', systemglobal.deepbooru_output_path, '--no-tag-output']
                    if (systemglobal.deepbooru_gpu)
                        ddOptions.push('--allow-gpu')
                    console.log(ddOptions.join(' '))
                    const muginoMeltdown = spawn(((systemglobal.deepbooru_exec) ? systemglobal.deepbooru_exec : 'deepbooru'), ddOptions, {encoding: 'utf8'})

                    if (!systemglobal.deepbooru_no_log)
                        muginoMeltdown.stdout.on('data', (data) => console.log(data.toString().trim().split('\n').filter(e => e.trim().length > 1 && !e.trim().includes('===] ')).join('\n')))
                    muginoMeltdown.stderr.on('data', (data) => console.error(data.toString()));
                    muginoMeltdown.on('close', (code, signal) => {
                        (fs.readdirSync(systemglobal.deepbooru_input_path))
                            .filter(e => fs.existsSync(path.join(systemglobal.deepbooru_output_path, `${e.split('.')[0]}.json`)))
                            .map(e => fs.unlinkSync(path.resolve(systemglobal.deepbooru_input_path, e)))
                        if (code !== 0) {
                            console.error(`Mugino Meltdown! MIITS reported a error during tagging operation!`);
                            mittsIsActive = false;
                            resolve(false)
                        } else {
                            console.log(`Tagging Completed in ${((Date.now() - startTime) / 1000).toFixed(0)} sec!`);
                            mittsIsActive = false;
                            resolve(true)
                        }
                    })
                } else {
                    console.info(`There are no file that need to be tagged!`);
                    mittsIsActive = false;
                    resolve(true)
                }
            })
        } else {
            return true;
        }
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
                        if (data.message) {
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
    async function validateImageInputs() {
        const imageFile = fs.readdirSync(systemglobal.deepbooru_input_path).map(async e => {
            try {
                const image = await sharp(fs.readFileSync(path.join(systemglobal.deepbooru_input_path, e))).metadata();
            } catch (err) {
                console.error(err);

            }
        })

    }



    async function parseUntilDone(analyzerGroups) {
        while (true) {
            let noResults = 0;
            if (analyzerGroups) {
                await new Promise(completed => {
                    let requests = analyzerGroups.reduce((promiseChain, w) => {
                        return promiseChain.then(() => new Promise(async (resolve) => {
                            const _r = await queryForTags(w);
                            if (_r)
                                noResults++;
                            resolve(true);
                        }))
                    }, Promise.resolve());
                    requests.then(async () => {
                        if (noResults !== analyzerGroups.length) {
                            console.log('Search Jobs Completed!, Starting MIITS Tagger...');
                            clearTimeout(startEvaluating);
                            startEvaluating = null;
                            startEvaluating = setTimeout(processGPUWorkloads, 3000)
                            await sleep(5000);
                            while (mittsIsActive) {
                                await sleep(5000);
                            }
                            console.log('MIITS Tagger finished!');
                            completed();
                        } else {
                            completed();
                        }
                    })
                })
            } else {
                const _r = await queryForTags();
                if (_r)
                    noResults++;
                console.log('Search Jobs Completed!, Starting MIITS Tagger...');
                clearTimeout(startEvaluating);
                startEvaluating = null;
                startEvaluating = setTimeout(processGPUWorkloads, 3000)
                await sleep(5000);
                while (mittsIsActive) {
                    await sleep(5000);
                }
                console.log('MIITS Tagger finished!');
            }
            if ((analyzerGroups && noResults === analyzerGroups.length) || (!analyzerGroups && noResults === 1))
                break;
            console.log('More work to be done, waiting for sync...!');
            await new Promise(done => setTimeout(() => { done(true) }, 60000))
        }
        console.log('Waiting for next run... Zzzzz')
        runTimer = setTimeout(parseUntilDone, 300000);
    }


})()
