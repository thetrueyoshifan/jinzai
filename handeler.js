(async () => {
    let systemglobal = require('./config.json');
    if (process.env.SYSTEM_NAME && process.env.SYSTEM_NAME.trim().length > 0)
        systemglobal.SystemName = process.env.SYSTEM_NAME.trim()
    const facilityName = 'Mugino-Ctrl';

    const chokidar = require('chokidar');
    const RateLimiter = require('limiter').RateLimiter;
    const limiter = new RateLimiter(5, 5000);
    const limiterlocal = new RateLimiter(1, 1000);
    const limiterbacklog = new RateLimiter(5, 5000);
    const amqp = require('amqplib/callback_api');
    let amqpConn = null;
    const request = require('request');
    //const sizeOf = require('image-size');
    //const rimraf = require('rimraf');
    //const ExifImage = require('exif').ExifImage;
    //const moment = require('moment');
    //let args = minimist(process.argv.slice(2));
    let init = false;

    const { fileSize } = require('./utils/tools');
    const Logger = require('./utils/logSystem');
    const redis = require('./utils/redisClient');
    const { sqlPromiseSafe } = require('./utils/sqlClient');

    Logger.printLine("Init", "Mugino Orchestrator Server", "debug")

    if (process.env.MQ_HOST && process.env.MQ_HOST.trim().length > 0)
        systemglobal.MQServer = process.env.MQ_HOST.trim()
    if (process.env.RABBITMQ_DEFAULT_USER && process.env.RABBITMQ_DEFAULT_USER.trim().length > 0)
        systemglobal.MQUsername = process.env.RABBITMQ_DEFAULT_USER.trim()
    if (process.env.RABBITMQ_DEFAULT_PASS && process.env.RABBITMQ_DEFAULT_PASS.trim().length > 0)
        systemglobal.MQPassword = process.env.RABBITMQ_DEFAULT_PASS.trim()

    const MQServer = `amqp://${systemglobal.MQUsername}:${systemglobal.MQPassword}@${systemglobal.MQServer}/?heartbeat=60`
    const MQWorker1 = `${systemglobal.Mugino_In}`
    const MQWorker2 = `${MQWorker1}.priority`
    const MQWorker3 = `${MQWorker1}.backlog`
    const baseKeyName = `mugino.${systemglobal.SystemName}.`

    const requestQueue     = new Map();
    const requestQueueMeta = new Map();

    let processTimer = null;
    let processLastRun = null;

    function shuffleArray(array) {
        let currentIndex = array.length,  randomIndex;

        // While there remain elements to shuffle.
        while (currentIndex != 0) {

            // Pick a remaining element.
            randomIndex = Math.floor(Math.random() * currentIndex);
            currentIndex--;

            // And swap it with the current element.
            [array[currentIndex], array[randomIndex]] = [
                array[randomIndex], array[currentIndex]];
        }

        return array;
    }

    // Set up the MQs for the following
    // * Pre-Filters for Discord messages
    // Redis
    // * Save all request messages to redis
    // Normal Requests
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
    async function work(msg, queue, cb) {
        try {
            const key = `${baseKeyName}.request.${queue}-${crypto.randomBytes(64).toString("hex")}`
            await redis.redisStore(key, Buffer.from(msg.content).toString('utf-8'));
            if (processTimer !== null || processLastRun === null) {
                clearTimeout(processTimer);
                processTimer = setTimeout(processPendingRequests, systemglobal.Mugino_Ingest_Delay || 150000);
                if (processLastRun === null)
                    processLastRun = Date.now();
            }
            cb(true);
        } catch (err) {
            Logger.printLine("JobParser", "Error Parsing Job - " + err.message, "critical")
            cb(false);
        }
    }
    function start() {
        amqp.connect(MQServer, function(err, conn) {
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
            Logger.printLine("KanmiMQ", `Connected to Kanmi Exchange as ${systemglobal.SystemName}!`, "info")
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
            request.get(`http://${systemglobal.Watchdog_Host}/watchdog/init?id=${systemglobal.Watchdog_ID}&entity=${facilityName}-${systemglobal.SystemName}`, async (err, res) => {
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

    // Interval to scan for items that need tags


    // Interval to scan for requests that have not been processed
    // Move to another processor and delist processor

    // Setup REST API server for Mugino Works to connect to
    // * /processor/register/ - Initialize GPU registration
    // * /processor/requests/ - Send Array of requests
    // * /processor/accept/ - Send Array of requests to accept
    // * /processor/results/ - Response from GPU with results

    // Process inbound messages
    // Move to Redis
    // Start/Reset Timer

    // Timeout to start processing inbound messages
    // Break up requests by number of processors
    // Send data to holding Q for processors
    async function processPendingRequests() {
        clearTimeout(processTimer);
        processTimer = null;
        const keys = await redis.redisSearch(`${baseKeyName}.request.`);
        const priority = keys.filter(k => k.startsWith(`${baseKeyName}.request.priority-`)).slice(0, systemglobal.Mugino_Priority_Max_Ingest || 100)
        const normal = keys.filter(k => k.startsWith(`${baseKeyName}.request.normal-`)).slice(0, systemglobal.Mugino_Normal_Max_Ingest || 50)
        const backlog = keys.filter(k => k.startsWith(`${baseKeyName}.request.backlog-`)).slice(0, systemglobal.Mugino_Backlog_Max_Ingest || 25)
        const allRequests = [...new Set([ ...priority, ...normal, ...backlog ])];
        const availableGpus = Array.from(requestQueueMeta.keys());
        const gpuMeta = availableGpus.map(k => requestQueueMeta.get(k));
        const perClientRatio = parseInt((availableGpus.length / allRequests.length).toString().split('.')[0])
        const hasRemainder = ((availableGpus.length / allRequests.length).toString().split('.').length > 0);
        let jobsToDispatch = shuffleArray([ ...allRequests ]);

        // Dispatch jobs to queues
        console.log(`Need to release ${jobsToDispatch.length} jobs to GPUs`)
        for (let i = 0; i < availableGpus.length;) {
            // Calculate the number of jobs to dispatch
            const jobsForGpu = (() => {
                const maxJobsAllowed = (gpuMeta[i].max_requests) ? parseInt(gpuMeta[i].max_requests.toString()) : 50
                if (perClientRatio <= maxJobsAllowed)
                    return perClientRatio;
                return maxJobsAllowed;
            })()
            // Create or load previous jobs
            let thisGpusJobs = [];
            if (requestQueue.has(availableGpus[i]))
                thisGpusJobs = requestQueue.get(availableGpus[i])
            for (let j = 0; j < jobsForGpu;) {
                if (jobsToDispatch.length !== 0)
                    thisGpusJobs.push(jobsToDispatch.pop());
            }
            if (i === 0 && hasRemainder  && jobsToDispatch.length !== 0)
                thisGpusJobs.push(jobsToDispatch.pop());
            console.log(`Have ${thisGpusJobs.length} jobs for ${gpuMeta[i].name}`)
            requestQueue.set(availableGpus[i], thisGpusJobs);
        }
        processLastRun = Date.now();

    }
    // Ensure Flooding does not stop processing
    setInterval(() => {
        if (processLastRun && ((Date.now() - processLastRun) > systemglobal.Mugino_Max_Ingest_Delay || 300000)) {
            clearTimeout(processTimer);
            processTimer = null;
            processPendingRequests();
        }
    }, 60000);
})()
