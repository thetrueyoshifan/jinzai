let config = require('../config.json');
if (process.env.REDIS_HOST && process.env.REDIS_HOST.trim().length > 0)
    config.redis_host = process.env.REDIS_HOST.trim()
if (process.env.REDIS_HOST && process.env.REDIS_PORT.trim().length > 0)
    config.redis_port = process.env.REDIS_PORT.trim()
if (process.env.REDIS_PASSWORD && process.env.REDIS_PASSWORD.trim().length > 0)
    config.redis_pass = process.env.REDIS_PASSWORD.trim()

const { printLine } = require('./logSystem');
const Redis = require('ioredis');

let redis = new Redis({
    host: config.redis_host,
    port: (config.redis_port) ? parseInt(config.redis_port.toString()) : undefined,
    password: config.redis_pass
});

async function redisStore(key, value) {
    return await redis.set(key, value);
};
async function redisRetrieve(key) {
    return await redis.get(key);
};

async function redisDelete(key) {
    return await redis.del(key);
};

async function redisSearch(pattern) {
    return await redis.keys(pattern);
};

process.on('uncaughtException', function(err) {
    printLine("uncaughtException", err.message, "critical", err);
    process.exit(1)
});

module.exports = { redisStore, redisRetrieve, redisDelete, redisSearch };
