const redis = require('redis');
const config = require('../config/env');

const client = redis.createClient({
  socket: {
    host: config.redis.host,
    port: config.redis.port,
    reconnectStrategy: (retries) => Math.min(retries * 100, 3000),
  },
  password: config.redis.password,
});

let available = false;

client.on('error', (err) => {
  available = false;
  console.error('Redis Client Error:', err.message);
});

client.on('ready', () => {
  available = true;
  console.log('Redis connection established');
});

client.on('end', () => {
  available = false;
});

async function connect() {
  try {
    await client.connect();
  } catch (err) {
    // Redis используется только как ускоряющий кэш, а не источник истины.
    // Его недоступность не должна останавливать сервер.
    console.error('Failed to connect to Redis (continuing without cache):', err.message);
  }
}

async function shutdown() {
  if (client.isOpen) {
    await client.quit();
  }
}

/**
 * Безопасное чтение из кэша. При недоступности Redis не бросает исключение,
 * а просто сообщает об отсутствии значения — бизнес-логика продолжит работу
 * через PostgreSQL как единственный источник истины.
 */
async function safeGet(key) {
  if (!available) return null;
  try {
    return await client.get(key);
  } catch (err) {
    console.error('Redis GET failed:', err.message);
    return null;
  }
}

async function safeSetEx(key, seconds, value) {
  if (!available) return;
  try {
    await client.setEx(key, seconds, value);
  } catch (err) {
    console.error('Redis SETEX failed:', err.message);
  }
}

async function safeDel(key) {
  if (!available) return;
  try {
    await client.del(key);
  } catch (err) {
    console.error('Redis DEL failed:', err.message);
  }
}

async function ping() {
  if (!available) throw new Error('Redis not connected');
  return client.ping();
}

module.exports = {
  client,
  connect,
  shutdown,
  safeGet,
  safeSetEx,
  safeDel,
  ping,
  isAvailable: () => available,
};