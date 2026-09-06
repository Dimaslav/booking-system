require('dotenv').config();

function required(name, fallback) {
  const value = process.env[name] ?? fallback;
  if (value === undefined) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

const config = {
  port: parseInt(process.env.PORT || '3000', 10),

  postgres: {
    host: required('PG_HOST', 'localhost'),
    port: parseInt(process.env.PG_PORT || '5432', 10),
    database: required('PG_DATABASE', 'booking_system'),
    user: required('PG_USER', 'postgres'),
    password: required('PG_PASSWORD', ''),
    max: parseInt(process.env.PG_POOL_MAX || '20', 10),
  },

  redis: {
    host: process.env.REDIS_HOST || 'localhost',
    port: parseInt(process.env.REDIS_PORT || '6379', 10),
    password: process.env.REDIS_PASSWORD || undefined,
  },

  cacheTtlSeconds: parseInt(process.env.CACHE_TTL_SECONDS || '3600', 10),

  rateLimit: {
    windowMs: parseInt(process.env.RATE_LIMIT_WINDOW_MS || '60000', 10),
    max: parseInt(process.env.RATE_LIMIT_MAX || '20', 10),
  },
};

module.exports = config;