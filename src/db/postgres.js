const { Pool } = require('pg');
const config = require('../config/env');

const pool = new Pool({
  host: config.postgres.host,
  port: config.postgres.port,
  database: config.postgres.database,
  user: config.postgres.user,
  password: config.postgres.password,
  max: config.postgres.max,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 5000,
});

// Ошибки на неактивных клиентах пула не должны валить процесс.
pool.on('error', (err) => {
  console.error('Unexpected PostgreSQL pool error:', err);
});

async function testConnection() {
  const client = await pool.connect();
  try {
    await client.query('SELECT 1');
  } finally {
    client.release();
  }
}

async function shutdown() {
  await pool.end();
}

module.exports = { pool, testConnection, shutdown };