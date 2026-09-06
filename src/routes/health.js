const express = require('express');
const { pool } = require('../db/postgres');
const cache = require('../db/redis');

const router = express.Router();

router.get('/', async (req, res) => {
  const status = {
    status: 'OK',
    database: 'unknown',
    redis: 'unknown',
    timestamp: new Date().toISOString(),
  };

  try {
    await pool.query('SELECT 1');
    status.database = 'connected';
  } catch (error) {
    status.database = 'disconnected';
    status.status = 'DEGRADED';
  }

  try {
    await cache.ping();
    status.redis = 'connected';
  } catch (error) {
    status.redis = 'disconnected';
    // Redis не критичен для работы сервиса — статус остаётся DEGRADED, а не ERROR.
    status.status = status.status === 'OK' ? 'DEGRADED' : status.status;
  }

  const httpCode = status.database === 'connected' ? 200 : 503;
  res.status(httpCode).json(status);
});

module.exports = router;