const express = require('express');
const { Pool } = require('pg');
const redis = require('redis');

const app = express();
app.use(express.json());

const pgPool = new Pool({
  user: 'postgres',
  host: 'localhost',
  database: 'booking_system',
  password: 'password',
  port: 5432,
});

const redisClient = redis.createClient({
  socket: { host: 'localhost', port: 6379 }
});

redisClient.on('error', (err) => console.log('Redis Client Error', err));

async function initialize() {
  await redisClient.connect();
  console.log('Database connections established');
}

function validateReserveBooking(req, res, next) {
  const { event_id, user_id } = req.body;
  
  if (!event_id || !Number.isInteger(event_id) || event_id <= 0) {
    return res.status(400).json({ error: 'Invalid event_id' });
  }
  
  if (!user_id || typeof user_id !== 'string' || user_id.trim().length === 0) {
    return res.status(400).json({ error: 'Invalid user_id' });
  }
  
  next();
}

async function checkBookingInCache(eventId, userId) {
  const cacheKey = `booking:${eventId}:${userId}`;
  const cached = await redisClient.get(cacheKey);
  return cached !== null;
}

app.post('/api/bookings/reserve', validateReserveBooking, async (req, res) => {
  const { event_id, user_id } = req.body;

  try {
    const isCached = await checkBookingInCache(event_id, user_id);
    if (isCached) {
      return res.status(400).json({ error: 'User has already booked this event' });
    }

    const client = await pgPool.connect();
    
    try {
      await client.query('BEGIN');
      
      const eventResult = await client.query(
        `SELECT e.total_seats, COUNT(b.id) as booked_seats
         FROM events e 
         LEFT JOIN bookings b ON e.id = b.event_id 
         WHERE e.id = $1 
         GROUP BY e.id, e.total_seats`,
        [event_id]
      );

      if (eventResult.rows.length === 0) {
        await client.query('ROLLBACK');
        return res.status(404).json({ error: 'Event not found' });
      }

      const { total_seats, booked_seats } = eventResult.rows[0];
      
      if (booked_seats >= total_seats) {
        await client.query('ROLLBACK');
        return res.status(400).json({ error: 'No available seats for this event' });
      }

      const existingBooking = await client.query(
        'SELECT id FROM bookings WHERE event_id = $1 AND user_id = $2',
        [event_id, user_id]
      );

      if (existingBooking.rows.length > 0) {
        await client.query('ROLLBACK');
        const cacheKey = `booking:${event_id}:${user_id}`;
        await redisClient.setEx(cacheKey, 3600, 'true');
        return res.status(400).json({ error: 'User has already booked this event' });
      }

      const result = await client.query(
        'INSERT INTO bookings (event_id, user_id) VALUES ($1, $2) RETURNING *',
        [event_id, user_id]
      );

      await client.query('COMMIT');

      const cacheKey = `booking:${event_id}:${user_id}`;
      await redisClient.setEx(cacheKey, 3600, 'true');

      res.status(201).json({
        success: true,
        message: 'Booking created successfully',
        data: {
          booking_id: result.rows[0].id,
          event_id: result.rows[0].event_id,
          user_id: result.rows[0].user_id,
          created_at: result.rows[0].created_at
        }
      });

    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }

  } catch (error) {
    console.error('Booking error:', error);
    
    if (error.code === '23505') {
      return res.status(400).json({ error: 'User has already booked this event' });
    }
    
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.get('/api/bookings/user/:user_id', async (req, res) => {
  try {
    const { user_id } = req.params;
    
    const result = await pgPool.query(
      `SELECT b.*, e.name as event_name 
       FROM bookings b 
       JOIN events e ON b.event_id = e.id 
       WHERE b.user_id = $1 
       ORDER BY b.created_at DESC`,
      [user_id]
    );
    
    res.json({
      success: true,
      data: result.rows
    });
  } catch (error) {
    console.error('Get bookings error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.get('/health', async (req, res) => {
  try {
    await pgPool.query('SELECT 1');
    await redisClient.ping();
    
    res.json({ 
      status: 'OK', 
      database: 'connected',
      redis: 'connected',
      timestamp: new Date().toISOString() 
    });
  } catch (error) {
    res.status(500).json({ 
      status: 'ERROR', 
      error: error.message 
    });
  }
});

app.use('*', (req, res) => {
  res.status(404).json({ error: 'Route not found' });
});

app.use((error, req, res, next) => {
  console.error('Unhandled error:', error);
  res.status(500).json({ error: 'Internal server error' });
});

const PORT = process.env.PORT || 3000;

async function startServer() {
  await initialize();
  
  app.listen(PORT, () => {
    console.log(`Booking system server running on port ${PORT}`);
    console.log(`Health check: http://localhost:${PORT}/health`);
    console.log(`Booking endpoint: POST http://localhost:${PORT}/api/bookings/reserve`);
  });
}

startServer().catch(console.error);
