const express = require('express');
const { pool } = require('../db/postgres');
const cache = require('../db/redis');
const config = require('../config/env');
const { validateReserveBooking, validatePagination } = require('../middleware/validators');

const router = express.Router();

function cacheKey(eventId, userId) {
  return `booking:${eventId}:${userId}`;
}

/**
 * Создание бронирования.
 *
 * Защита от овербукинга: SELECT ... FOR UPDATE блокирует строку события
 * на время транзакции. Второй параллельный запрос на тот же event_id
 * дождётся коммита первого и увидит уже актуальный COUNT(*) броней —
 * это исключает превышение total_seats при гонке запросов.
 */
router.post('/reserve', validateReserveBooking, async (req, res, next) => {
  const { event_id, user_id } = req.body;
  const key = cacheKey(event_id, user_id);

  try {
    const isCached = await cache.safeGet(key);
    if (isCached !== null) {
      return res.status(409).json({ error: 'User has already booked this event' });
    }

    const client = await pool.connect();

    try {
      await client.query('BEGIN');

      const eventResult = await client.query(
        'SELECT total_seats FROM events WHERE id = $1 FOR UPDATE',
        [event_id]
      );

      if (eventResult.rows.length === 0) {
        await client.query('ROLLBACK');
        return res.status(404).json({ error: 'Event not found' });
      }

      const { total_seats } = eventResult.rows[0];

      const countResult = await client.query(
        'SELECT COUNT(*)::int AS booked_seats FROM bookings WHERE event_id = $1',
        [event_id]
      );
      const bookedSeats = countResult.rows[0].booked_seats;

      if (bookedSeats >= total_seats) {
        await client.query('ROLLBACK');
        return res.status(409).json({ error: 'No available seats for this event' });
      }

      const existingBooking = await client.query(
        'SELECT id FROM bookings WHERE event_id = $1 AND user_id = $2',
        [event_id, user_id]
      );

      if (existingBooking.rows.length > 0) {
        await client.query('ROLLBACK');
        await cache.safeSetEx(key, config.cacheTtlSeconds, 'true');
        return res.status(409).json({ error: 'User has already booked this event' });
      }

      const insertResult = await client.query(
        'INSERT INTO bookings (event_id, user_id) VALUES ($1, $2) RETURNING *',
        [event_id, user_id]
      );

      await client.query('COMMIT');
      await cache.safeSetEx(key, config.cacheTtlSeconds, 'true');

      const booking = insertResult.rows[0];
      res.status(201).json({
        success: true,
        message: 'Booking created successfully',
        data: {
          booking_id: booking.id,
          event_id: booking.event_id,
          user_id: booking.user_id,
          created_at: booking.created_at,
        },
      });
    } catch (error) {
      await client.query('ROLLBACK').catch(() => {});
      throw error;
    } finally {
      client.release();
    }
  } catch (error) {
    next(error);
  }
});

/**
 * Отмена бронирования. Также инвалидирует кэш, чтобы пользователь
 * мог забронировать место повторно.
 */
router.delete('/:booking_id', async (req, res, next) => {
  const bookingId = parseInt(req.params.booking_id, 10);
  if (!Number.isInteger(bookingId) || bookingId <= 0) {
    return res.status(400).json({ error: 'Invalid booking_id' });
  }

  try {
    const result = await pool.query(
      'DELETE FROM bookings WHERE id = $1 RETURNING event_id, user_id',
      [bookingId]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Booking not found' });
    }

    const { event_id, user_id } = result.rows[0];
    await cache.safeDel(cacheKey(event_id, user_id));

    res.json({ success: true, message: 'Booking cancelled successfully' });
  } catch (error) {
    next(error);
  }
});

router.get('/user/:user_id', validatePagination, async (req, res, next) => {
  try {
    const { user_id } = req.params;
    const { limit, offset } = req.pagination;

    const result = await pool.query(
      `SELECT b.id, b.event_id, b.user_id, b.created_at, e.name AS event_name
       FROM bookings b
       JOIN events e ON b.event_id = e.id
       WHERE b.user_id = $1
       ORDER BY b.created_at DESC
       LIMIT $2 OFFSET $3`,
      [user_id, limit, offset]
    );

    res.json({
      success: true,
      data: result.rows,
      pagination: { limit, offset, count: result.rows.length },
    });
  } catch (error) {
    next(error);
  }
});

module.exports = router;