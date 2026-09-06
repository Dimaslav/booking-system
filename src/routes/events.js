const express = require('express');
const { pool } = require('../db/postgres');

const router = express.Router();

router.get('/', async (req, res, next) => {
  try {
    const result = await pool.query(
      `SELECT e.id, e.name, e.total_seats,
              COUNT(b.id)::int AS booked_seats,
              (e.total_seats - COUNT(b.id))::int AS available_seats,
              e.created_at
       FROM events e
       LEFT JOIN bookings b ON e.id = b.event_id
       GROUP BY e.id, e.total_seats
       ORDER BY e.id`
    );

    res.json({ success: true, data: result.rows });
  } catch (error) {
    next(error);
  }
});

router.get('/:id', async (req, res, next) => {
  const eventId = parseInt(req.params.id, 10);
  if (!Number.isInteger(eventId) || eventId <= 0) {
    return res.status(400).json({ error: 'Invalid event id' });
  }

  try {
    const result = await pool.query(
      `SELECT e.id, e.name, e.total_seats,
              COUNT(b.id)::int AS booked_seats,
              (e.total_seats - COUNT(b.id))::int AS available_seats,
              e.created_at
       FROM events e
       LEFT JOIN bookings b ON e.id = b.event_id
       WHERE e.id = $1
       GROUP BY e.id, e.total_seats`,
      [eventId]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Event not found' });
    }

    res.json({ success: true, data: result.rows[0] });
  } catch (error) {
    next(error);
  }
});

module.exports = router;