function validateReserveBooking(req, res, next) {
  const { event_id, user_id } = req.body;

  if (!Number.isInteger(event_id) || event_id <= 0) {
    return res.status(400).json({ error: 'Invalid event_id: must be a positive integer' });
  }

  if (typeof user_id !== 'string' || user_id.trim().length === 0) {
    return res.status(400).json({ error: 'Invalid user_id: must be a non-empty string' });
  }

  if (user_id.length > 255) {
    return res.status(400).json({ error: 'Invalid user_id: exceeds maximum length of 255' });
  }

  req.body.user_id = user_id.trim();
  next();
}

function validatePagination(req, res, next) {
  let { limit = '20', offset = '0' } = req.query;
  limit = parseInt(limit, 10);
  offset = parseInt(offset, 10);

  if (!Number.isInteger(limit) || limit <= 0 || limit > 100) {
    return res.status(400).json({ error: 'Invalid limit: must be between 1 and 100' });
  }
  if (!Number.isInteger(offset) || offset < 0) {
    return res.status(400).json({ error: 'Invalid offset: must be >= 0' });
  }

  req.pagination = { limit, offset };
  next();
}

module.exports = { validateReserveBooking, validatePagination };