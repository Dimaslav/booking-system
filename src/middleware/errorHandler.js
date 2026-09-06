function notFoundHandler(req, res) {
  res.status(404).json({ error: 'Route not found' });
}

function errorHandler(error, req, res, next) {
  console.error('Unhandled error:', error);

  // Дублирующая бронь (гонка между проверкой и вставкой на уровне БД).
  if (error.code === '23505') {
    return res.status(409).json({ error: 'User has already booked this event' });
  }

  // Ссылка на несуществующее мероприятие.
  if (error.code === '23503') {
    return res.status(400).json({ error: 'Referenced event does not exist' });
  }

  res.status(500).json({ error: 'Internal server error' });
}

module.exports = { notFoundHandler, errorHandler };