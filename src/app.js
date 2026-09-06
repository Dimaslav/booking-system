const express = require('express');
const helmet = require('helmet');
const cors = require('cors');
const rateLimit = require('express-rate-limit');

const config = require('./config/env');
const bookingsRouter = require('./routes/bookings');
const eventsRouter = require('./routes/events');
const healthRouter = require('./routes/health');
const { notFoundHandler, errorHandler } = require('./middleware/errorHandler');

function createApp() {
  const app = express();

  app.use(helmet());
  app.use(cors());
  app.use(express.json());

  const reserveLimiter = rateLimit({
    windowMs: config.rateLimit.windowMs,
    max: config.rateLimit.max,
    standardHeaders: true,
    legacyHeaders: false,
    message: { error: 'Too many booking attempts, please try again later' },
  });

  // Rate limit применяется только к чувствительному эндпоинту бронирования.
  app.use('/api/bookings/reserve', reserveLimiter);

  app.use('/api/bookings', bookingsRouter);
  app.use('/api/events', eventsRouter);
  app.use('/health', healthRouter);

  app.use(notFoundHandler);
  app.use(errorHandler);

  return app;
}

module.exports = createApp;