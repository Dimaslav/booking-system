const config = require('./src/config/env');
const createApp = require('./src/app');
const pg = require('./src/db/postgres');
const cache = require('./src/db/redis');

let httpServer;

async function startServer() {
  await pg.testConnection();
  console.log('PostgreSQL connection established');

  await cache.connect();

  const app = createApp();
  httpServer = app.listen(config.port, () => {
    console.log(`Booking system server running on port ${config.port}`);
    console.log(`Health check:      http://localhost:${config.port}/health`);
    console.log(`Events list:       GET  http://localhost:${config.port}/api/events`);
    console.log(`Reserve booking:   POST http://localhost:${config.port}/api/bookings/reserve`);
    console.log(`Cancel booking:    DELETE http://localhost:${config.port}/api/bookings/:id`);
    console.log(`User bookings:     GET  http://localhost:${config.port}/api/bookings/user/:user_id`);
  });
}

async function shutdown(signal) {
  console.log(`\nReceived ${signal}, shutting down gracefully...`);

  if (httpServer) {
    await new Promise((resolve) => httpServer.close(resolve));
  }

  await cache.shutdown().catch((err) => console.error('Redis shutdown error:', err));
  await pg.shutdown().catch((err) => console.error('PostgreSQL shutdown error:', err));

  console.log('Shutdown complete');
  process.exit(0);
}

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('unhandledRejection', (reason) => {
  console.error('Unhandled Rejection:', reason);
});

startServer().catch((error) => {
  console.error('Failed to start server:', error);
  process.exit(1);
});