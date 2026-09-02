import { createApp } from './app';
import { env } from './config/env';
import { logger } from './config/logger';
import { queueRegistry } from './queues/queue.registry';
import { EmailWorker } from './workers/email.worker';
import { PushWorker } from './workers/push.worker';

const app = createApp();

// Start workers embedded in server for local development or can be disabled via WORKERS_EMBEDDED=false
const startEmbeddedWorkers = process.env.WORKERS_EMBEDDED !== 'false';
let emailWorker: EmailWorker | null = null;
let pushWorker: PushWorker | null = null;

if (startEmbeddedWorkers) {
  logger.info('Starting embedded workers alongside API server...');
  emailWorker = new EmailWorker();
  pushWorker = new PushWorker();
  emailWorker.start();
  pushWorker.start();
}

const server = app.listen(env.PORT, () => {
  logger.info(
    { port: env.PORT, env: env.NODE_ENV, embeddedWorkers: startEmbeddedWorkers },
    `⚡ Dual-Channel Notification Engine server listening on port ${env.PORT}`
  );
});

// Graceful Shutdown
const shutdown = async (signal: string) => {
  logger.info({ signal }, 'Shutting down server gracefully...');

  server.close(async () => {
    logger.info('HTTP server closed');

    try {
      if (emailWorker) await emailWorker.close();
      if (pushWorker) await pushWorker.close();
      await queueRegistry.close();
      logger.info('All queue and worker resources released. Exiting.');
      process.exit(0);
    } catch (err: unknown) {
      const errorMsg = err instanceof Error ? err.message : String(err);
      logger.error({ error: errorMsg }, 'Error during shutdown');
      process.exit(1);
    }
  });

  // Force close after 10s if hanging
  setTimeout(() => {
    logger.error('Forced shutdown due to timeout');
    process.exit(1);
  }, 10000);
};

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
