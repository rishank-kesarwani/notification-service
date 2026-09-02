import { logger } from '../config/logger';
import { queueRegistry } from '../queues/queue.registry';
import { EmailWorker } from './email.worker';
import { PushWorker } from './push.worker';

const emailWorker = new EmailWorker();
const pushWorker = new PushWorker();

logger.info('🚀 Starting notification background workers...');

emailWorker.start();
pushWorker.start();

logger.info('✅ Email and Push worker clusters are actively listening to queues');

// Graceful shutdown handling
const shutdown = async (signal: string) => {
  logger.info({ signal }, 'Shutting down workers gracefully...');
  try {
    await Promise.all([emailWorker.close(), pushWorker.close(), queueRegistry.close()]);
    logger.info('Workers and queue connections closed gracefully');
    process.exit(0);
  } catch (err: unknown) {
    const errorMsg = err instanceof Error ? err.message : String(err);
    logger.error({ error: errorMsg }, 'Error during worker shutdown');
    process.exit(1);
  }
};

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
