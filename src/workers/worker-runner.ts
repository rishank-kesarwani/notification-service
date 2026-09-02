import { logger } from '../config/logger';
import { queueRegistry } from '../queues/queue.registry';
import {
  EmailCriticalWorker,
  EmailBulkWorker,
  PushCriticalWorker,
  PushBulkWorker,
} from './index';

const workerType = process.env.WORKER_TYPE || process.argv[2] || 'all';

logger.info({ target: workerType }, '🚀 Initializing independent notification worker processes...');

const emailCriticalWorker = new EmailCriticalWorker();
const emailBulkWorker = new EmailBulkWorker();
const pushCriticalWorker = new PushCriticalWorker();
const pushBulkWorker = new PushBulkWorker();

const activeWorkers: { name: string; close: () => Promise<void> }[] = [];

if (workerType === 'all' || workerType === 'email_critical' || workerType === 'email:critical') {
  emailCriticalWorker.start();
  activeWorkers.push({ name: 'EmailCriticalWorker', close: () => emailCriticalWorker.close() });
}

if (workerType === 'all' || workerType === 'email_bulk' || workerType === 'email:bulk') {
  emailBulkWorker.start();
  activeWorkers.push({ name: 'EmailBulkWorker', close: () => emailBulkWorker.close() });
}

if (workerType === 'all' || workerType === 'push_critical' || workerType === 'push:critical') {
  pushCriticalWorker.start();
  activeWorkers.push({ name: 'PushCriticalWorker', close: () => pushCriticalWorker.close() });
}

if (workerType === 'all' || workerType === 'push_bulk' || workerType === 'push:bulk') {
  pushBulkWorker.start();
  activeWorkers.push({ name: 'PushBulkWorker', close: () => pushBulkWorker.close() });
}

logger.info(
  { runningClusters: activeWorkers.map((w) => w.name) },
  '✅ Dedicated worker clusters are actively running without bottlenecks'
);

// Graceful shutdown handling
const shutdown = async (signal: string) => {
  logger.info({ signal }, 'Shutting down active worker clusters gracefully...');
  try {
    await Promise.all(activeWorkers.map((w) => w.close()));
    await queueRegistry.close();
    logger.info('All worker clusters and queue connections closed gracefully');
    process.exit(0);
  } catch (err: unknown) {
    const errorMsg = err instanceof Error ? err.message : String(err);
    logger.error({ error: errorMsg }, 'Error during worker cluster shutdown');
    process.exit(1);
  }
};

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
