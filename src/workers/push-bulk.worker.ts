import { Job, Worker } from 'bullmq';
import { logger } from '../config/logger';
import { createBullMQRedisConnection } from '../config/redis';
import { PushNotificationStrategy } from '../patterns/strategy/push.strategy';
import { strategyRegistry } from '../patterns/strategy/strategy.registry';
import { queueRegistry } from '../queues/queue.registry';
import { PushJobData } from '../types/notification';

/**
 * Dedicated Worker Cluster for BULK Push Notifications (Marketing, Daily Digests, App Updates).
 * Uses rate-controlled concurrency to prevent overwhelming vendor gateways.
 */
export class PushBulkWorker {
  private worker: Worker<PushJobData> | null = null;
  private pushStrategy: PushNotificationStrategy;
  public readonly queueName = 'push_bulk' as const;
  public readonly concurrency: number;

  constructor(concurrency = 10, pushStrategy?: PushNotificationStrategy) {
    this.concurrency = concurrency;
    this.pushStrategy = pushStrategy || strategyRegistry.getPushStrategy();
  }

  start(): void {
    if (this.worker) return;

    this.worker = new Worker<PushJobData>(
      this.queueName,
      async (job: Job<PushJobData>) => {
        return this.processJob(job);
      },
      {
        connection: createBullMQRedisConnection(),
        concurrency: this.concurrency,
        limiter: {
          max: 20,
          duration: 1000,
        },
      }
    );

    this.setupEvents();
    logger.info(
      { queue: this.queueName, concurrency: this.concurrency, tier: 'BULK' },
      '📦 Dedicated Push BULK Worker Cluster started'
    );
  }

  private async processJob(job: Job<PushJobData>): Promise<unknown> {
    const { data } = job;
    const { notificationId } = data;

    logger.info(
      { jobId: job.id, notificationId, queue: this.queueName, attempt: job.attemptsMade + 1 },
      '📬 [PUSH-BULK] Processing bulk marketing/engagement push'
    );

    const result = await this.pushStrategy.process(data);

    logger.info(
      {
        jobId: job.id,
        notificationId,
        provider: result.provider,
      },
      '✅ [PUSH-BULK] Bulk push delivered successfully'
    );

    return result;
  }

  private setupEvents(): void {
    if (!this.worker) return;

    this.worker.on('completed', (job) => {
      logger.debug(
        { jobId: job.id, queue: this.queueName, notificationId: job.data.notificationId },
        '[PUSH-BULK] Job completed'
      );
    });

    this.worker.on('failed', async (job, err) => {
      if (!job) return;

      const maxAttempts = job.opts.attempts || 3;
      logger.error(
        {
          jobId: job.id,
          queue: this.queueName,
          notificationId: job.data.notificationId,
          attemptsMade: job.attemptsMade,
          maxAttempts,
          error: err.message,
        },
        '❌ [PUSH-BULK] Job failed'
      );

      if (job.attemptsMade >= maxAttempts) {
        logger.error(
          { jobId: job.id, notificationId: job.data.notificationId },
          '🚨 [PUSH-BULK] All retries exhausted. Moving to push_dlq'
        );
        try {
          await queueRegistry.moveToPushDlq(job.data, `[BULK_FAILURE] ${err.message}`);
        } catch (dlqErr: unknown) {
          const dlqMsg = dlqErr instanceof Error ? dlqErr.message : String(dlqErr);
          logger.error({ error: dlqMsg }, 'Failed to route bulk push to DLQ');
        }
      }
    });

    this.worker.on('error', (err) => {
      logger.error({ queue: this.queueName, error: err.message }, '[PUSH-BULK] Worker error');
    });
  }

  async close(): Promise<void> {
    if (this.worker) {
      await this.worker.close();
      this.worker = null;
      logger.info({ queue: this.queueName }, 'Push Bulk Worker closed');
    }
  }
}
