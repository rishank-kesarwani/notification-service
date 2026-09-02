import { Job, Worker } from 'bullmq';
import { logger } from '../config/logger';
import { createBullMQRedisConnection } from '../config/redis';
import { PushNotificationStrategy } from '../patterns/strategy/push.strategy';
import { strategyRegistry } from '../patterns/strategy/strategy.registry';
import { queueRegistry } from '../queues/queue.registry';
import { PushJobData } from '../types/notification';

/**
 * Dedicated Worker Cluster for CRITICAL Push Notifications (Real-time security alerts, 2FA, Fraud notices).
 * Runs with high concurrency and instant dispatch.
 */
export class PushCriticalWorker {
  private worker: Worker<PushJobData> | null = null;
  private pushStrategy: PushNotificationStrategy;
  public readonly queueName = 'push_critical' as const;
  public readonly concurrency: number;

  constructor(concurrency = 30, pushStrategy?: PushNotificationStrategy) {
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
        lockDuration: 30000,
        stalledInterval: 15000,
      }
    );

    this.setupEvents();
    logger.info(
      { queue: this.queueName, concurrency: this.concurrency, tier: 'CRITICAL' },
      '🚀 Dedicated Push CRITICAL Worker Cluster started'
    );
  }

  private async processJob(job: Job<PushJobData>): Promise<unknown> {
    const { data } = job;
    const { notificationId } = data;

    logger.info(
      { jobId: job.id, notificationId, queue: this.queueName, attempt: job.attemptsMade + 1 },
      '⚡ [PUSH-CRITICAL] Processing urgent push notification'
    );

    const result = await this.pushStrategy.process(data);

    logger.info(
      {
        jobId: job.id,
        notificationId,
        provider: result.provider,
      },
      '✅ [PUSH-CRITICAL] Push delivered successfully'
    );

    return result;
  }

  private setupEvents(): void {
    if (!this.worker) return;

    this.worker.on('completed', (job) => {
      logger.debug(
        { jobId: job.id, queue: this.queueName, notificationId: job.data.notificationId },
        '[PUSH-CRITICAL] Job completed'
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
        '❌ [PUSH-CRITICAL] Job failed'
      );

      if (job.attemptsMade >= maxAttempts) {
        logger.error(
          { jobId: job.id, notificationId: job.data.notificationId },
          '🚨 [PUSH-CRITICAL] All retries exhausted. Moving to push_dlq'
        );
        try {
          await queueRegistry.moveToPushDlq(job.data, `[CRITICAL_FAILURE] ${err.message}`);
        } catch (dlqErr: unknown) {
          const dlqMsg = dlqErr instanceof Error ? dlqErr.message : String(dlqErr);
          logger.error({ error: dlqMsg }, 'Failed to route critical push to DLQ');
        }
      }
    });

    this.worker.on('error', (err) => {
      logger.error({ queue: this.queueName, error: err.message }, '[PUSH-CRITICAL] Worker error');
    });
  }

  async close(): Promise<void> {
    if (this.worker) {
      await this.worker.close();
      this.worker = null;
      logger.info({ queue: this.queueName }, 'Push Critical Worker closed');
    }
  }
}
