import { Job, Worker } from 'bullmq';
import { logger } from '../config/logger';
import { createBullMQRedisConnection } from '../config/redis';
import { EmailNotificationStrategy } from '../patterns/strategy/email.strategy';
import { strategyRegistry } from '../patterns/strategy/strategy.registry';
import { queueRegistry } from '../queues/queue.registry';
import { EmailJobData } from '../types/notification';

/**
 * Dedicated Worker Cluster for BULK Email Notifications (Newsletters, Promos, Marketing).
 * Operates with controlled concurrency to prevent vendor rate-limit exhaustion.
 */
export class EmailBulkWorker {
  private worker: Worker<EmailJobData> | null = null;
  private emailStrategy: EmailNotificationStrategy;
  public readonly queueName = 'email_bulk' as const;
  public readonly concurrency: number;

  constructor(concurrency = 5, emailStrategy?: EmailNotificationStrategy) {
    this.concurrency = concurrency;
    this.emailStrategy = emailStrategy || strategyRegistry.getEmailStrategy();
  }

  start(): void {
    if (this.worker) return;

    this.worker = new Worker<EmailJobData>(
      this.queueName,
      async (job: Job<EmailJobData>) => {
        return this.processJob(job);
      },
      {
        connection: createBullMQRedisConnection(),
        concurrency: this.concurrency,
        limiter: {
          max: 10,
          duration: 1000,
        },
      }
    );

    this.setupEvents();
    logger.info(
      { queue: this.queueName, concurrency: this.concurrency, tier: 'BULK' },
      '📦 Dedicated Email BULK Worker Cluster started'
    );
  }

  private async processJob(job: Job<EmailJobData>): Promise<unknown> {
    const { data } = job;
    const { notificationId } = data;

    logger.info(
      { jobId: job.id, notificationId, queue: this.queueName, attempt: job.attemptsMade + 1 },
      '📬 [EMAIL-BULK] Processing bulk marketing/newsletter email'
    );

    const result = await this.emailStrategy.process(data);

    logger.info(
      {
        jobId: job.id,
        notificationId,
        provider: result.provider,
        fallbackUsed: result.fallbackUsed,
      },
      '✅ [EMAIL-BULK] Bulk email delivered successfully'
    );

    return result;
  }

  private setupEvents(): void {
    if (!this.worker) return;

    this.worker.on('completed', (job) => {
      logger.debug(
        { jobId: job.id, queue: this.queueName, notificationId: job.data.notificationId },
        '[EMAIL-BULK] Job completed'
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
        '❌ [EMAIL-BULK] Job failed'
      );

      if (job.attemptsMade >= maxAttempts) {
        logger.error(
          { jobId: job.id, notificationId: job.data.notificationId },
          '🚨 [EMAIL-BULK] All retries exhausted. Moving to email_dlq'
        );
        try {
          await queueRegistry.moveToEmailDlq(job.data, `[BULK_FAILURE] ${err.message}`);
        } catch (dlqErr: unknown) {
          const dlqMsg = dlqErr instanceof Error ? dlqErr.message : String(dlqErr);
          logger.error({ error: dlqMsg }, 'Failed to route bulk email to DLQ');
        }
      }
    });

    this.worker.on('error', (err) => {
      logger.error({ queue: this.queueName, error: err.message }, '[EMAIL-BULK] Worker error');
    });
  }

  async close(): Promise<void> {
    if (this.worker) {
      await this.worker.close();
      this.worker = null;
      logger.info({ queue: this.queueName }, 'Email Bulk Worker closed');
    }
  }
}
