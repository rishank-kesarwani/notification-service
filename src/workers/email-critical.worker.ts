import { Job, Worker } from 'bullmq';
import { logger } from '../config/logger';
import { createBullMQRedisConnection } from '../config/redis';
import { EmailNotificationStrategy } from '../patterns/strategy/email.strategy';
import { strategyRegistry } from '../patterns/strategy/strategy.registry';
import { queueRegistry } from '../queues/queue.registry';
import { EmailJobData } from '../types/notification';

/**
 * Dedicated Worker Cluster for CRITICAL Email Notifications (OTPs, Security Alerts, Password Resets).
 * Runs with high concurrency and zero throttling.
 */
export class EmailCriticalWorker {
  private worker: Worker<EmailJobData> | null = null;
  private emailStrategy: EmailNotificationStrategy;
  public readonly queueName = 'email_critical' as const;
  public readonly concurrency: number;

  constructor(concurrency = 25, emailStrategy?: EmailNotificationStrategy) {
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
        // Critical queue polling settings for lowest latency
        lockDuration: 30000,
        stalledInterval: 15000,
      }
    );

    this.setupEvents();
    logger.info(
      { queue: this.queueName, concurrency: this.concurrency, tier: 'CRITICAL' },
      '🚀 Dedicated Email CRITICAL Worker Cluster started'
    );
  }

  private async processJob(job: Job<EmailJobData>): Promise<unknown> {
    const { data } = job;
    const { notificationId } = data;

    logger.info(
      { jobId: job.id, notificationId, queue: this.queueName, attempt: job.attemptsMade + 1 },
      '⚡ [EMAIL-CRITICAL] Processing high-priority email notification'
    );

    const result = await this.emailStrategy.process(data);

    logger.info(
      {
        jobId: job.id,
        notificationId,
        provider: result.provider,
        fallbackUsed: result.fallbackUsed,
      },
      '✅ [EMAIL-CRITICAL] Email delivered successfully'
    );

    return result;
  }

  private setupEvents(): void {
    if (!this.worker) return;

    this.worker.on('completed', (job) => {
      logger.debug(
        { jobId: job.id, queue: this.queueName, notificationId: job.data.notificationId },
        '[EMAIL-CRITICAL] Job completed'
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
        '❌ [EMAIL-CRITICAL] Job failed'
      );

      if (job.attemptsMade >= maxAttempts) {
        logger.error(
          { jobId: job.id, notificationId: job.data.notificationId },
          '🚨 [EMAIL-CRITICAL] All retries exhausted. Moving to email_dlq'
        );
        try {
          await queueRegistry.moveToEmailDlq(job.data, `[CRITICAL_FAILURE] ${err.message}`);
        } catch (dlqErr: unknown) {
          const dlqMsg = dlqErr instanceof Error ? dlqErr.message : String(dlqErr);
          logger.error({ error: dlqMsg }, 'Failed to route critical email to DLQ');
        }
      }
    });

    this.worker.on('error', (err) => {
      logger.error({ queue: this.queueName, error: err.message }, '[EMAIL-CRITICAL] Worker error');
    });
  }

  async close(): Promise<void> {
    if (this.worker) {
      await this.worker.close();
      this.worker = null;
      logger.info({ queue: this.queueName }, 'Email Critical Worker closed');
    }
  }
}
