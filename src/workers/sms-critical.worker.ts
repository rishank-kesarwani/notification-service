import { Job, Worker } from 'bullmq';
import { logger } from '../config/logger';
import { createBullMQRedisConnection } from '../config/redis';
import { SmsNotificationStrategy } from '../patterns/strategy/sms.strategy';
import { strategyRegistry } from '../patterns/strategy/strategy.registry';
import { queueRegistry } from '../queues/queue.registry';
import { SmsJobData } from '../types/notification';

/**
 * Dedicated Worker Cluster for CRITICAL SMS Notifications (2FA OTPs, Security Alerts, Urgent Codes).
 * Runs with high concurrency and zero artificial throttling.
 */
export class SmsCriticalWorker {
  private worker: Worker<SmsJobData> | null = null;
  private smsStrategy: SmsNotificationStrategy;
  public readonly queueName = 'sms_critical' as const;
  public readonly concurrency: number;

  constructor(concurrency = 25, smsStrategy?: SmsNotificationStrategy) {
    this.concurrency = concurrency;
    this.smsStrategy = smsStrategy || strategyRegistry.getSmsStrategy();
  }

  start(): void {
    if (this.worker) return;

    this.worker = new Worker<SmsJobData>(
      this.queueName,
      async (job: Job<SmsJobData>) => {
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
      '🚀 Dedicated SMS CRITICAL Worker Cluster started'
    );
  }

  private async processJob(job: Job<SmsJobData>): Promise<unknown> {
    const { data } = job;
    const { notificationId } = data;

    logger.info(
      { jobId: job.id, notificationId, queue: this.queueName, attempt: job.attemptsMade + 1 },
      '⚡ [SMS-CRITICAL] Processing high-priority critical SMS notification'
    );

    const result = await this.smsStrategy.process(data);

    logger.info(
      {
        jobId: job.id,
        notificationId,
        provider: result.provider,
        fallbackUsed: result.fallbackUsed,
      },
      '✅ [SMS-CRITICAL] SMS delivered successfully'
    );

    return result;
  }

  private setupEvents(): void {
    if (!this.worker) return;

    this.worker.on('completed', (job) => {
      logger.debug(
        { jobId: job.id, queue: this.queueName, notificationId: job.data.notificationId },
        '[SMS-CRITICAL] Job completed'
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
        '❌ [SMS-CRITICAL] Job failed'
      );

      if (job.attemptsMade >= maxAttempts) {
        logger.error(
          { jobId: job.id, notificationId: job.data.notificationId },
          '🚨 [SMS-CRITICAL] All retries exhausted. Moving to sms_dlq'
        );
        try {
          await queueRegistry.moveToSmsDlq(job.data, `[CRITICAL_FAILURE] ${err.message}`);
        } catch (dlqErr: unknown) {
          const dlqMsg = dlqErr instanceof Error ? dlqErr.message : String(dlqErr);
          logger.error({ error: dlqMsg }, 'Failed to route critical SMS to DLQ');
        }
      }
    });

    this.worker.on('error', (err) => {
      logger.error({ queue: this.queueName, error: err.message }, '[SMS-CRITICAL] Worker error');
    });
  }

  async close(): Promise<void> {
    if (this.worker) {
      await this.worker.close();
      this.worker = null;
      logger.info({ queue: this.queueName }, 'SMS Critical Worker closed');
    }
  }
}
