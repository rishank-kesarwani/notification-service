import { Job, Worker } from 'bullmq';
import { logger } from '../config/logger';
import { createBullMQRedisConnection } from '../config/redis';
import { EmailNotificationStrategy } from '../patterns/strategy/email.strategy';
import { strategyRegistry } from '../patterns/strategy/strategy.registry';
import { queueRegistry } from '../queues/queue.registry';
import { EmailJobData, EmailQueueName } from '../types/notification';

export class EmailWorker {
  private workers: Worker<EmailJobData>[] = [];
  private emailStrategy: EmailNotificationStrategy;

  constructor(emailStrategy?: EmailNotificationStrategy) {
    this.emailStrategy = emailStrategy || strategyRegistry.getEmailStrategy();
  }

  /**
   * Starts dedicated workers for email queues
   */
  start(): void {
    const emailQueues: { name: EmailQueueName; concurrency: number }[] = [
      { name: 'email_critical', concurrency: 10 },
      { name: 'email_bulk', concurrency: 5 },
    ];

    for (const { name, concurrency } of emailQueues) {
      const worker = new Worker<EmailJobData>(
        name,
        async (job: Job<EmailJobData>) => {
          return this.processEmailJob(job, name);
        },
        {
          connection: createBullMQRedisConnection(),
          concurrency,
        }
      );

      this.setupWorkerEvents(worker, name);
      this.workers.push(worker);
      logger.info({ queue: name, concurrency }, `Email worker started for queue: ${name}`);
    }
  }

  private async processEmailJob(job: Job<EmailJobData>, queueName: EmailQueueName): Promise<unknown> {
    const { data } = job;
    const { notificationId } = data;

    logger.info(
      { jobId: job.id, notificationId, queue: queueName, attempt: job.attemptsMade + 1 },
      'Processing email notification job via Strategy pattern'
    );

    // Execute via Strategy (includes rate limiting, Resend primary, Circuit Breaker, SMTP fallback)
    const result = await this.emailStrategy.process(data);

    logger.info(
      {
        jobId: job.id,
        notificationId,
        provider: result.provider,
        fallbackUsed: result.fallbackUsed,
      },
      'Email job successfully processed by strategy'
    );

    return result;
  }

  private setupWorkerEvents(worker: Worker<EmailJobData>, queueName: EmailQueueName): void {
    worker.on('completed', (job) => {
      logger.info(
        { jobId: job.id, queue: queueName, notificationId: job.data.notificationId },
        'Email job completed successfully'
      );
    });

    worker.on('failed', async (job, err) => {
      if (!job) return;

      const maxAttempts = job.opts.attempts || 3;
      logger.error(
        {
          jobId: job.id,
          queue: queueName,
          notificationId: job.data.notificationId,
          attemptsMade: job.attemptsMade,
          maxAttempts,
          error: err.message,
        },
        'Email job failed'
      );

      // If all retries exhausted, push to Dead Letter Queue (DLQ)
      if (job.attemptsMade >= maxAttempts) {
        logger.error(
          { jobId: job.id, notificationId: job.data.notificationId, queue: queueName },
          'Email job exhausted all retries. Routing to email_dlq'
        );
        try {
          await queueRegistry.moveToEmailDlq(job.data, err.message);
        } catch (dlqErr: unknown) {
          const dlqMsg = dlqErr instanceof Error ? dlqErr.message : String(dlqErr);
          logger.error({ error: dlqMsg }, 'Failed to route email job to DLQ');
        }
      }
    });

    worker.on('error', (err) => {
      logger.error({ queue: queueName, error: err.message }, 'Email worker error');
    });
  }

  async close(): Promise<void> {
    await Promise.all(this.workers.map((w) => w.close()));
    logger.info('Email workers closed successfully');
  }
}
