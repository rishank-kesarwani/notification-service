import { Job, Worker } from 'bullmq';
import { logger } from '../config/logger';
import { createBullMQRedisConnection } from '../config/redis';
import { PushNotificationStrategy } from '../patterns/strategy/push.strategy';
import { strategyRegistry } from '../patterns/strategy/strategy.registry';
import { queueRegistry } from '../queues/queue.registry';
import { PushJobData, PushQueueName } from '../types/notification';

export class PushWorker {
  private workers: Worker<PushJobData>[] = [];
  private pushStrategy: PushNotificationStrategy;

  constructor(pushStrategy?: PushNotificationStrategy) {
    this.pushStrategy = pushStrategy || strategyRegistry.getPushStrategy();
  }

  /**
   * Starts dedicated workers for push queues
   */
  start(): void {
    const pushQueues: { name: PushQueueName; concurrency: number }[] = [
      { name: 'push_critical', concurrency: 15 },
      { name: 'push_bulk', concurrency: 10 },
    ];

    for (const { name, concurrency } of pushQueues) {
      const worker = new Worker<PushJobData>(
        name,
        async (job: Job<PushJobData>) => {
          return this.processPushJob(job, name);
        },
        {
          connection: createBullMQRedisConnection(),
          concurrency,
        }
      );

      this.setupWorkerEvents(worker, name);
      this.workers.push(worker);
      logger.info({ queue: name, concurrency }, `Push worker started for queue: ${name}`);
    }
  }

  private async processPushJob(job: Job<PushJobData>, queueName: PushQueueName): Promise<unknown> {
    const { data } = job;
    const { notificationId } = data;

    logger.info(
      { jobId: job.id, notificationId, queue: queueName, attempt: job.attemptsMade + 1 },
      'Processing push notification job via Strategy pattern'
    );

    // Execute via Push Strategy (includes rate limiting, FCM dispatch, Circuit Breaker)
    const result = await this.pushStrategy.process(data);

    logger.info(
      {
        jobId: job.id,
        notificationId,
        provider: result.provider,
      },
      'Push job successfully processed by strategy'
    );

    return result;
  }

  private setupWorkerEvents(worker: Worker<PushJobData>, queueName: PushQueueName): void {
    worker.on('completed', (job) => {
      logger.info(
        { jobId: job.id, queue: queueName, notificationId: job.data.notificationId },
        'Push job completed successfully'
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
        'Push job failed'
      );

      // If all retries exhausted, push to Dead Letter Queue (DLQ)
      if (job.attemptsMade >= maxAttempts) {
        logger.error(
          { jobId: job.id, notificationId: job.data.notificationId, queue: queueName },
          'Push job exhausted all retries. Routing to push_dlq'
        );
        try {
          await queueRegistry.moveToPushDlq(job.data, err.message);
        } catch (dlqErr: unknown) {
          const dlqMsg = dlqErr instanceof Error ? dlqErr.message : String(dlqErr);
          logger.error({ error: dlqMsg }, 'Failed to route push job to DLQ');
        }
      }
    });

    worker.on('error', (err) => {
      logger.error({ queue: queueName, error: err.message }, 'Push worker error');
    });
  }

  async close(): Promise<void> {
    await Promise.all(this.workers.map((w) => w.close()));
    logger.info('Push workers closed successfully');
  }
}
