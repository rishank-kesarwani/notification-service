import { Queue, JobsOptions } from 'bullmq';
import { createBullMQRedisConnection } from '../config/redis';
import { logger } from '../config/logger';
import {
  EmailJobData,
  EmailQueueName,
  PushJobData,
  PushQueueName,
  QueueName,
} from '../types/notification';

/**
 * Standard retry and backoff settings for critical queues
 */
export const CRITICAL_JOB_OPTIONS: JobsOptions = {
  attempts: 3,
  backoff: {
    type: 'exponential',
    delay: 1000, // 1s, 2s, 4s
  },
  removeOnComplete: {
    age: 3600, // Keep completed jobs for 1 hour
    count: 1000,
  },
  removeOnFail: false, // Keep failed jobs for DLQ inspection
};

/**
 * Standard retry and backoff settings for bulk queues
 */
export const BULK_JOB_OPTIONS: JobsOptions = {
  attempts: 3,
  backoff: {
    type: 'exponential',
    delay: 3000, // 3s, 6s, 12s
  },
  removeOnComplete: {
    age: 1800,
    count: 500,
  },
  removeOnFail: false,
};

export class QueueRegistry {
  private emailCriticalQueue: Queue<EmailJobData>;
  private emailBulkQueue: Queue<EmailJobData>;
  private emailDlq: Queue<EmailJobData>;

  private pushCriticalQueue: Queue<PushJobData>;
  private pushBulkQueue: Queue<PushJobData>;
  private pushDlq: Queue<PushJobData>;

  constructor() {
    // Independent BullMQ connections for each dedicated queue
    this.emailCriticalQueue = new Queue<EmailJobData>('email_critical', {
      connection: createBullMQRedisConnection(),
      defaultJobOptions: CRITICAL_JOB_OPTIONS,
    });

    this.emailBulkQueue = new Queue<EmailJobData>('email_bulk', {
      connection: createBullMQRedisConnection(),
      defaultJobOptions: BULK_JOB_OPTIONS,
    });

    this.emailDlq = new Queue<EmailJobData>('email_dlq', {
      connection: createBullMQRedisConnection(),
    });

    this.pushCriticalQueue = new Queue<PushJobData>('push_critical', {
      connection: createBullMQRedisConnection(),
      defaultJobOptions: CRITICAL_JOB_OPTIONS,
    });

    this.pushBulkQueue = new Queue<PushJobData>('push_bulk', {
      connection: createBullMQRedisConnection(),
      defaultJobOptions: BULK_JOB_OPTIONS,
    });

    this.pushDlq = new Queue<PushJobData>('push_dlq', {
      connection: createBullMQRedisConnection(),
    });

    logger.info('BullMQ Queue Registry initialized with dedicated channel queues');
  }

  public getEmailCriticalQueue(): Queue<EmailJobData> {
    return this.emailCriticalQueue;
  }

  public getEmailBulkQueue(): Queue<EmailJobData> {
    return this.emailBulkQueue;
  }

  public getEmailDlq(): Queue<EmailJobData> {
    return this.emailDlq;
  }

  public getPushCriticalQueue(): Queue<PushJobData> {
    return this.pushCriticalQueue;
  }

  public getPushBulkQueue(): Queue<PushJobData> {
    return this.pushBulkQueue;
  }

  public getPushDlq(): Queue<PushJobData> {
    return this.pushDlq;
  }

  /**
   * Route and enqueue email job based on priority tier
   */
  async enqueueEmail(jobData: EmailJobData): Promise<{ queueName: EmailQueueName; jobId: string }> {
    const queueName: EmailQueueName =
      jobData.priority === 'CRITICAL' ? 'email_critical' : 'email_bulk';
    const queue =
      jobData.priority === 'CRITICAL' ? this.emailCriticalQueue : this.emailBulkQueue;

    const job = await queue.add(
      `send_email_${jobData.notificationId}`,
      jobData,
      jobData.priority === 'CRITICAL' ? CRITICAL_JOB_OPTIONS : BULK_JOB_OPTIONS
    );

    logger.info(
      { notificationId: jobData.notificationId, queueName, jobId: job.id },
      'Email job added to queue'
    );

    return { queueName, jobId: job.id || jobData.notificationId };
  }

  /**
   * Route and enqueue push job based on priority tier
   */
  async enqueuePush(jobData: PushJobData): Promise<{ queueName: PushQueueName; jobId: string }> {
    const queueName: PushQueueName =
      jobData.priority === 'CRITICAL' ? 'push_critical' : 'push_bulk';
    const queue =
      jobData.priority === 'CRITICAL' ? this.pushCriticalQueue : this.pushBulkQueue;

    const job = await queue.add(
      `send_push_${jobData.notificationId}`,
      jobData,
      jobData.priority === 'CRITICAL' ? CRITICAL_JOB_OPTIONS : BULK_JOB_OPTIONS
    );

    logger.info(
      { notificationId: jobData.notificationId, queueName, jobId: job.id },
      'Push job added to queue'
    );

    return { queueName, jobId: job.id || jobData.notificationId };
  }

  /**
   * Move failed job into Dead Letter Queue
   */
  async moveToEmailDlq(jobData: EmailJobData, errorReason: string): Promise<void> {
    logger.warn(
      { notificationId: jobData.notificationId, errorReason },
      'Moving failed email job to email_dlq'
    );
    await this.emailDlq.add(`dlq_email_${jobData.notificationId}`, {
      ...jobData,
      metadata: { ...(jobData.metadata || {}), dlqReason: errorReason, dlqTimestamp: new Date().toISOString() },
    });
  }

  /**
   * Move failed push job into Dead Letter Queue
   */
  async moveToPushDlq(jobData: PushJobData, errorReason: string): Promise<void> {
    logger.warn(
      { notificationId: jobData.notificationId, errorReason },
      'Moving failed push job to push_dlq'
    );
    await this.pushDlq.add(`dlq_push_${jobData.notificationId}`, {
      ...jobData,
      metadata: { ...(jobData.metadata || {}), dlqReason: errorReason, dlqTimestamp: new Date().toISOString() },
    });
  }

  /**
   * Collect metrics from all queues
   */
  async getMetrics(): Promise<Record<QueueName, { waiting: number; active: number; completed: number; failed: number; delayed: number }>> {
    const queues: { name: QueueName; queue: Queue }[] = [
      { name: 'email_critical', queue: this.emailCriticalQueue },
      { name: 'email_bulk', queue: this.emailBulkQueue },
      { name: 'email_dlq', queue: this.emailDlq },
      { name: 'push_critical', queue: this.pushCriticalQueue },
      { name: 'push_bulk', queue: this.pushBulkQueue },
      { name: 'push_dlq', queue: this.pushDlq },
    ];

    const metrics = {} as Record<QueueName, { waiting: number; active: number; completed: number; failed: number; delayed: number }>;

    for (const { name, queue } of queues) {
      const counts = await queue.getJobCounts('waiting', 'active', 'completed', 'failed', 'delayed');
      metrics[name] = {
        waiting: counts.waiting || 0,
        active: counts.active || 0,
        completed: counts.completed || 0,
        failed: counts.failed || 0,
        delayed: counts.delayed || 0,
      };
    }

    return metrics;
  }

  /**
   * Gracefully close all queue connections
   */
  async close(): Promise<void> {
    await Promise.all([
      this.emailCriticalQueue.close(),
      this.emailBulkQueue.close(),
      this.emailDlq.close(),
      this.pushCriticalQueue.close(),
      this.pushBulkQueue.close(),
      this.pushDlq.close(),
    ]);
    logger.info('All BullMQ queues closed successfully');
  }
}

export const queueRegistry = new QueueRegistry();
