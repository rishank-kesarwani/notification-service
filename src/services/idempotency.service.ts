import { env } from '../config/env';
import { logger } from '../config/logger';
import { redisClient } from '../config/redis';

export interface IdempotencyRecord {
  status: 'PENDING' | 'PROCESSED';
  notificationId: string;
  responsePayload?: unknown;
  createdAt: string;
}

export class IdempotencyService {
  private readonly prefix = 'idempotency:';
  private readonly defaultTtl = env.IDEMPOTENCY_TTL_SECONDS;

  /**
   * Attempts to acquire an idempotency lock for the given key.
   * If key does not exist, sets it with status 'PENDING' and returns { isDuplicate: false, notificationId }.
   * If key exists, fetches and returns existing record with { isDuplicate: true, existingRecord }.
   */
  async checkAndAcquire(
    idempotencyKey: string,
    notificationId: string,
    ttlSeconds: number = this.defaultTtl
  ): Promise<{ isDuplicate: boolean; notificationId: string; existingRecord?: IdempotencyRecord }> {
    const key = `${this.prefix}${idempotencyKey}`;
    const initialRecord: IdempotencyRecord = {
      status: 'PENDING',
      notificationId,
      createdAt: new Date().toISOString(),
    };

    // SET key value NX EX ttl (Atomic distributed lock/idempotency check)
    const acquired = await redisClient.set(key, JSON.stringify(initialRecord), 'EX', ttlSeconds, 'NX');

    if (acquired === 'OK') {
      logger.debug({ idempotencyKey, notificationId }, 'Idempotency key acquired (fresh request)');
      return { isDuplicate: false, notificationId };
    }

    // Key already exists, retrieve previous record
    const existingRaw = await redisClient.get(key);
    let existingRecord: IdempotencyRecord | undefined;

    if (existingRaw) {
      try {
        existingRecord = JSON.parse(existingRaw) as IdempotencyRecord;
      } catch {
        logger.warn({ idempotencyKey }, 'Failed to parse cached idempotency record');
      }
    }

    logger.info({ idempotencyKey, notificationId: existingRecord?.notificationId }, 'Duplicate idempotency key detected');

    return {
      isDuplicate: true,
      notificationId: existingRecord?.notificationId || notificationId,
      existingRecord,
    };
  }

  /**
   * Updates an acquired idempotency key with its final processed response payload
   */
  async markProcessed(
    idempotencyKey: string,
    notificationId: string,
    responsePayload: unknown,
    ttlSeconds: number = this.defaultTtl
  ): Promise<void> {
    const key = `${this.prefix}${idempotencyKey}`;
    const updatedRecord: IdempotencyRecord = {
      status: 'PROCESSED',
      notificationId,
      responsePayload,
      createdAt: new Date().toISOString(),
    };

    await redisClient.set(key, JSON.stringify(updatedRecord), 'EX', ttlSeconds);
    logger.debug({ idempotencyKey, notificationId }, 'Idempotency key marked as PROCESSED');
  }
}

export const idempotencyService = new IdempotencyService();
