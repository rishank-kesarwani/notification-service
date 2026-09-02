import Redis, { RedisOptions } from 'ioredis';
import { env } from './env';
import { logger } from './logger';

export const redisConfig: RedisOptions = {
  host: env.REDIS_HOST,
  port: env.REDIS_PORT,
  password: env.REDIS_PASSWORD || undefined,
  maxRetriesPerRequest: null, // Required by BullMQ
  enableReadyCheck: false,
  retryStrategy(times) {
    const delay = Math.min(times * 50, 2000);
    logger.warn({ attempt: times, nextDelayMs: delay }, 'Retrying Redis connection...');
    return delay;
  },
};

// General-purpose Redis client for Caching, Idempotency, Rate Limiting
export const redisClient = new Redis(redisConfig);

redisClient.on('connect', () => {
  logger.info({ host: env.REDIS_HOST, port: env.REDIS_PORT }, 'Redis client connected successfully');
});

redisClient.on('error', (err) => {
  logger.error({ err: err.message }, 'Redis client connection error');
});

// Helper factory for BullMQ connections (BullMQ creates its own dedicated connections)
export const createBullMQRedisConnection = (): Redis => {
  return new Redis(redisConfig);
};
