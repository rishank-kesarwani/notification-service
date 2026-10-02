import Redis, { RedisOptions } from 'ioredis';
import { env } from './env';
import { logger } from './logger';

export const getBaseRedisOptions = (): RedisOptions => ({
  maxRetriesPerRequest: null, // Required by BullMQ
  enableReadyCheck: false,
  lazyConnect: env.NODE_ENV === 'test',
  retryStrategy(times) {
    if (env.NODE_ENV === 'test') {
      return null; // Don't hang tests with retries if Redis is unavailable
    }
    const delay = Math.min(times * 50, 2000);
    logger.warn({ attempt: times, nextDelayMs: delay }, 'Retrying Redis connection...');
    return delay;
  },
});

export const createRedisConnection = (): Redis => {
  const baseOptions = getBaseRedisOptions();

  if (env.REDIS_URL) {
    return new Redis(env.REDIS_URL, baseOptions);
  }

  return new Redis({
    host: env.REDIS_HOST,
    port: env.REDIS_PORT,
    password: env.REDIS_PASSWORD || undefined,
    ...baseOptions,
  });
};

// General-purpose Redis client for Caching, Idempotency, Rate Limiting
export const redisClient = createRedisConnection();

redisClient.on('connect', () => {
  if (env.REDIS_URL) {
    logger.info('Redis client connected successfully via canonical REDIS_URL');
  } else {
    logger.info({ host: env.REDIS_HOST, port: env.REDIS_PORT }, 'Redis client connected successfully via host/port fallback');
  }
});

redisClient.on('error', (err) => {
  logger.error({ err: err.message }, 'Redis client connection error');
});

// Helper factory for BullMQ connections (BullMQ creates its own dedicated connections)
export const createBullMQRedisConnection = (): Redis => {
  return createRedisConnection();
};

