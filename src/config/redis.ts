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
  const redisUrl = (env.REDIS_URL || process.env.REDIS_URL || '').trim();
  const baseOptions = getBaseRedisOptions();

  if (redisUrl) {
    return new Redis(redisUrl, {
      ...baseOptions,
    });
  }

  const host = (env.REDIS_HOST || process.env.REDIS_HOST || 'localhost').trim() || 'localhost';
  const port = Number(env.REDIS_PORT || process.env.REDIS_PORT || 6379);
  const password = env.REDIS_PASSWORD || process.env.REDIS_PASSWORD || undefined;
  const db = Number(env.REDIS_DB || process.env.REDIS_DB || 0);

  return new Redis({
    host,
    port,
    password: password || undefined,
    db,
    ...baseOptions,
  });
};

// General-purpose Redis client for Caching, Idempotency, Rate Limiting
export const redisClient = createRedisConnection();

redisClient.on('connect', () => {
  const redisUrl = (env.REDIS_URL || process.env.REDIS_URL || '').trim();
  if (redisUrl) {
    logger.info('Connected to Redis via connection URL');
  } else {
    const host = (env.REDIS_HOST || process.env.REDIS_HOST || 'localhost').trim() || 'localhost';
    const port = Number(env.REDIS_PORT || process.env.REDIS_PORT || 6379);
    logger.info(`Connected to Redis at ${host}:${port}`);
  }
});

redisClient.on('error', (err) => {
  logger.error({ err: err.message }, 'Redis client connection error');
});

// Helper factory for BullMQ connections (BullMQ creates its own dedicated connections)
export const createBullMQRedisConnection = (): Redis => {
  return createRedisConnection();
};

