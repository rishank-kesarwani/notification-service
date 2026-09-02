import { redisClient } from '../config/redis';
import { logger } from '../config/logger';

export interface RateLimitResult {
  allowed: boolean;
  retryAfterMs?: number;
  currentCount?: number;
}

export class VendorRateLimiterService {
  private readonly prefix = 'ratelimit:';

  // Lua script for atomic sliding window rate limiting
  private readonly slidingWindowLuaScript = `
    local key = KEYS[1]
    local now = tonumber(ARGV[1])
    local windowMs = tonumber(ARGV[2])
    local maxLimit = tonumber(ARGV[3])
    local memberId = ARGV[4]
    local clearBefore = now - windowMs

    -- Remove expired timestamps older than sliding window
    redis.call('ZREMRANGEBYSCORE', key, '-inf', clearBefore)

    -- Count existing requests in current window
    local currentCount = redis.call('ZCARD', key)

    if currentCount < maxLimit then
      -- Add current request timestamp
      redis.call('ZADD', key, now, memberId)
      -- Set TTL on key
      redis.call('PEXPIRE', key, windowMs * 2)
      return {1, 0, currentCount + 1}
    else
      -- Fetch the oldest entry to calculate precise retry delay
      local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
      local retryAfterMs = windowMs
      if oldest and oldest[2] then
        local oldestTimestamp = tonumber(oldest[2])
        retryAfterMs = math.max(1, (oldestTimestamp + windowMs) - now)
      end
      return {0, retryAfterMs, currentCount}
    end
  `;

  /**
   * Evaluates if a request is permitted under the vendor's sliding window rate limit.
   */
  async checkRateLimit(
    vendor: string,
    maxLimit: number,
    windowMs: number
  ): Promise<RateLimitResult> {
    const key = `${this.prefix}${vendor}`;
    const now = Date.now();
    const memberId = `${now}-${Math.random().toString(36).substring(2, 9)}`;

    try {
      const result = (await redisClient.eval(
        this.slidingWindowLuaScript,
        1,
        key,
        now.toString(),
        windowMs.toString(),
        maxLimit.toString(),
        memberId
      )) as [number, number, number];

      const allowed = result[0] === 1;
      const retryAfterMs = result[1];
      const currentCount = result[2];

      if (!allowed) {
        logger.warn(
          { vendor, currentCount, maxLimit, retryAfterMs },
          'Vendor rate limit reached. Backoff required.'
        );
      }

      return {
        allowed,
        retryAfterMs: allowed ? 0 : retryAfterMs,
        currentCount,
      };
    } catch (err: unknown) {
      const errorMsg = err instanceof Error ? err.message : String(err);
      logger.error({ vendor, error: errorMsg }, 'Error executing rate limit check. Defaulting to allow.');
      // Fail open on rate limiter check error to avoid halting processing
      return { allowed: true };
    }
  }

  /**
   * Helper that blocks/sleeps until a rate limit token becomes available, up to maxWaitMs
   */
  async waitForToken(
    vendor: string,
    maxLimit: number,
    windowMs: number,
    maxWaitMs = 5000
  ): Promise<boolean> {
    const startTime = Date.now();

    while (Date.now() - startTime < maxWaitMs) {
      const result = await this.checkRateLimit(vendor, maxLimit, windowMs);
      if (result.allowed) {
        return true;
      }
      const sleepTime = Math.min(result.retryAfterMs || 100, 500);
      await new Promise((resolve) => setTimeout(resolve, sleepTime));
    }

    return false;
  }
}

export const vendorRateLimiter = new VendorRateLimiterService();
