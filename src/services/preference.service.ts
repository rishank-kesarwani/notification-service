import { logger } from '../config/logger';
import { redisClient } from '../config/redis';
import { NotificationChannel, NotificationPriority, UserPreference } from '../types/notification';

export class UserPreferenceService {
  private readonly prefix = 'user:preferences:';
  private readonly defaultTtl = 3600; // 1 hour

  /**
   * Retrieves user preferences from Redis cache or defaults
   */
  async getUserPreferences(userId: string): Promise<UserPreference> {
    const key = `${this.prefix}${userId}`;
    const cached = await redisClient.get(key);

    if (cached) {
      try {
        return JSON.parse(cached) as UserPreference;
      } catch {
        logger.warn({ userId }, 'Failed to parse cached user preferences');
      }
    }

    // Default preferences: not opted out of anything
    const defaultPreferences: UserPreference = {
      userId,
      emailOptOut: false,
      pushOptOut: false,
      bulkOptOut: false,
    };

    // Store defaults in cache for subsequent calls
    await this.setUserPreferences(defaultPreferences);

    return defaultPreferences;
  }

  /**
   * Updates or sets user preferences in Redis
   */
  async setUserPreferences(preferences: UserPreference, ttlSeconds: number = this.defaultTtl): Promise<void> {
    const key = `${this.prefix}${preferences.userId}`;
    await redisClient.set(key, JSON.stringify(preferences), 'EX', ttlSeconds);
    logger.debug({ userId: preferences.userId }, 'User preferences updated in cache');
  }

  /**
   * Evaluates if a notification should be delivered to the given channel for the user
   * Critical notifications always bypass bulk opt-outs.
   */
  async isChannelAllowed(
    userId: string,
    channel: NotificationChannel,
    priority: NotificationPriority
  ): Promise<{ allowed: boolean; reason?: string }> {
    const preferences = await this.getUserPreferences(userId);

    // Channel-specific hard opt-outs apply to both critical and bulk (unless critical security overrides apply)
    if (channel === 'EMAIL' && preferences.emailOptOut) {
      return { allowed: false, reason: 'User has opted out of all EMAIL notifications' };
    }

    if (channel === 'PUSH' && preferences.pushOptOut) {
      return { allowed: false, reason: 'User has opted out of all PUSH notifications' };
    }

    // Bulk opt-out only applies to BULK priority
    if (priority === 'BULK' && preferences.bulkOptOut) {
      return { allowed: false, reason: 'User has opted out of BULK promotional/newsletter notifications' };
    }

    return { allowed: true };
  }
}

export const userPreferenceService = new UserPreferenceService();
