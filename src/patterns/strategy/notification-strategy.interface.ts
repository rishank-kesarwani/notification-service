import { ChannelJobData, NotificationChannel, ProviderResponse } from '../../types/notification';

export interface INotificationChannelStrategy<T extends ChannelJobData = ChannelJobData> {
  readonly channel: NotificationChannel;

  /**
   * Dispatches the channel-specific notification with rate limiting, circuit breaker protection,
   * and fallback mechanisms.
   */
  process(jobData: T): Promise<ProviderResponse>;
}
