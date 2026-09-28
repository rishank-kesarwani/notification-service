import { env } from '../../config/env';
import { logger } from '../../config/logger';
import { HttpSmsProvider } from '../../providers/sms/httpsms.provider';
import { ISmsProvider } from '../../providers/sms/sms.interface';
import { SmsFallbackService } from '../../providers/sms/sms-fallback.service';
import { TextBeeSmsProvider } from '../../providers/sms/textbee.provider';
import { vendorRateLimiter } from '../../services/rate-limiter.service';
import { ProviderResponse, SmsJobData } from '../../types/notification';
import { CircuitBreaker } from '../circuit-breaker/circuit-breaker';
import { INotificationChannelStrategy } from './notification-strategy.interface';

export class SmsNotificationStrategy implements INotificationChannelStrategy<SmsJobData> {
  public readonly channel = 'SMS' as const;

  private fallbackService: SmsFallbackService;

  constructor(
    primaryProvider: ISmsProvider = new HttpSmsProvider(),
    fallbackProvider: ISmsProvider = new TextBeeSmsProvider(),
    fallbackService?: SmsFallbackService
  ) {
    this.fallbackService = fallbackService || new SmsFallbackService(primaryProvider, fallbackProvider);
  }

  get httpSmsCircuitBreaker(): CircuitBreaker {
    return this.fallbackService.httpSmsCircuitBreaker;
  }

  get textBeeCircuitBreaker(): CircuitBreaker {
    return this.fallbackService.textBeeCircuitBreaker;
  }

  async process(jobData: SmsJobData): Promise<ProviderResponse> {
    const { notificationId, recipient, sms } = jobData;

    // 1. Sliding Window Vendor Rate-Limit Check
    const rateLimitCheck = await vendorRateLimiter.checkRateLimit(
      'SMS_VENDOR',
      env.SMS_RATE_LIMIT_MAX,
      env.SMS_RATE_LIMIT_WINDOW_MS
    );

    if (!rateLimitCheck.allowed) {
      const retryDelay = rateLimitCheck.retryAfterMs || 1000;
      throw new Error(`Rate limit exceeded for SMS_VENDOR. Retry in ${retryDelay}ms`);
    }

    // 2. Dispatch via Circuit-Breaker protected Fallback Service
    try {
      return await this.fallbackService.sendSms({
        recipient,
        sms,
        notificationId,
      });
    } catch (error: unknown) {
      const errorMsg = error instanceof Error ? error.message : String(error);
      logger.error(
        {
          notificationId,
          to: sms.to || recipient.phone,
          error: errorMsg,
          httpSmsCircuitState: this.httpSmsCircuitBreaker.getState(),
          textBeeCircuitState: this.textBeeCircuitBreaker.getState(),
        },
        'SMS delivery failed in SmsNotificationStrategy'
      );
      throw error;
    }
  }
}

export { SmsNotificationStrategy as SmsStrategy };
