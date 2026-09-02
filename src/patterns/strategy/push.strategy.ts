import { env } from '../../config/env';
import { FcmPushProvider } from '../../providers/push/fcm.provider';
import { IPushProvider } from '../../providers/push/push.interface';
import { vendorRateLimiter } from '../../services/rate-limiter.service';
import { ProviderResponse, PushJobData } from '../../types/notification';
import { CircuitBreaker } from '../circuit-breaker/circuit-breaker';
import { INotificationChannelStrategy } from './notification-strategy.interface';

export class PushNotificationStrategy implements INotificationChannelStrategy<PushJobData> {
  public readonly channel = 'PUSH' as const;

  private pushProvider: IPushProvider;
  public readonly fcmCircuitBreaker: CircuitBreaker;

  constructor(pushProvider: IPushProvider = new FcmPushProvider()) {
    this.pushProvider = pushProvider;
    this.fcmCircuitBreaker = new CircuitBreaker({
      name: 'Firebase_FCM_Breaker',
      failureThreshold: 4,
      recoveryTimeoutMs: 25000,
      successThreshold: 2,
    });
  }

  async process(jobData: PushJobData): Promise<ProviderResponse> {
    const { notificationId, recipient, push } = jobData;

    // 1. Sliding Window Vendor Rate-Limit Check
    const rateLimitCheck = await vendorRateLimiter.checkRateLimit(
      'PUSH_VENDOR',
      env.PUSH_RATE_LIMIT_MAX,
      env.PUSH_RATE_LIMIT_WINDOW_MS
    );

    if (!rateLimitCheck.allowed) {
      const retryDelay = rateLimitCheck.retryAfterMs || 1000;
      throw new Error(`Rate limit exceeded for PUSH_VENDOR. Retry in ${retryDelay}ms`);
    }

    // 2. Dispatch via FCM protected by Circuit Breaker
    return await this.fcmCircuitBreaker.execute(async () => {
      return await this.pushProvider.send({
        recipient,
        push,
        notificationId,
      });
    });
  }
}
