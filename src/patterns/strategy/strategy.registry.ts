import { NotificationChannel } from '../../types/notification';
import { EmailNotificationStrategy } from './email.strategy';
import { INotificationChannelStrategy } from './notification-strategy.interface';
import { PushNotificationStrategy } from './push.strategy';

export class NotificationStrategyRegistry {
  private strategies: Map<NotificationChannel, INotificationChannelStrategy> = new Map();

  constructor() {
    this.registerStrategy(new EmailNotificationStrategy());
    this.registerStrategy(new PushNotificationStrategy());
  }

  public registerStrategy(strategy: INotificationChannelStrategy): void {
    this.strategies.set(strategy.channel, strategy);
  }

  public getStrategy<T extends INotificationChannelStrategy>(channel: NotificationChannel): T {
    const strategy = this.strategies.get(channel);
    if (!strategy) {
      throw new Error(`No notification strategy registered for channel: ${channel}`);
    }
    return strategy as T;
  }

  public getEmailStrategy(): EmailNotificationStrategy {
    return this.getStrategy<EmailNotificationStrategy>('EMAIL');
  }

  public getPushStrategy(): PushNotificationStrategy {
    return this.getStrategy<PushNotificationStrategy>('PUSH');
  }

  public getAllCircuitBreakerDiagnostics() {
    const emailStrategy = this.getEmailStrategy();
    const pushStrategy = this.getPushStrategy();

    return [
      emailStrategy.resendCircuitBreaker.getDiagnostics(),
      emailStrategy.smtpCircuitBreaker.getDiagnostics(),
      pushStrategy.fcmCircuitBreaker.getDiagnostics(),
    ];
  }
}

export const strategyRegistry = new NotificationStrategyRegistry();
