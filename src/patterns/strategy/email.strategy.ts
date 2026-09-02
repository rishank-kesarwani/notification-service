import { env } from '../../config/env';
import { logger } from '../../config/logger';
import { ResendEmailProvider } from '../../providers/email/resend.provider';
import { SmtpEmailProvider } from '../../providers/email/smtp.provider';
import { IEmailProvider } from '../../providers/email/email.interface';
import { vendorRateLimiter } from '../../services/rate-limiter.service';
import { EmailJobData, ProviderResponse } from '../../types/notification';
import { CircuitBreaker } from '../circuit-breaker/circuit-breaker';
import { INotificationChannelStrategy } from './notification-strategy.interface';

export class EmailNotificationStrategy implements INotificationChannelStrategy<EmailJobData> {
  public readonly channel = 'EMAIL' as const;

  private primaryProvider: IEmailProvider;
  private fallbackProvider: IEmailProvider;

  // Circuit Breakers for each downstream provider
  public readonly resendCircuitBreaker: CircuitBreaker;
  public readonly smtpCircuitBreaker: CircuitBreaker;

  constructor(
    primaryProvider: IEmailProvider = new ResendEmailProvider(),
    fallbackProvider: IEmailProvider = new SmtpEmailProvider()
  ) {
    this.primaryProvider = primaryProvider;
    this.fallbackProvider = fallbackProvider;

    this.resendCircuitBreaker = new CircuitBreaker({
      name: 'Resend_API_Breaker',
      failureThreshold: 3, // Trip after 3 consecutive failures
      recoveryTimeoutMs: 20000, // 20s cooldown
      successThreshold: 2,
    });

    this.smtpCircuitBreaker = new CircuitBreaker({
      name: 'Nodemailer_SMTP_Breaker',
      failureThreshold: 3,
      recoveryTimeoutMs: 30000,
      successThreshold: 2,
    });
  }

  async process(jobData: EmailJobData): Promise<ProviderResponse> {
    const { notificationId, recipient, email } = jobData;

    // 1. Sliding Window Vendor Rate-Limit Check
    const rateLimitCheck = await vendorRateLimiter.checkRateLimit(
      'EMAIL_VENDOR',
      env.EMAIL_RATE_LIMIT_MAX,
      env.EMAIL_RATE_LIMIT_WINDOW_MS
    );

    if (!rateLimitCheck.allowed) {
      const retryDelay = rateLimitCheck.retryAfterMs || 1000;
      throw new Error(`Rate limit exceeded for EMAIL_VENDOR. Retry in ${retryDelay}ms`);
    }

    // 2. Try Primary Provider (Resend) protected by Circuit Breaker
    try {
      return await this.resendCircuitBreaker.execute(async () => {
        return await this.primaryProvider.send({
          recipient,
          email,
          notificationId,
        });
      });
    } catch (primaryError: unknown) {
      const primaryMsg = primaryError instanceof Error ? primaryError.message : String(primaryError);

      logger.warn(
        {
          notificationId,
          primaryError: primaryMsg,
          resendCircuitState: this.resendCircuitBreaker.getState(),
        },
        'Primary email provider failed or circuit OPEN. Failing over to Nodemailer SMTP...'
      );

      // 3. Failover to Fallback Provider (SMTP) protected by Circuit Breaker
      try {
        const fallbackResult = await this.smtpCircuitBreaker.execute(async () => {
          return await this.fallbackProvider.send({
            recipient,
            email,
            notificationId,
          });
        });

        logger.info(
          { notificationId, fallbackProvider: this.fallbackProvider.name },
          'Email successfully dispatched via fallback provider strategy'
        );

        return fallbackResult;
      } catch (fallbackError: unknown) {
        const fallbackMsg = fallbackError instanceof Error ? fallbackError.message : String(fallbackError);

        logger.error(
          {
            notificationId,
            primaryError: primaryMsg,
            fallbackError: fallbackMsg,
            smtpCircuitState: this.smtpCircuitBreaker.getState(),
          },
          'All email strategies and fallback circuit breakers exhausted'
        );

        throw new Error(
          `Email delivery failed across all providers. Primary: "${primaryMsg}". Fallback: "${fallbackMsg}"`
        );
      }
    }
  }
}
