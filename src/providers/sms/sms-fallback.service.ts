import { logger } from '../../config/logger';
import { CircuitBreaker } from '../../patterns/circuit-breaker/circuit-breaker';
import { ProviderResponse } from '../../types/notification';
import { HttpSmsProvider } from './httpsms.provider';
import { ISmsProvider, SendSmsOptions } from './sms.interface';
import { TextBeeSmsProvider } from './textbee.provider';

export class SmsFallbackService {
  private primaryProvider: ISmsProvider;
  private fallbackProvider: ISmsProvider;

  // Circuit Breakers for SMS providers (Trip to OPEN after 3 failures, 30s cooldown)
  public readonly httpSmsCircuitBreaker: CircuitBreaker;
  public readonly textBeeCircuitBreaker: CircuitBreaker;

  constructor(
    primaryProvider: ISmsProvider = new HttpSmsProvider(),
    fallbackProvider: ISmsProvider = new TextBeeSmsProvider()
  ) {
    this.primaryProvider = primaryProvider;
    this.fallbackProvider = fallbackProvider;

    this.httpSmsCircuitBreaker = new CircuitBreaker({
      name: 'HttpSMS_Breaker',
      failureThreshold: 3, // Trip to OPEN after 3 consecutive failures
      recoveryTimeoutMs: 30000, // 30s cooldown before probing in HALF_OPEN
      successThreshold: 2,
    });

    this.textBeeCircuitBreaker = new CircuitBreaker({
      name: 'TextBee_Breaker',
      failureThreshold: 3,
      recoveryTimeoutMs: 30000,
      successThreshold: 2,
    });
  }

  async sendSms(options: SendSmsOptions): Promise<ProviderResponse> {
    const { notificationId, recipient, sms } = options;
    const toNumber = sms.to || recipient.phone;

    // 1. Try Primary SMS Provider (httpSMS) protected by Circuit Breaker
    try {
      logger.info(
        {
          notificationId,
          to: toNumber,
          provider: this.primaryProvider.name,
          circuitState: this.httpSmsCircuitBreaker.getState(),
        },
        'Dispatching SMS via primary httpSMS provider'
      );

      return await this.httpSmsCircuitBreaker.execute(async () => {
        return await this.primaryProvider.send(options);
      });
    } catch (primaryError: unknown) {
      const primaryMessage = primaryError instanceof Error ? primaryError.message : String(primaryError);

      logger.warn(
        {
          notificationId,
          to: toNumber,
          primaryProvider: this.primaryProvider.name,
          primaryCircuitState: this.httpSmsCircuitBreaker.getState(),
          error: primaryMessage,
          fallbackProvider: this.fallbackProvider.name,
        },
        'Primary SMS provider failed or circuit is OPEN. Activating TextBee failover...'
      );

      // 2. Try Fallback SMS Provider (TextBee) protected by Circuit Breaker
      try {
        const fallbackResult = await this.textBeeCircuitBreaker.execute(async () => {
          return await this.fallbackProvider.send(options);
        });

        logger.info(
          {
            notificationId,
            fallbackProvider: this.fallbackProvider.name,
            messageId: fallbackResult.messageId,
          },
          'SMS successfully delivered via TextBee fallback provider'
        );

        return fallbackResult;
      } catch (fallbackError: unknown) {
        const fallbackMessage = fallbackError instanceof Error ? fallbackError.message : String(fallbackError);

        logger.error(
          {
            notificationId,
            primaryError: primaryMessage,
            fallbackError: fallbackMessage,
            primaryCircuitState: this.httpSmsCircuitBreaker.getState(),
            fallbackCircuitState: this.textBeeCircuitBreaker.getState(),
          },
          'Both primary (httpSMS) and fallback (TextBee) SMS providers failed'
        );

        throw new Error(
          `All SMS providers failed. Primary (${this.primaryProvider.name}): "${primaryMessage}". Fallback (${this.fallbackProvider.name}): "${fallbackMessage}"`
        );
      }
    }
  }

  getPrimaryProvider(): ISmsProvider {
    return this.primaryProvider;
  }

  getFallbackProvider(): ISmsProvider {
    return this.fallbackProvider;
  }
}
