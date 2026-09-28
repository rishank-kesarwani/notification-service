import { env } from '../../config/env';
import { logger } from '../../config/logger';
import { ProviderResponse } from '../../types/notification';
import { ISmsProvider, SendSmsOptions } from './sms.interface';

export class HttpSmsProvider implements ISmsProvider {
  public readonly name = 'HTTPSMS';
  private readonly baseUrl = 'https://api.httpsms.com/v1';

  constructor() {
    if (!env.HTTPSMS_API_KEY) {
      logger.warn('httpSMS API key is not configured. httpSMS provider will fail over to fallback.');
    }
  }

  async send(options: SendSmsOptions): Promise<ProviderResponse> {
    const { recipient, sms, notificationId } = options;

    if (!env.HTTPSMS_API_KEY) {
      throw new Error('httpSMS client is not configured due to missing HTTPSMS_API_KEY');
    }

    const toNumber = sms.to || recipient.phone;
    if (!toNumber) {
      throw new Error('Recipient phone number is required for httpSMS provider');
    }

    const fromNumber = sms.from || env.HTTPSMS_FROM_NUMBER;
    if (!fromNumber) {
      throw new Error('Sender phone number (HTTPSMS_FROM_NUMBER or sms.from) is required for httpSMS provider');
    }

    logger.debug(
      { notificationId, to: toNumber, from: fromNumber, provider: this.name },
      'Sending SMS via httpSMS Android SIM & FCM Gateway'
    );

    const payload = {
      content: sms.message,
      from: fromNumber,
      to: toNumber,
    };

    try {
      const response = await fetch(`${this.baseUrl}/messages/send`, {
        method: 'POST',
        headers: {
          'x-api-key': env.HTTPSMS_API_KEY,
          'Content-Type': 'application/json',
          'Accept': 'application/json',
        },
        body: JSON.stringify(payload),
      });

      if (!response.ok) {
        const errorText = await response.text();
        logger.error(
          { notificationId, status: response.status, statusText: response.statusText, error: errorText, provider: this.name },
          'httpSMS API returned an error response'
        );
        throw new Error(`httpSMS Error (${response.status}): ${errorText}`);
      }

      const responseData = (await response.json()) as { data?: { id?: string }; id?: string; status?: string };
      const messageId = responseData.data?.id || responseData.id || notificationId;

      logger.info(
        { notificationId, messageId, provider: this.name },
        'SMS successfully sent via httpSMS'
      );

      return {
        success: true,
        provider: this.name,
        messageId,
        fallbackUsed: false,
      };
    } catch (error: unknown) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      logger.error(
        { notificationId, error: errorMessage, provider: this.name },
        'httpSMS send failure'
      );
      throw error instanceof Error ? error : new Error(errorMessage);
    }
  }
}
