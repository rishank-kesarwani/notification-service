import { env } from '../../config/env';
import { logger } from '../../config/logger';
import { ProviderResponse } from '../../types/notification';
import { ISmsProvider, SendSmsOptions } from './sms.interface';

export class TextBeeSmsProvider implements ISmsProvider {
  public readonly name = 'TEXTBEE';
  private readonly baseUrl = 'https://api.textbee.dev/api/v1';

  constructor() {
    if (!env.TEXTBEE_API_KEY || !env.TEXTBEE_DEVICE_ID) {
      logger.warn('TextBee API key or Device ID is not configured.');
    }
  }

  async send(options: SendSmsOptions): Promise<ProviderResponse> {
    const { recipient, sms, notificationId } = options;

    if (!env.TEXTBEE_API_KEY || !env.TEXTBEE_DEVICE_ID) {
      throw new Error('TextBee client is not configured due to missing TEXTBEE_API_KEY or TEXTBEE_DEVICE_ID');
    }

    const toNumber = sms.to || recipient.phone;
    if (!toNumber) {
      throw new Error('Recipient phone number is required for TextBee provider');
    }

    logger.debug(
      { notificationId, to: toNumber, deviceId: env.TEXTBEE_DEVICE_ID, provider: this.name },
      'Sending SMS via TextBee open-source gateway'
    );

    const payload = {
      recipients: [toNumber],
      message: sms.message,
    };

    const url = `${this.baseUrl}/gateway/devices/${env.TEXTBEE_DEVICE_ID}/send-sms`;

    try {
      const response = await fetch(url, {
        method: 'POST',
        headers: {
          'x-api-key': env.TEXTBEE_API_KEY,
          'Content-Type': 'application/json',
          'Accept': 'application/json',
        },
        body: JSON.stringify(payload),
      });

      if (!response.ok) {
        const errorText = await response.text();
        logger.error(
          { notificationId, status: response.status, statusText: response.statusText, error: errorText, provider: this.name },
          'TextBee API returned an error response'
        );
        throw new Error(`TextBee Error (${response.status}): ${errorText}`);
      }

      const responseData = (await response.json()) as { data?: { id?: string; messageId?: string }; messageId?: string; success?: boolean; id?: string };
      const messageId = responseData.data?.id || responseData.data?.messageId || responseData.messageId || responseData.id || notificationId;

      logger.info(
        { notificationId, messageId, provider: this.name },
        'SMS successfully sent via TextBee fallback provider'
      );

      return {
        success: true,
        provider: this.name,
        messageId,
        fallbackUsed: true,
      };
    } catch (error: unknown) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      logger.error(
        { notificationId, error: errorMessage, provider: this.name },
        'TextBee send failure'
      );
      throw error instanceof Error ? error : new Error(errorMessage);
    }
  }
}
