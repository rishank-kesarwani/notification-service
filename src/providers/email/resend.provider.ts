import { Resend } from 'resend';
import { env } from '../../config/env';
import { logger } from '../../config/logger';
import { ProviderResponse } from '../../types/notification';
import { IEmailProvider, SendEmailOptions } from './email.interface';

export class ResendEmailProvider implements IEmailProvider {
  public readonly name = 'RESEND';
  private resend: Resend | null = null;

  constructor() {
    if (env.RESEND_API_KEY) {
      this.resend = new Resend(env.RESEND_API_KEY);
    } else {
      logger.warn('Resend API key is not configured. Resend provider will fail over to fallback.');
    }
  }

  async send(options: SendEmailOptions): Promise<ProviderResponse> {
    const { recipient, email, notificationId } = options;

    if (!this.resend) {
      throw new Error('Resend client is not initialized due to missing RESEND_API_KEY');
    }

    if (!recipient.email) {
      throw new Error('Recipient email is required for Resend provider');
    }

    const fromAddress = email.from || env.EMAIL_FROM;

    logger.debug(
      { notificationId, to: recipient.email, from: fromAddress, provider: this.name },
      'Attempting to send email via Resend primary provider'
    );

    const emailPayload = {
      from: fromAddress,
      to: [recipient.email],
      subject: email.subject,
      ...(email.html ? { html: email.html } : {}),
      ...(email.text ? { text: email.text } : {}),
      ...(email.replyTo ? { replyTo: email.replyTo } : {}),
      ...(email.attachments && email.attachments.length > 0
        ? {
            attachments: email.attachments.map((att) => ({
              filename: att.filename,
              path: att.path,
              content: att.content ? Buffer.from(att.content, 'base64') : undefined,
            })),
          }
        : {}),
      headers: {
        'X-Notification-ID': notificationId,
      },
    } as Parameters<Resend['emails']['send']>[0];

    const response = await this.resend.emails.send(emailPayload);

    if (response.error) {
      logger.error(
        { notificationId, error: response.error, provider: this.name },
        'Resend API returned an error response'
      );
      throw new Error(`Resend Error: ${response.error.name} - ${response.error.message}`);
    }

    logger.info(
      { notificationId, messageId: response.data?.id, provider: this.name },
      'Email successfully sent via Resend'
    );

    return {
      success: true,
      provider: this.name,
      messageId: response.data?.id,
      fallbackUsed: false,
    };
  }
}
