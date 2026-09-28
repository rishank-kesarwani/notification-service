import nodemailer, { Transporter } from 'nodemailer';
import { env } from '../../config/env';
import { logger } from '../../config/logger';
import { ProviderResponse } from '../../types/notification';
import { IEmailProvider, SendEmailOptions } from './email.interface';

export class SmtpEmailProvider implements IEmailProvider {
  public readonly name = 'NODEMAILER_SMTP';
  private transporter: Transporter | null = null;

  constructor() {
    if (env.SMTP_USER && env.SMTP_PASS) {
      this.transporter = nodemailer.createTransport({
        host: env.SMTP_HOST,
        port: env.SMTP_PORT,
        secure: env.SMTP_PORT === 465,
        auth: {
          user: env.SMTP_USER,
          pass: env.SMTP_PASS,
        },
        // Timeout after 10 seconds for resilient failover
        connectionTimeout: 10000,
        greetingTimeout: 10000,
        socketTimeout: 15000,
      });
      logger.info({ host: env.SMTP_HOST, port: env.SMTP_PORT }, 'SMTP fallback transporter initialized');
    } else {
      logger.warn('SMTP credentials not configured. SMTP fallback will fail if invoked.');
    }
  }

  async send(options: SendEmailOptions): Promise<ProviderResponse> {
    const { recipient, email, notificationId } = options;

    if (!this.transporter) {
      throw new Error('SMTP transporter is not initialized due to missing SMTP credentials');
    }

    if (!recipient.email) {
      throw new Error('Recipient email is required for SMTP provider');
    }

    const fromAddress = email.from || env.SMTP_FROM || env.SMTP_USER || 'noreply@notification-service.com';

    logger.debug(
      { notificationId, to: recipient.email, from: fromAddress, provider: this.name },
      'Attempting to send email via SMTP fallback provider'
    );

    const info = await this.transporter.sendMail({
      from: fromAddress,
      to: recipient.email,
      subject: email.subject,
      text: email.text,
      html: email.html,
      replyTo: email.replyTo,
      attachments: email.attachments?.map((att) => ({
        filename: att.filename,
        path: att.path,
        content: att.content ? Buffer.from(att.content, 'base64') : undefined,
        contentType: att.contentType,
      })),
      headers: {
        'X-Notification-ID': notificationId,
      },
    });

    logger.info(
      { notificationId, messageId: info.messageId, response: info.response, provider: this.name },
      'Email successfully sent via SMTP fallback'
    );

    return {
      success: true,
      provider: this.name,
      messageId: info.messageId,
      fallbackUsed: true,
    };
  }
}
