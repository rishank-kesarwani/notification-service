import { logger } from '../../config/logger';
import { ProviderResponse } from '../../types/notification';
import { IEmailProvider, SendEmailOptions } from './email.interface';
import { ResendEmailProvider } from './resend.provider';
import { SmtpEmailProvider } from './smtp.provider';

export class EmailFallbackService {
  private primaryProvider: IEmailProvider;
  private fallbackProvider: IEmailProvider;

  constructor(
    primaryProvider: IEmailProvider = new ResendEmailProvider(),
    fallbackProvider: IEmailProvider = new SmtpEmailProvider()
  ) {
    this.primaryProvider = primaryProvider;
    this.fallbackProvider = fallbackProvider;
  }

  async sendEmail(options: SendEmailOptions): Promise<ProviderResponse> {
    const { notificationId, recipient } = options;

    try {
      logger.info(
        { notificationId, to: recipient.email, provider: this.primaryProvider.name },
        'Dispatching email via primary provider'
      );
      return await this.primaryProvider.send(options);
    } catch (primaryError: unknown) {
      const errorMessage = primaryError instanceof Error ? primaryError.message : String(primaryError);

      logger.warn(
        {
          notificationId,
          primaryProvider: this.primaryProvider.name,
          error: errorMessage,
          fallbackProvider: this.fallbackProvider.name,
        },
        'Primary email provider failed. Activating automatic fallback provider...'
      );

      try {
        const fallbackResult = await this.fallbackProvider.send(options);
        logger.info(
          {
            notificationId,
            fallbackProvider: this.fallbackProvider.name,
            messageId: fallbackResult.messageId,
          },
          'Email successfully delivered via fallback provider'
        );
        return fallbackResult;
      } catch (fallbackError: unknown) {
        const fallbackErrorMessage = fallbackError instanceof Error ? fallbackError.message : String(fallbackError);

        logger.error(
          {
            notificationId,
            primaryError: errorMessage,
            fallbackError: fallbackErrorMessage,
          },
          'Both primary and fallback email providers failed'
        );

        throw new Error(
          `All email providers failed. Primary (${this.primaryProvider.name}): "${errorMessage}". Fallback (${this.fallbackProvider.name}): "${fallbackErrorMessage}"`
        );
      }
    }
  }
}
