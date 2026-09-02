import { EmailPayload, NotificationRecipient, ProviderResponse } from '../../types/notification';

export interface SendEmailOptions {
  recipient: NotificationRecipient;
  email: EmailPayload;
  notificationId: string;
}

export interface IEmailProvider {
  name: string;
  send(options: SendEmailOptions): Promise<ProviderResponse>;
}
