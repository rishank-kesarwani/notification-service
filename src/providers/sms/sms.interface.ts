import { NotificationRecipient, ProviderResponse, SmsPayload } from '../../types/notification';

export interface SendSmsOptions {
  recipient: NotificationRecipient;
  sms: SmsPayload;
  notificationId: string;
}

export interface ISmsProvider {
  name: string;
  send(options: SendSmsOptions): Promise<ProviderResponse>;
}
