import { NotificationRecipient, ProviderResponse, PushPayload } from '../../types/notification';

export interface SendPushOptions {
  recipient: NotificationRecipient;
  push: PushPayload;
  notificationId: string;
}

export interface IPushProvider {
  name: string;
  send(options: SendPushOptions): Promise<ProviderResponse>;
}
