export type NotificationChannel = 'EMAIL' | 'PUSH';

export type NotificationPriority = 'CRITICAL' | 'BULK';

export type EmailQueueName = 'email_critical' | 'email_bulk' | 'email_dlq';
export type PushQueueName = 'push_critical' | 'push_bulk' | 'push_dlq';
export type QueueName = EmailQueueName | PushQueueName;

export interface NotificationRecipient {
  userId: string;
  email?: string;
  pushToken?: string;
}

export interface EmailAttachment {
  filename: string;
  path?: string; // Public URL, Signed S3/GCS URL, or file path
  content?: string; // Base64 encoded string
  contentType?: string; // e.g. 'application/pdf'
}

export interface EmailPayload {
  subject: string;
  html?: string;
  text?: string;
  from?: string;
  replyTo?: string;
  tags?: Record<string, string>;
  attachments?: EmailAttachment[];
}

export interface PushPayload {
  title: string;
  body: string;
  data?: Record<string, string>;
  imageUrl?: string;
}

export interface NotificationRequestPayload {
  idempotencyKey?: string;
  priority: NotificationPriority;
  channels: NotificationChannel[];
  recipient: NotificationRecipient;
  email?: EmailPayload;
  push?: PushPayload;
  metadata?: Record<string, unknown>;
}

export interface BaseJobData {
  notificationId: string;
  idempotencyKey?: string;
  priority: NotificationPriority;
  recipient: NotificationRecipient;
  metadata?: Record<string, unknown>;
  createdAt: string;
  attemptsMade?: number;
}

export interface EmailJobData extends BaseJobData {
  channel: 'EMAIL';
  email: EmailPayload;
}

export interface PushJobData extends BaseJobData {
  channel: 'PUSH';
  push: PushPayload;
}

export type ChannelJobData = EmailJobData | PushJobData;

export interface ProviderResponse {
  success: boolean;
  provider: string;
  messageId?: string;
  error?: string;
  fallbackUsed?: boolean;
}

export interface UserPreference {
  userId: string;
  emailOptOut: boolean;
  pushOptOut: boolean;
  bulkOptOut: boolean;
}

export interface IngestionResponse {
  success: boolean;
  message: string;
  notificationId: string;
  idempotencyStatus: 'PROCESSED' | 'CACHED';
  enqueuedChannels: {
    channel: NotificationChannel;
    queue: QueueName;
    jobId: string;
  }[];
  skippedChannels?: {
    channel: NotificationChannel;
    reason: string;
  }[];
}
