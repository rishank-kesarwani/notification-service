import admin from 'firebase-admin';
import { env } from '../../config/env';
import { logger } from '../../config/logger';
import { ProviderResponse } from '../../types/notification';
import { IPushProvider, SendPushOptions } from './push.interface';

export class FcmPushProvider implements IPushProvider {
  public readonly name = 'FIREBASE_FCM';
  private initialized = false;

  constructor() {
    this.initializeFirebase();
  }

  private initializeFirebase(): void {
    if (admin.apps.length > 0) {
      this.initialized = true;
      return;
    }

    if (!env.FCM_PROJECT_ID || !env.FCM_CLIENT_EMAIL || !env.FCM_PRIVATE_KEY) {
      logger.warn('Firebase credentials not fully configured. FCM provider will fail if invoked.');
      return;
    }

    try {
      // Correct escaped newlines in PEM format if stored in environment variable
      const formattedPrivateKey = env.FCM_PRIVATE_KEY.replace(/\\n/g, '\n');

      admin.initializeApp({
        credential: admin.credential.cert({
          projectId: env.FCM_PROJECT_ID,
          clientEmail: env.FCM_CLIENT_EMAIL,
          privateKey: formattedPrivateKey,
        }),
      });

      this.initialized = true;
      logger.info({ projectId: env.FCM_PROJECT_ID }, 'Firebase Admin SDK initialized successfully');
    } catch (err: unknown) {
      const errorMsg = err instanceof Error ? err.message : String(err);
      logger.error({ error: errorMsg }, 'Failed to initialize Firebase Admin SDK');
    }
  }

  async send(options: SendPushOptions): Promise<ProviderResponse> {
    const { recipient, push, notificationId } = options;

    if (!this.initialized || admin.apps.length === 0) {
      throw new Error('Firebase Admin SDK is not initialized. Check FCM credentials.');
    }

    if (!recipient.pushToken) {
      throw new Error('Recipient pushToken is required for FCM push provider');
    }

    logger.debug(
      { notificationId, tokenPrefix: recipient.pushToken.substring(0, 10) + '...', provider: this.name },
      'Sending push notification via Firebase FCM'
    );

    const message: admin.messaging.Message = {
      token: recipient.pushToken,
      notification: {
        title: push.title,
        body: push.body,
        ...(push.imageUrl ? { imageUrl: push.imageUrl } : {}),
      },
      data: {
        ...(push.data || {}),
        notificationId,
      },
      android: {
        priority: 'high',
        notification: {
          sound: 'default',
        },
      },
      apns: {
        payload: {
          aps: {
            sound: 'default',
            badge: 1,
          },
        },
      },
    };

    try {
      const messageId = await admin.messaging().send(message);

      logger.info(
        { notificationId, messageId, provider: this.name },
        'Push notification successfully sent via FCM'
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
        'Firebase FCM push notification failed'
      );
      throw new Error(`FCM Error: ${errorMessage}`);
    }
  }
}
