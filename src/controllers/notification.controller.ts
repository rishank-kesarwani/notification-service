import { Request, Response, NextFunction } from 'express';
import crypto from 'crypto';
import { logger } from '../config/logger';
import { queueRegistry } from '../queues/queue.registry';
import { idempotencyService } from '../services/idempotency.service';
import { userPreferenceService } from '../services/preference.service';
import {
  EmailJobData,
  IngestionResponse,
  NotificationChannel,
  PushJobData,
  QueueName,
} from '../types/notification';
import { CreateNotificationInput, UserPreferenceInput } from '../types/zod-schemas';

export class NotificationController {
  /**
   * Main Ingestion Endpoint: POST /v1/notifications
   * Dispatches notifications to dedicated BullMQ channel queues with idempotency & preference filtering
   */
  async ingestNotification(req: Request, res: Response, next: NextFunction): Promise<void> {
    try {
      const payload = req.body as CreateNotificationInput;
      const { priority, channels, recipient, email, push, metadata, idempotencyKey } = payload;
      const notificationId = `notif_${Date.now()}_${crypto.randomBytes(4).toString('hex')}`;

      // 1. Check Distributed Idempotency via Redis
      if (idempotencyKey) {
        const idempotencyResult = await idempotencyService.checkAndAcquire(
          idempotencyKey,
          notificationId
        );

        if (idempotencyResult.isDuplicate) {
          logger.info(
            { idempotencyKey, notificationId: idempotencyResult.notificationId },
            'Idempotency hit: Returning cached / accepted response'
          );

          if (idempotencyResult.existingRecord?.responsePayload) {
            res.status(202).json(idempotencyResult.existingRecord.responsePayload);
            return;
          }

          res.status(202).json({
            success: true,
            message: 'Notification request already received and is being processed (Idempotency)',
            notificationId: idempotencyResult.notificationId,
            idempotencyStatus: 'CACHED',
            enqueuedChannels: [],
          });
          return;
        }
      }

      const enqueuedChannels: {
        channel: NotificationChannel;
        queue: QueueName;
        jobId: string;
      }[] = [];

      const skippedChannels: {
        channel: NotificationChannel;
        reason: string;
      }[] = [];

      const createdAt = new Date().toISOString();

      // 2. Process each requested channel independently (Zero Coupling / No Single Bottleneck)
      for (const channel of channels) {
        // Evaluate User Preference and Opt-Outs
        const preferenceCheck = await userPreferenceService.isChannelAllowed(
          recipient.userId,
          channel,
          priority
        );

        if (!preferenceCheck.allowed) {
          logger.info(
            { notificationId, userId: recipient.userId, channel, reason: preferenceCheck.reason },
            'Channel skipped due to user preference/opt-out'
          );
          skippedChannels.push({
            channel,
            reason: preferenceCheck.reason || 'User opted out of this channel',
          });
          continue;
        }

        // Enqueue to Channel-Specific BullMQ Queue
        if (channel === 'EMAIL' && email) {
          const emailJobData: EmailJobData = {
            notificationId,
            idempotencyKey,
            priority,
            channel: 'EMAIL',
            recipient,
            email,
            metadata,
            createdAt,
          };

          const enqueued = await queueRegistry.enqueueEmail(emailJobData);
          enqueuedChannels.push({
            channel: 'EMAIL',
            queue: enqueued.queueName,
            jobId: enqueued.jobId,
          });
        }

        if (channel === 'PUSH' && push) {
          const pushJobData: PushJobData = {
            notificationId,
            idempotencyKey,
            priority,
            channel: 'PUSH',
            recipient,
            push,
            metadata,
            createdAt,
          };

          const enqueued = await queueRegistry.enqueuePush(pushJobData);
          enqueuedChannels.push({
            channel: 'PUSH',
            queue: enqueued.queueName,
            jobId: enqueued.jobId,
          });
        }
      }

      const responsePayload: IngestionResponse = {
        success: true,
        message: enqueuedChannels.length > 0 ? 'Notification enqueued successfully' : 'No notifications enqueued',
        notificationId,
        idempotencyStatus: 'PROCESSED',
        enqueuedChannels,
        ...(skippedChannels.length > 0 ? { skippedChannels } : {}),
      };

      // 3. Mark Idempotency as Processed in Redis
      if (idempotencyKey) {
        await idempotencyService.markProcessed(
          idempotencyKey,
          notificationId,
          responsePayload
        );
      }

      logger.info(
        { notificationId, enqueuedCount: enqueuedChannels.length, skippedCount: skippedChannels.length },
        'Notification ingestion completed successfully'
      );

      res.status(200).json(responsePayload);
    } catch (err) {
      next(err);
    }
  }

  /**
   * Get queue health & metrics: GET /metrics
   */
  async getMetrics(_req: Request, res: Response, next: NextFunction): Promise<void> {
    try {
      const metrics = await queueRegistry.getMetrics();
      const { strategyRegistry } = await import('../patterns/strategy/strategy.registry');
      const circuitBreakers = strategyRegistry.getAllCircuitBreakerDiagnostics();

      res.status(200).json({
        success: true,
        timestamp: new Date().toISOString(),
        queues: metrics,
        circuitBreakers,
      });
    } catch (err) {
      next(err);
    }
  }

  /**
   * Get User Preferences: GET /v1/preferences/:userId
   */
  async getUserPreferences(req: Request, res: Response, next: NextFunction): Promise<void> {
    try {
      const { userId } = req.params;
      const preferences = await userPreferenceService.getUserPreferences(userId);
      res.status(200).json({
        success: true,
        preferences,
      });
    } catch (err) {
      next(err);
    }
  }

  /**
   * Update User Preferences: PUT /v1/preferences/:userId
   */
  async updateUserPreferences(req: Request, res: Response, next: NextFunction): Promise<void> {
    try {
      const { userId } = req.params;
      const payload = req.body as Partial<UserPreferenceInput>;

      const current = await userPreferenceService.getUserPreferences(userId);
      const updated = {
        ...current,
        ...payload,
        userId,
      };

      await userPreferenceService.setUserPreferences(updated);

      res.status(200).json({
        success: true,
        message: 'User preferences updated successfully',
        preferences: updated,
      });
    } catch (err) {
      next(err);
    }
  }
}

export const notificationController = new NotificationController();
