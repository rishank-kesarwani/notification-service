import { Router, Request, Response } from 'express';
import { notificationController } from '../controllers/notification.controller';
import { authenticate } from '../middlewares/auth.middleware';
import { validateBody } from '../middlewares/validate.middleware';
import { CreateNotificationSchema } from '../types/zod-schemas';
import { redisClient } from '../config/redis';

const router = Router();

// Health Check Endpoint (Public)
router.get('/health', async (_req: Request, res: Response) => {
  let redisStatus = 'UNKNOWN';
  try {
    const ping = await redisClient.ping();
    redisStatus = ping === 'PONG' ? 'HEALTHY' : 'UNHEALTHY';
  } catch {
    redisStatus = 'DISCONNECTED';
  }

  const isHealthy = redisStatus === 'HEALTHY';

  res.status(isHealthy ? 200 : 503).json({
    status: isHealthy ? 'UP' : 'DEGRADED',
    timestamp: new Date().toISOString(),
    services: {
      api: 'UP',
      redis: redisStatus,
    },
  });
});

// Queue Metrics Endpoint (Protected)
router.get('/metrics', authenticate, (req, res, next) => {
  notificationController.getMetrics(req, res, next);
});

// Ingestion Endpoint: POST /v1/notifications (Protected & Validated)
router.post(
  '/v1/notifications',
  authenticate,
  validateBody(CreateNotificationSchema),
  (req, res, next) => {
    notificationController.ingestNotification(req, res, next);
  }
);

// User Preferences Endpoints (Protected)
router.get('/v1/preferences/:userId', authenticate, (req, res, next) => {
  notificationController.getUserPreferences(req, res, next);
});

router.put('/v1/preferences/:userId', authenticate, (req, res, next) => {
  notificationController.updateUserPreferences(req, res, next);
});

export default router;
