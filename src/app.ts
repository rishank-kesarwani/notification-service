import express, { Application, Request, Response, NextFunction } from 'express';
import cors from 'cors';
import helmet from 'helmet';
import routes from './routes/notification.routes';
import { errorHandler } from './middlewares/error.middleware';
import { logger } from './config/logger';

export const createApp = (): Application => {
  const app = express();

  // Security & standard middleware
  app.use(helmet());
  app.use(cors());
  app.use(express.json({ limit: '1mb' }));
  app.use(express.urlencoded({ extended: true }));

  // HTTP Request Logger
  app.use((req: Request, res: Response, next: NextFunction) => {
    const startTime = Date.now();
    res.on('finish', () => {
      const durationMs = Date.now() - startTime;
      logger.info(
        {
          method: req.method,
          url: req.originalUrl,
          statusCode: res.statusCode,
          durationMs,
          ip: req.ip,
        },
        'HTTP Request Processed'
      );
    });
    next();
  });

  // Mount API routes
  app.use('/', routes);

  // 404 Route Handler
  app.use((req: Request, res: Response) => {
    res.status(404).json({
      success: false,
      error: 'Not Found',
      message: `Cannot ${req.method} ${req.path}`,
    });
  });

  // Centralized Error Handler
  app.use(errorHandler);

  return app;
};
