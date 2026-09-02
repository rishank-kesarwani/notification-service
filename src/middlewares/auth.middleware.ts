import { Request, Response, NextFunction } from 'express';
import jwt from 'jsonwebtoken';
import { env } from '../config/env';
import { logger } from '../config/logger';

export interface AuthenticatedUser {
  id?: string;
  role?: string;
  apiKeyUsed?: boolean;
}

declare global {
  namespace Express {
    interface Request {
      user?: AuthenticatedUser;
    }
  }
}

export const authenticate = (req: Request, res: Response, next: NextFunction): void => {
  const apiKeyHeader = req.headers['x-api-key'] as string | undefined;
  const authHeader = req.headers.authorization;

  // 1. Check API Key in header
  if (apiKeyHeader) {
    if (apiKeyHeader === env.API_KEY) {
      req.user = { apiKeyUsed: true, role: 'service' };
      return next();
    }
    logger.warn({ ip: req.ip }, 'Invalid API Key provided');
    res.status(401).json({
      success: false,
      error: 'Unauthorized',
      message: 'Invalid API Key provided',
    });
    return;
  }

  // 2. Check Authorization Header (ApiKey or Bearer JWT)
  if (authHeader) {
    if (authHeader.startsWith('ApiKey ')) {
      const key = authHeader.substring(7).trim();
      if (key === env.API_KEY) {
        req.user = { apiKeyUsed: true, role: 'service' };
        return next();
      }
      res.status(401).json({
        success: false,
        error: 'Unauthorized',
        message: 'Invalid API Key provided',
      });
      return;
    }

    if (authHeader.startsWith('Bearer ')) {
      const token = authHeader.substring(7).trim();
      try {
        const decoded = jwt.verify(token, env.JWT_SECRET) as AuthenticatedUser;
        req.user = decoded;
        return next();
      } catch (err: unknown) {
        const errorMsg = err instanceof Error ? err.message : 'Invalid token';
        logger.warn({ ip: req.ip, error: errorMsg }, 'JWT verification failed');
        res.status(401).json({
          success: false,
          error: 'Unauthorized',
          message: 'Invalid or expired JWT token',
        });
        return;
      }
    }
  }

  // No authentication credentials provided
  logger.warn({ ip: req.ip, path: req.path }, 'Missing authentication credentials');
  res.status(401).json({
    success: false,
    error: 'Unauthorized',
    message: 'Authentication credentials required (x-api-key header or Bearer JWT)',
  });
};
