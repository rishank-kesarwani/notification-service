import { Request, Response, NextFunction } from 'express';
import jwt from 'jsonwebtoken';
import { env } from '../config/env';
import { logger } from '../config/logger';

export type KnownServiceName = 'travel' | 'movie' | 'sports' | 'study' | 'legacy';
export type ServiceName = KnownServiceName | string;

export interface AuthenticatedUser {
  id?: string;
  role?: string;
  apiKeyUsed?: boolean;
  serviceName?: ServiceName;
}

declare global {
  namespace Express {
    interface Request {
      user?: AuthenticatedUser;
    }
  }
}

/**
 * Builds the centralized in-memory service API key lookup registry with dynamic discovery.
 * Priority:
 * 1. Dynamic discovery of any NOTIFICATION_<NAME>_API_KEY environment variables
 * 2. Explicit application-specific variables (NOTIFICATION_*_API_KEY)
 * 3. Legacy fallback key (API_KEY)
 */
export const getServiceApiKeyRegistry = (): Map<string, ServiceName> => {
  const registry = new Map<string, ServiceName>();

  // 1. Dynamic Discovery: Automatically discovers any NOTIFICATION_<SERVICE>_API_KEY
  for (const [key, value] of Object.entries(process.env)) {
    const match = key.match(/^NOTIFICATION_([A-Z0-9_]+)_API_KEY$/i);
    if (match && value && value.trim().length > 0) {
      const serviceName = match[1].toLowerCase();
      registry.set(value.trim(), serviceName);
    }
  }

  // 2. Explicitly mapped variables from parsed env config (ensures in-memory test overrides work)
  if (env.NOTIFICATION_TRAVEL_API_KEY) {
    registry.set(env.NOTIFICATION_TRAVEL_API_KEY, 'travel');
  }
  if (env.NOTIFICATION_MOVIE_API_KEY) {
    registry.set(env.NOTIFICATION_MOVIE_API_KEY, 'movie');
  }
  if (env.NOTIFICATION_SPORTS_API_KEY) {
    registry.set(env.NOTIFICATION_SPORTS_API_KEY, 'sports');
  }
  if (env.NOTIFICATION_STUDY_API_KEY) {
    registry.set(env.NOTIFICATION_STUDY_API_KEY, 'study');
  }

  // 3. Backward compatibility fallback: Legacy API_KEY during migration
  if (env.API_KEY && !registry.has(env.API_KEY)) {
    registry.set(env.API_KEY, 'legacy');
  }

  return registry;
};

export const authenticate = (req: Request, res: Response, next: NextFunction): void => {
  const apiKeyHeader = req.headers['x-api-key'] as string | undefined;
  const authHeader = req.headers.authorization;
  const serviceKeyRegistry = getServiceApiKeyRegistry();

  // 1. Check x-api-key header (Primary service-to-service mechanism)
  if (apiKeyHeader) {
    const serviceName = serviceKeyRegistry.get(apiKeyHeader);
    if (serviceName) {
      req.user = { apiKeyUsed: true, role: 'service', serviceName };
      return next();
    }
    // Security: Do NOT log key value or leak metadata
    logger.warn({ ip: req.ip }, 'Invalid API Key provided');
    res.status(401).json({
      success: false,
      error: 'Unauthorized',
      message: 'Invalid API Key provided',
    });
    return;
  }

  // 2. Check Authorization Header (ApiKey <key> or Bearer <JWT>)
  if (authHeader) {
    if (authHeader.startsWith('ApiKey ')) {
      const key = authHeader.substring(7).trim();
      const serviceName = serviceKeyRegistry.get(key);
      if (serviceName) {
        req.user = { apiKeyUsed: true, role: 'service', serviceName };
        return next();
      }
      logger.warn({ ip: req.ip }, 'Invalid API Key provided via Authorization header');
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

  // 3. No authentication credentials provided
  logger.warn({ ip: req.ip, path: req.path }, 'Missing authentication credentials');
  res.status(401).json({
    success: false,
    error: 'Unauthorized',
    message: 'Authentication credentials required (x-api-key header or Bearer JWT)',
  });
};

