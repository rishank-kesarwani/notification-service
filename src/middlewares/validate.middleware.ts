import { Request, Response, NextFunction } from 'express';
import { ZodSchema, ZodError } from 'zod';
import { logger } from '../config/logger';

export const validateBody = (schema: ZodSchema) => {
  return async (req: Request, res: Response, next: NextFunction): Promise<void> => {
    try {
      req.body = await schema.parseAsync(req.body);
      next();
    } catch (error: unknown) {
      if (error instanceof ZodError) {
        const formattedErrors = error.errors.map((err) => ({
          field: err.path.join('.'),
          message: err.message,
        }));

        logger.warn({ path: req.path, errors: formattedErrors }, 'Validation error on incoming request');

        res.status(400).json({
          success: false,
          error: 'Validation Error',
          details: formattedErrors,
        });
        return;
      }

      next(error);
    }
  };
};
