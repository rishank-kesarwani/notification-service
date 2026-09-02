import dotenv from 'dotenv';
import { z } from 'zod';

dotenv.config();

const envSchema = z.object({
  PORT: z.coerce.number().default(3000),
  NODE_ENV: z.enum(['development', 'production', 'test']).default('development'),
  API_KEY: z.string().default('test-api-key-12345'),
  JWT_SECRET: z.string().default('default-super-secret-jwt-key-for-development'),

  // Redis Configuration
  REDIS_HOST: z.string().default('localhost'),
  REDIS_PORT: z.coerce.number().default(6379),
  REDIS_PASSWORD: z.string().optional(),

  // Email Configuration (Resend)
  RESEND_API_KEY: z.string().optional().default(''),
  EMAIL_FROM: z.string().default('onboarding@resend.dev'),

  // Fallback SMTP Configuration (Nodemailer / Gmail)
  SMTP_HOST: z.string().default('smtp.gmail.com'),
  SMTP_PORT: z.coerce.number().default(587),
  SMTP_USER: z.string().optional().default(''),
  SMTP_PASS: z.string().optional().default(''),
  SMTP_FROM: z.string().optional().default(''),

  // Push Notifications (Firebase FCM)
  FCM_PROJECT_ID: z.string().optional().default(''),
  FCM_CLIENT_EMAIL: z.string().optional().default(''),
  FCM_PRIVATE_KEY: z.string().optional().default(''),

  // Rate Limiting Defaults
  EMAIL_RATE_LIMIT_MAX: z.coerce.number().default(10), // Max requests per window
  EMAIL_RATE_LIMIT_WINDOW_MS: z.coerce.number().default(1000), // Window in ms (1s)
  PUSH_RATE_LIMIT_MAX: z.coerce.number().default(50),
  PUSH_RATE_LIMIT_WINDOW_MS: z.coerce.number().default(1000),

  // Idempotency TTL
  IDEMPOTENCY_TTL_SECONDS: z.coerce.number().default(300), // 5 minutes
});

const _env = envSchema.safeParse(process.env);

if (!_env.success) {
  console.error('❌ Invalid environment variables:', JSON.stringify(_env.error.format(), null, 2));
  process.exit(1);
}

export const env = _env.data;
