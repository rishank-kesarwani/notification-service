import dotenv from 'dotenv';
import { z } from 'zod';

dotenv.config();

const envSchema = z.object({
  PORT: z.coerce.number().default(3000),
  NODE_ENV: z.enum(['development', 'production', 'test']).default('development'),
  // Application-Specific Service Authentication Keys
  NOTIFICATION_TRAVEL_API_KEY: z.string().optional(),
  NOTIFICATION_MOVIE_API_KEY: z.string().optional(),
  NOTIFICATION_SPORTS_API_KEY: z.string().optional(),
  NOTIFICATION_STUDY_API_KEY: z.string().optional(),

  // Legacy Fallback API Key (for backward compatibility during migration)
  API_KEY: z.string().optional().default('test-api-key-12345'),
  JWT_SECRET: z.string().default('default-super-secret-jwt-key-for-development'),

  // Redis Configuration (REDIS_URL is canonical; REDIS_HOST/PORT/PASSWORD as fallback)
  REDIS_URL: z.string().optional(),
  REDIS_HOST: z.string().default('localhost'),
  REDIS_PORT: z.coerce.number().default(6379),
  REDIS_PASSWORD: z.string().optional(),

  // Primary Email Configuration (Resend)
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

  // SMS Configuration (Primary: httpSMS, Fallback: TextBee)
  HTTPSMS_API_KEY: z.string().optional().default(''),
  HTTPSMS_FROM_NUMBER: z.string().optional().default(''),
  TEXTBEE_API_KEY: z.string().optional().default(''),
  TEXTBEE_DEVICE_ID: z.string().optional().default(''),

  // Rate Limiting Defaults
  EMAIL_RATE_LIMIT_MAX: z.coerce.number().default(10), // Max requests per window
  EMAIL_RATE_LIMIT_WINDOW_MS: z.coerce.number().default(1000), // Window in ms (1s)
  PUSH_RATE_LIMIT_MAX: z.coerce.number().default(50),
  PUSH_RATE_LIMIT_WINDOW_MS: z.coerce.number().default(1000),
  SMS_RATE_LIMIT_MAX: z.coerce.number().default(20),
  SMS_RATE_LIMIT_WINDOW_MS: z.coerce.number().default(1000),

  // Idempotency TTL
  IDEMPOTENCY_TTL_SECONDS: z.coerce.number().default(300), // 5 minutes
}).superRefine((data, ctx) => {
  if (data.NODE_ENV === 'production') {
    const hasDynamicAppKey = Object.keys(process.env).some(
      (k) => /^NOTIFICATION_[A-Z0-9_]+_API_KEY$/i.test(k) && Boolean(process.env[k]?.trim())
    );
    const hasConfiguredLegacyKey = Boolean(data.API_KEY && data.API_KEY !== 'test-api-key-12345');

    if (!hasDynamicAppKey && !hasConfiguredLegacyKey) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message:
          'Production requires at least one configured application API key (e.g. NOTIFICATION_TRAVEL_API_KEY, NOTIFICATION_RESUME_API_KEY, etc.) or non-default API_KEY',
        path: ['NOTIFICATION_TRAVEL_API_KEY'],
      });
    }
  }
});

const _env = envSchema.safeParse(process.env);

if (!_env.success) {
  console.error('❌ Invalid environment variables:', JSON.stringify(_env.error.format(), null, 2));
  process.exit(1);
}

export const env = _env.data;
