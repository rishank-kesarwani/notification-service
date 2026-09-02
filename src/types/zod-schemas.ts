import { z } from 'zod';

export const ChannelEnum = z.enum(['EMAIL', 'PUSH']);
export const PriorityEnum = z.enum(['CRITICAL', 'BULK']);

export const RecipientSchema = z.object({
  userId: z.string().min(1, 'userId is required'),
  email: z.string().email('Invalid email address format').optional(),
  pushToken: z.string().min(1, 'pushToken cannot be empty').optional(),
});

export const EmailAttachmentSchema = z
  .object({
    filename: z.string().min(1, 'filename is required'),
    path: z.string().url('Invalid attachment URL format').optional(),
    content: z.string().optional(),
    contentType: z.string().optional(),
  })
  .refine((data) => data.path || data.content, {
    message: 'Either path (URL/Signed URL) or content (Base64) must be provided for the attachment',
    path: ['path'],
  });

export const EmailPayloadSchema = z
  .object({
    subject: z.string().min(1, 'Email subject is required'),
    html: z.string().optional(),
    text: z.string().optional(),
    from: z.string().optional(),
    replyTo: z.string().email('Invalid reply-to email').optional(),
    tags: z.record(z.string()).optional(),
    attachments: z.array(EmailAttachmentSchema).optional(),
  })
  .refine((data) => data.html || data.text, {
    message: 'Either html or text content must be provided in email payload',
    path: ['html'],
  });

export const PushPayloadSchema = z.object({
  title: z.string().min(1, 'Push notification title is required'),
  body: z.string().min(1, 'Push notification body is required'),
  data: z.record(z.string()).optional(),
  imageUrl: z.string().url('Invalid image URL format').optional(),
});

export const CreateNotificationSchema = z
  .object({
    idempotencyKey: z
      .string()
      .min(1)
      .max(255)
      .optional(),
    priority: PriorityEnum.default('BULK'),
    channels: z
      .array(ChannelEnum)
      .min(1, 'At least one channel (EMAIL or PUSH) must be specified')
      .transform((val) => Array.from(new Set(val))),
    recipient: RecipientSchema,
    email: EmailPayloadSchema.optional(),
    push: PushPayloadSchema.optional(),
    metadata: z.record(z.unknown()).optional(),
  })
  .superRefine((data, ctx) => {
    if (data.channels.includes('EMAIL')) {
      if (!data.recipient.email) {
        ctx.addIssue({
          code: z.ZodIssueCode.custom,
          message: 'Recipient email is required when EMAIL channel is requested',
          path: ['recipient', 'email'],
        });
      }
      if (!data.email) {
        ctx.addIssue({
          code: z.ZodIssueCode.custom,
          message: 'Email payload is required when EMAIL channel is requested',
          path: ['email'],
        });
      }
    }

    if (data.channels.includes('PUSH')) {
      if (!data.recipient.pushToken) {
        ctx.addIssue({
          code: z.ZodIssueCode.custom,
          message: 'Recipient pushToken is required when PUSH channel is requested',
          path: ['recipient', 'pushToken'],
        });
      }
      if (!data.push) {
        ctx.addIssue({
          code: z.ZodIssueCode.custom,
          message: 'Push payload is required when PUSH channel is requested',
          path: ['push'],
        });
      }
    }
  });

export const UserPreferenceSchema = z.object({
  userId: z.string().min(1),
  emailOptOut: z.boolean().default(false),
  pushOptOut: z.boolean().default(false),
  bulkOptOut: z.boolean().default(false),
});

export type CreateNotificationInput = z.infer<typeof CreateNotificationSchema>;
export type UserPreferenceInput = z.infer<typeof UserPreferenceSchema>;
