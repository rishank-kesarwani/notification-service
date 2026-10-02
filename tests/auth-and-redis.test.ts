import { describe, it, after } from 'node:test';
import assert from 'node:assert/strict';
import request from 'supertest';
import express, { Request, Response } from 'express';
import { createApp } from '../src/app';
import { authenticate, getServiceApiKeyRegistry } from '../src/middlewares/auth.middleware';
import { env } from '../src/config/env';
import { createRedisConnection } from '../src/config/redis';

describe('Notification Service Authentication & Redis Tests', () => {
  after(async () => {
    try {
      const { redisClient } = await import('../src/config/redis');
      const { queueRegistry } = await import('../src/queues/queue.registry');
      await queueRegistry.close();
      redisClient.disconnect();
    } catch {
      // ignore cleanup error
    }
  });
  const travelKey = 'test-travel-key-111';
  const movieKey = 'test-movie-key-222';
  const sportsKey = 'test-sports-key-333';
  const studyKey = 'test-study-key-444';
  const legacyKey = 'test-legacy-fallback-key-999';

  // Setup test environment keys
  env.NOTIFICATION_TRAVEL_API_KEY = travelKey;
  env.NOTIFICATION_MOVIE_API_KEY = movieKey;
  env.NOTIFICATION_SPORTS_API_KEY = sportsKey;
  env.NOTIFICATION_STUDY_API_KEY = studyKey;
  env.API_KEY = legacyKey;

  const app = createApp();

  // Test App with an inspection endpoint to verify attached req.user
  const testApp = express();
  testApp.get('/test-auth', authenticate, (req: Request, res: Response) => {
    res.status(200).json({
      success: true,
      user: req.user,
    });
  });

  it('1. Travel API key is accepted and identifies travel service', async () => {
    const res = await request(testApp)
      .get('/test-auth')
      .set('x-api-key', travelKey);

    assert.equal(res.status, 200);
    assert.equal(res.body.success, true);
    assert.equal(res.body.user.serviceName, 'travel');
    assert.equal(res.body.user.role, 'service');
    assert.equal(res.body.user.apiKeyUsed, true);
  });

  it('2. Movie API key is accepted and identifies movie service', async () => {
    const res = await request(testApp)
      .get('/test-auth')
      .set('x-api-key', movieKey);

    assert.equal(res.status, 200);
    assert.equal(res.body.success, true);
    assert.equal(res.body.user.serviceName, 'movie');
    assert.equal(res.body.user.role, 'service');
    assert.equal(res.body.user.apiKeyUsed, true);
  });

  it('3. Sports API key is accepted and identifies sports service', async () => {
    const res = await request(testApp)
      .get('/test-auth')
      .set('x-api-key', sportsKey);

    assert.equal(res.status, 200);
    assert.equal(res.body.success, true);
    assert.equal(res.body.user.serviceName, 'sports');
    assert.equal(res.body.user.role, 'service');
    assert.equal(res.body.user.apiKeyUsed, true);
  });

  it('4. Study API key is accepted and identifies study service', async () => {
    const res = await request(testApp)
      .get('/test-auth')
      .set('x-api-key', studyKey);

    assert.equal(res.status, 200);
    assert.equal(res.body.success, true);
    assert.equal(res.body.user.serviceName, 'study');
    assert.equal(res.body.user.role, 'service');
    assert.equal(res.body.user.apiKeyUsed, true);
  });

  it('4b. Dynamically discovered API key (e.g. NOTIFICATION_RESUME_API_KEY) is automatically accepted and identifies resume service', async () => {
    const resumeKey = 'test-dynamic-resume-key-888';
    process.env.NOTIFICATION_RESUME_API_KEY = resumeKey;

    try {
      const res = await request(testApp)
        .get('/test-auth')
        .set('x-api-key', resumeKey);

      assert.equal(res.status, 200);
      assert.equal(res.body.success, true);
      assert.equal(res.body.user.serviceName, 'resume');
      assert.equal(res.body.user.role, 'service');
      assert.equal(res.body.user.apiKeyUsed, true);
    } finally {
      delete process.env.NOTIFICATION_RESUME_API_KEY;
    }
  });

  it('5. Invalid key returns 401 Unauthorized without exposing secret details', async () => {
    const res = await request(testApp)
      .get('/test-auth')
      .set('x-api-key', 'invalid-secret-key-12345');

    assert.equal(res.status, 401);
    assert.equal(res.body.success, false);
    assert.equal(res.body.error, 'Unauthorized');
    assert.equal(res.body.message, 'Invalid API Key provided');
    // Ensure the key itself is not echoed back in response
    assert.equal(JSON.stringify(res.body).includes('invalid-secret-key-12345'), false);
  });

  it('6. Missing key returns 401 Unauthorized with required credentials message', async () => {
    const res = await request(testApp)
      .get('/test-auth');

    assert.equal(res.status, 401);
    assert.equal(res.body.success, false);
    assert.equal(res.body.error, 'Unauthorized');
    assert.match(res.body.message, /Authentication credentials required/i);
  });

  it('7. /health endpoint remains public without requiring credentials', async () => {
    const res = await request(app).get('/health');
    // Health check returns 200 (or 503 if redis is unreachable), never 401
    assert.notEqual(res.status, 401);
    assert.ok(res.body.status === 'UP' || res.body.status === 'DEGRADED');
  });

  it('8. POST /v1/notifications requires authentication (returns 401 when unauthenticated)', async () => {
    const res = await request(app)
      .post('/v1/notifications')
      .send({
        priority: 'CRITICAL',
        channels: ['EMAIL'],
        recipient: { userId: 'u123', email: 'test@example.com' },
        email: { subject: 'Test', text: 'Hello' },
      });

    assert.equal(res.status, 401);
    assert.equal(res.body.success, false);
  });

  it('9. GET /metrics requires authentication (returns 401 when unauthenticated)', async () => {
    const res = await request(app).get('/metrics');
    assert.equal(res.status, 401);
    assert.equal(res.body.success, false);
  });

  it('10. Authorization: ApiKey <key> compatibility format works correctly', async () => {
    const res = await request(testApp)
      .get('/test-auth')
      .set('Authorization', `ApiKey ${travelKey}`);

    assert.equal(res.status, 200);
    assert.equal(res.body.success, true);
    assert.equal(res.body.user.serviceName, 'travel');
  });

  it('11. Existing legacy API_KEY fallback works during migration', async () => {
    const res = await request(testApp)
      .get('/test-auth')
      .set('x-api-key', legacyKey);

    assert.equal(res.status, 200);
    assert.equal(res.body.success, true);
    assert.equal(res.body.user.serviceName, 'legacy');
    assert.equal(res.body.user.apiKeyUsed, true);
  });

  it('12. Calling service identity is correctly attached to request context', async () => {
    const registry = getServiceApiKeyRegistry();
    assert.equal(registry.get(travelKey), 'travel');
    assert.equal(registry.get(movieKey), 'movie');
    assert.equal(registry.get(sportsKey), 'sports');
    assert.equal(registry.get(studyKey), 'study');
    assert.equal(registry.get(legacyKey), 'legacy');
  });

  it('13. API keys never appear in response payloads or error responses', async () => {
    const invalidSecret = 'ultra-secret-test-token-to-verify-no-leak';
    const res = await request(testApp)
      .get('/test-auth')
      .set('x-api-key', invalidSecret);

    assert.equal(res.status, 401);
    const bodyString = JSON.stringify(res.body);
    assert.equal(bodyString.includes(invalidSecret), false);
    assert.equal(bodyString.includes(travelKey), false);
  });

  it('14. Redis connection prefers REDIS_URL when configured', () => {
    const originalUrl = env.REDIS_URL;
    try {
      env.REDIS_URL = 'redis://:mypassword@custom-redis-host:6380/0';
      const client = createRedisConnection();
      // ioredis client options store the connection configuration
      assert.equal(client.options.port, 6380);
      assert.equal(client.options.host, 'custom-redis-host');
      assert.equal(client.options.password, 'mypassword');
      client.disconnect();
    } finally {
      env.REDIS_URL = originalUrl;
    }
  });

  it('15. Redis connection falls back to host/port/password when REDIS_URL is undefined', () => {
    const originalUrl = env.REDIS_URL;
    const originalHost = env.REDIS_HOST;
    const originalPort = env.REDIS_PORT;
    const originalPassword = env.REDIS_PASSWORD;

    try {
      env.REDIS_URL = undefined;
      env.REDIS_HOST = '127.0.0.1';
      env.REDIS_PORT = 6399;
      env.REDIS_PASSWORD = 'fallback-pass';

      const client = createRedisConnection();
      assert.equal(client.options.host, '127.0.0.1');
      assert.equal(client.options.port, 6399);
      assert.equal(client.options.password, 'fallback-pass');
      client.disconnect();
    } finally {
      env.REDIS_URL = originalUrl;
      env.REDIS_HOST = originalHost;
      env.REDIS_PORT = originalPort;
      env.REDIS_PASSWORD = originalPassword;
    }
  });
});
