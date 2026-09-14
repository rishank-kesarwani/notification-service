# Production-Grade Dual-Channel Notification Engine (Email & Push)

A high-performance, non-blocking, and resilient multi-channel notification engine built with **Node.js, TypeScript, Express, Redis, BullMQ, and Zod**.

---

## Table of Contents
- [System Architecture (High-Level Design)](#system-architecture-high-level-design)
  - [1. High-Level Design (HLD) Architecture Diagram](#1-high-level-design-hld-architecture-diagram)
  - [2. End-to-End Sequence & Request Lifecycle Diagram](#2-end-to-end-sequence--request-lifecycle-diagram)
- [Deep-Dive Component Explanations](#deep-dive-component-explanations)
  - [1. API Ingestion Gateway & Security Middleware](#1-api-ingestion-gateway--security-middleware)
  - [2. Distributed Idempotency Engine](#2-distributed-idempotency-engine)
  - [3. User Preference & Channel Opt-Out Engine](#3-user-preference--channel-opt-out-engine)
  - [4. Notification Router & Queue Dispatcher](#4-notification-router--queue-dispatcher)
  - [5. Multi-Queue & Channel Segregation Topology](#5-multi-queue--channel-segregation-topology)
  - [6. Autonomous Worker Fleet & Cluster Runner](#6-autonomous-worker-fleet--cluster-runner)
  - [7. Channel Strategy Layer](#7-channel-strategy-layer)
  - [8. Distributed Sliding-Window Rate Limiter](#8-distributed-sliding-window-rate-limiter)
  - [9. Multi-Tier Resilient Circuit Breakers](#9-multi-tier-resilient-circuit-breakers)
  - [10. Downstream Provider Adapters & Multi-Provider Failover](#10-downstream-provider-adapters--multi-provider-failover)
  - [11. Dead Letter Queue (DLQ) & Poison Pill Isolation](#11-dead-letter-queue-dlq--poison-pill-isolation)
  - [12. Real-Time Observability, Diagnostics & Health Monitoring](#12-real-time-observability-diagnostics--health-monitoring)
- [Design Patterns Implemented](#design-patterns-implemented)
  - [1. Strategy Design Pattern](#1-strategy-design-pattern)
  - [2. Circuit Breaker Design Pattern](#2-circuit-breaker-design-pattern)
  - [3. Producer-Consumer & Channel Segregation Pattern](#3-producer-consumer--channel-segregation-pattern)
  - [4. Distributed Idempotent Receiver Pattern](#4-distributed-idempotent-receiver-pattern)
- [Zero-Bottleneck Worker Segregation](#zero-bottleneck-worker-segregation)
- [Tech Stack](#tech-stack)
- [Project Directory Structure](#project-directory-structure)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [1. Start Redis](#1-start-redis)
  - [2. Configure Environment Variables](#2-configure-environment-variables)
  - [3. Install Dependencies & Build](#3-install-dependencies--build)
  - [4. Running the Service](#4-running-the-service)
- [API Reference & cURL Examples](#api-reference--curl-examples)
  - [1. Ingest Dual-Channel Notification (Email + Push)](#1-ingest-dual-channel-notification-email--push)
  - [2. Distributed Idempotency Verification](#2-distributed-idempotency-verification)
  - [3. User Preference & Opt-Out Filtering](#3-user-preference--opt-out-filtering)
  - [4. Queue & Circuit Breaker Diagnostics Endpoint](#4-queue--circuit-breaker-diagnostics-endpoint)
  - [5. Health Check Endpoint](#5-health-check-endpoint)
- [Resiliency & Error Handling Matrix](#resiliency--error-handling-matrix)
- [License](#license)

---

## System Architecture (High-Level Design)

### 1. High-Level Design (HLD) Architecture Diagram

```mermaid
flowchart TD
    %% Client Tier
    subgraph ClientTier ["1. Client & Upstream Microservices Tier"]
        ClientWeb["Web Application"]
        ClientMobile["Mobile App"]
        Microservices["Backend Microservices\n(Auth, Billing, Orders)"]
    end

    %% Ingestion & API Gateway Tier
    subgraph IngestionTier ["2. Ingestion & API Gateway Tier"]
        ExpressApp["Express.js Server\n(Helmet, CORS, JSON Parser)"]
        AuthMid["Auth Middleware\n(x-api-key / Bearer JWT)"]
        ZodVal["Zod Schema Validator\n(Conditional Cross-Field Validation)"]
        NotifController["Notification Ingestion Controller\n(Orchestration & UUID Generation)"]
    end

    %% Distributed State & Redis Store
    subgraph RedisTier ["3. Distributed State & Storage Layer (Redis Cluster / IORedis)"]
        IdemStore[("Idempotency Store\nSET NX EX 300\nKey: idempotency:<id>")]
        PrefStore[("User Preferences Cache\nKey: user:preferences:<userId>\nTTL: 3600s")]
        RateLimitStore[("Sliding Window Sorted Sets\nKey: ratelimit:<vendor>\nLua Script / ZREMRANGEBYSCORE")]
        BullMQStreams[("BullMQ Stream Storage\nQueue Keys, Delayed Sets, Hashes")]
    end

    %% Routing & Queue Segregation Tier
    subgraph QueueTier ["4. Isolated Priority & Channel Queues (BullMQ / Redis Streams)"]
        ECQ[("email_critical\n(OTPs, 2FA, Password Resets)")]
        EBQ[("email_bulk\n(Newsletters, Marketing, Digests)")]
        PCQ[("push_critical\n(Security Alerts, Order Updates)")]
        PBQ[("push_bulk\n(Promotions, Re-engagement)")]
        EDLQ[("email_dlq\n(Exhausted Retries)")]
        PDLQ[("push_dlq\n(Exhausted Retries)")]
    end

    %% Autonomous Worker Processing Tier
    subgraph WorkerTier ["5. Dedicated Autonomous Worker Fleet"]
        ECW["Email Critical Worker\nConcurrency: 25 | Priority: Real-time\nExponential Backoff (1s, 2s, 4s)"]
        EBW["Email Bulk Worker\nConcurrency: 5 | Rate Paced\nBatch Safe"]
        PCW["Push Critical Worker\nConcurrency: 30 | Priority: Real-time\nExponential Backoff (1s, 2s, 4s)"]
        PBW["Push Bulk Worker\nConcurrency: 10 | Rate Paced\nBatch Safe"]
    end

    %% Strategy & Resiliency Engine
    subgraph StrategyTier ["6. Channel Strategy & Resiliency Engine"]
        StratRegistry["Notification Strategy Registry"]
        
        subgraph EmailPipeline ["Email Strategy Pipeline"]
            EmailStrat["Email Notification Strategy"]
            EmailRateLimit{"Sliding Window\nRate Check"}
            ResendBreaker{"Resend Circuit Breaker\n(Threshold: 3 | Cooldown: 20s)"}
            SmtpBreaker{"SMTP Circuit Breaker\n(Threshold: 3 | Cooldown: 30s)"}
        end

        subgraph PushPipeline ["Push Strategy Pipeline"]
            PushStrat["Push Notification Strategy"]
            PushRateLimit{"Sliding Window\nRate Check"}
            FcmBreaker{"FCM Circuit Breaker\n(Threshold: 4 | Cooldown: 25s)"}
        end
    end

    %% Downstream Delivery Providers
    subgraph ProviderTier ["7. Downstream Third-Party Delivery Vendors"]
        ResendSDK["Resend API SDK\n(Primary Email Provider)"]
        SmtpTransport["Nodemailer SMTP\n(Fallback Email Provider)"]
        FirebaseAdmin["Firebase Admin SDK\n(FCM Push Provider)"]
    end

    %% Observability & Diagnostics
    subgraph TelemetryTier ["8. Observability & Operational Diagnostics"]
        MetricsRoute["GET /metrics\n(Queue Depths & Breaker States)"]
        HealthRoute["GET /health\n(API & Redis Liveness Probe)"]
        PinoLogger["Structured Pino JSON Logger"]
    end

    %% Connectors - Ingestion
    ClientWeb -->|POST /v1/notifications| ExpressApp
    ClientMobile -->|POST /v1/notifications| ExpressApp
    Microservices -->|POST /v1/notifications| ExpressApp

    ExpressApp --> AuthMid
    AuthMid --> ZodVal
    ZodVal --> NotifController

    %% Connectors - Controller to State
    NotifController <-->|1. Check & Acquire Lock| IdemStore
    NotifController <-->|2. Fetch Opt-out Flags| PrefStore

    %% Connectors - Queue Ingestion
    NotifController -->|Priority == CRITICAL| ECQ
    NotifController -->|Priority == BULK| EBQ
    NotifController -->|Priority == CRITICAL| PCQ
    NotifController -->|Priority == BULK| PBQ
    NotifController -.->|3. Mark Processed & Save Response| IdemStore

    %% Connectors - Worker Consumption
    ECQ --> ECW
    EBQ --> EBW
    PCQ --> PCW
    PBQ --> PBW

    %% Worker to Strategy
    ECW --> StratRegistry
    EBW --> StratRegistry
    PCW --> StratRegistry
    PBW --> StratRegistry

    StratRegistry --> EmailStrat
    StratRegistry --> PushStrat

    %% Email Strategy Dispatch
    EmailStrat <-->|Atomic Lua Eval| RateLimitStore
    EmailStrat --> EmailRateLimit
    EmailRateLimit -->|Allowed| ResendBreaker
    EmailRateLimit -->|Exceeded| ECW

    ResendBreaker -->|CLOSED / HALF-OPEN| ResendSDK
    ResendSDK -.->|Success 200 OK| ResendBreaker
    ResendSDK -.->|5xx / Timeout / Circuit OPEN| SmtpBreaker
    
    SmtpBreaker -->|CLOSED / HALF-OPEN| SmtpTransport
    SmtpTransport -.->|Success 250 OK| SmtpBreaker
    SmtpTransport -.->|Fatal Error / All Retries Failed| EDLQ

    %% Push Strategy Dispatch
    PushStrat <-->|Atomic Lua Eval| RateLimitStore
    PushStrat --> PushRateLimit
    PushRateLimit -->|Allowed| FcmBreaker
    PushRateLimit -->|Exceeded| PCW

    FcmBreaker -->|CLOSED / HALF-OPEN| FirebaseAdmin
    FirebaseAdmin -.->|Success Message ID| FcmBreaker
    FirebaseAdmin -.->|Fatal Error / All Retries Failed| PDLQ

    %% Observability Wiring
    MetricsRoute -.-> BullMQStreams
    MetricsRoute -.-> StratRegistry
    HealthRoute -.-> ExpressApp
    HealthRoute -.-> RedisTier
```

---

### 2. End-to-End Sequence & Request Lifecycle Diagram

```mermaid
sequenceDiagram
    autonumber
    actor Client as Client / Microservice
    participant Gateway as Express Gateway & Auth
    participant Zod as Zod Schema Validator
    participant Idem as Redis Idempotency Service
    participant Pref as User Preference Cache
    participant Queues as BullMQ Redis Queues
    participant Worker as Autonomous Worker Fleet
    participant Limiter as Redis Lua Rate Limiter
    participant Breaker as Circuit Breaker Engine
    participant Primary as Primary Provider (Resend / FCM)
    participant Fallback as Fallback Provider (Nodemailer SMTP)
    participant DLQ as Dead Letter Queue (DLQ)

    %% Ingestion Phase
    Client->>Gateway: POST /v1/notifications (Bearer Token / API Key)
    Gateway->>Gateway: Validate Authentication Credentials
    Gateway->>Zod: Validate Request Body Structure & Constraints
    Zod-->>Gateway: Validation Passed

    Gateway->>Idem: checkAndAcquire(idempotencyKey)
    alt Idempotency Key Exists (Duplicate Request)
        Idem-->>Gateway: isDuplicate = true (Cached Result)
        Gateway-->>Client: HTTP 202 Accepted (Cached Payload)
    else First Ingestion (Lock Acquired via SET NX EX 300)
        Idem-->>Gateway: isDuplicate = false
        
        loop For each channel in [EMAIL, PUSH]
            Gateway->>Pref: isChannelAllowed(userId, channel, priority)
            alt User Opted-Out
                Pref-->>Gateway: allowed = false (reason: opt-out)
                Gateway->>Gateway: Record Skipped Channel
            else Allowed
                Pref-->>Gateway: allowed = true
                Gateway->>Queues: enqueue(channel_priority_queue, jobPayload)
                Queues-->>Gateway: Enqueued (jobId generated)
            end
        end

        Gateway->>Idem: markProcessed(idempotencyKey, responsePayload)
        Gateway-->>Client: HTTP 200 OK (notificationId, enqueuedChannels)
    end

    %% Asynchronous Processing Phase
    Note over Queues,Worker: Asynchronous Consumer Decoupling
    Queues->>Worker: Dequeue Job (e.g., email_critical)
    Worker->>Limiter: checkRateLimit(EMAIL_VENDOR, max, windowMs)

    alt Rate Limit Exceeded
        Limiter-->>Worker: allowed = false (retryAfterMs = 1200ms)
        Worker->>Queues: Delay Job (job.moveToDelayed)
    else Rate Limit Permitted
        Limiter-->>Worker: allowed = true
        Worker->>Breaker: execute(resendProvider.send)
        
        alt Resend Circuit Breaker is OPEN
            Breaker-->>Worker: Fast-Fail (Trip immediately to fallback)
            Worker->>Fallback: smtpProvider.send(payload)
            Fallback-->>Worker: Delivery Succeeded (250 OK)
        else Resend Circuit Breaker is CLOSED / HALF-OPEN
            Breaker->>Primary: resendSDK.emails.send()
            alt Primary Delivery Succeeds
                Primary-->>Breaker: Success (200 OK)
                Breaker-->>Worker: Ack Success
            else Primary Delivery Fails / Times out
                Primary-->>Breaker: Error (500 Internal / Socket Timeout)
                Breaker->>Breaker: Record Failure (Trip if threshold reached)
                Breaker-->>Worker: Primary Failed
                Worker->>Fallback: smtpProvider.send(payload)
                alt Fallback Delivery Succeeds
                    Fallback-->>Worker: Success (250 OK)
                else Fallback Delivery Fails
                    Fallback-->>Worker: Smtp Error
                    Worker->>Worker: Retry with Exponential Backoff (1s, 2s, 4s)
                    alt Max Retries Exceeded
                        Worker->>DLQ: Route to email_dlq with error stack
                    end
                end
            end
        end
    end
```

---

## Deep-Dive Component Explanations

### 1. API Ingestion Gateway & Security Middleware
- **Source Files**: [`src/app.ts`](file:///Users/rishankkesharwani/Documents/Personal/notification-service/src/app.ts), [`src/middlewares/auth.middleware.ts`](file:///Users/rishankkesharwani/Documents/Personal/notification-service/src/middlewares/auth.middleware.ts), [`src/middlewares/validate.middleware.ts`](file:///Users/rishankkesharwani/Documents/Personal/notification-service/src/middlewares/validate.middleware.ts), [`src/types/zod-schemas.ts`](file:///Users/rishankkesharwani/Documents/Personal/notification-service/src/types/zod-schemas.ts)
- **Role & Purpose**: Serves as the single unified ingestion point for all notification requests across web applications, mobile backends, and microservices.
- **Internal Mechanics**:
  - **Security Shield**: Configured with `helmet` for secure HTTP headers, `cors` for origin restriction, and standard JSON payload body parsers.
  - **Dual-Mode Authentication**: Supports dual auth modes:
    - Shared API Key Header (`x-api-key`).
    - Standard RFC 6750 Bearer JWT Authentication (`Authorization: Bearer <token>`).
  - **Zod Runtime Schema Validation**: Intercepts requests before reaching controllers. Executes strict cross-field validations:
    - If `EMAIL` is in `channels`, the `email` payload object (`subject`, `html`/`text`, optional `attachments`) is mandatory.
    - If `PUSH` is in `channels`, the `push` payload object (`title`, `body`) is mandatory.
    - Attachments are strictly validated for URL format, MIME type, and size bounds.
- **Performance**: Completely stateless and non-blocking, responding to upstream callers in `< 10ms`.

---

### 2. Distributed Idempotency Engine
- **Source File**: [`src/services/idempotency.service.ts`](file:///Users/rishankkesharwani/Documents/Personal/notification-service/src/services/idempotency.service.ts)
- **Role & Purpose**: Guarantees distributed **Exactly-Once Ingestion Semantics**, preventing duplicate email/push dispatches caused by client retries, network glitches, or webhook replay storms.
- **Internal Mechanics**:
  - **Atomic Acquisition (`checkAndAcquire`)**: Uses Redis `SET idempotency:<key> <payload> NX EX 300` (5-minute TTL).
  - **Locking States**:
    - `PENDING`: Initial lock acquired. Prevents concurrent duplicate requests from enqueueing jobs simultaneously.
    - `PROCESSED`: Once jobs are placed in BullMQ, the full response payload is cached in Redis.
  - **Cache Hit Resolution**: If a duplicate request arrives within 300 seconds, the engine detects the existing record and instantly returns **HTTP 202 Accepted** with the cached job IDs without touching the queue.
- **Failure Handling**: If the ingestion controller crashes before enqueueing, the 300-second TTL automatically expires, freeing the key for subsequent retry.

---

### 3. User Preference & Channel Opt-Out Engine
- **Source File**: [`src/services/preference.service.ts`](file:///Users/rishankkesharwani/Documents/Personal/notification-service/src/services/preference.service.ts)
- **Role & Purpose**: Evaluates user notification preferences, channel-level opt-outs, and marketing bulk subscription settings before messages enter the queues.
- **Internal Mechanics**:
  - **Cached Preference Lookups**: Queries Redis key `user:preferences:<userId>` with a 1-hour TTL. Defaults to un-opted state on cache miss and writes back default preferences.
  - **Priority-Aware Filtering Rules**:
    - `emailOptOut = true` $\rightarrow$ Drops `EMAIL` channel regardless of priority (hard opt-out).
    - `pushOptOut = true` $\rightarrow$ Drops `PUSH` channel regardless of priority (hard opt-out).
    - `bulkOptOut = true` $\rightarrow$ Drops messages with `priority == 'BULK'` (e.g. newsletters, promos), but **never blocks** `CRITICAL` notifications (e.g., OTPs, 2FA, password resets, fraud alerts).
  - **Skipped Channel Feedback**: If a channel is skipped, the API response informs the client with clear diagnostic reasoning in the `skippedChannels` array without failing the valid channels.

---

### 4. Notification Router & Queue Dispatcher
- **Source Files**: [`src/controllers/notification.controller.ts`](file:///Users/rishankkesharwani/Documents/Personal/notification-service/src/controllers/notification.controller.ts), [`src/queues/queue.registry.ts`](file:///Users/rishankkesharwani/Documents/Personal/notification-service/src/queues/queue.registry.ts)
- **Role & Purpose**: Unpacks validated multi-channel notification requests, generates immutable unique notification IDs (`notif_<timestamp>_<randomHex>`), and routes channel jobs to their isolated queues.
- **Internal Mechanics**:
  - **Channel Decoupling**: If a single request requests `["EMAIL", "PUSH"]`, the router creates two independent jobs: one for email and one for push.
  - **Priority Queue Resolution**:
    - `EMAIL` + `CRITICAL` $\rightarrow$ Enqueued to `email_critical`
    - `EMAIL` + `BULK` $\rightarrow$ Enqueued to `email_bulk`
    - `PUSH` + `CRITICAL` $\rightarrow$ Enqueued to `push_critical`
    - `PUSH` + `BULK` $\rightarrow$ Enqueued to `push_bulk`
  - **Retry Policy Injection**: Injects retry configurations: 3 attempts for critical jobs with exponential backoff (`1000ms * 2^(attempt - 1)`), 2 attempts for bulk jobs with fixed delay.

---

### 5. Multi-Queue & Channel Segregation Topology
- **Source File**: [`src/queues/queue.registry.ts`](file:///Users/rishankkesharwani/Documents/Personal/notification-service/src/queues/queue.registry.ts)
- **Role & Purpose**: Implements queue-level isolation to prevent large-volume marketing campaigns from blocking high-priority transactional OTPs or security alerts.
- **Queue Breakdown**:

| Queue Name | Primary Message Types | Concurrency | Backoff Strategy | DLQ Target |
| :--- | :--- | :--- | :--- | :--- |
| `email_critical` | OTPs, 2FA, Password Resets, Receipts | 25 Workers | Exponential (1s, 2s, 4s) | `email_dlq` |
| `email_bulk` | Newsletters, Weekly Digests, Product Updates | 5 Workers | Fixed Delay (5s) | `email_dlq` |
| `push_critical` | Account Lockout, 2FA Push, Fraud Alerts | 30 Workers | Exponential (1s, 2s, 4s) | `push_dlq` |
| `push_bulk` | Marketing Pushes, Re-engagement, Tips | 10 Workers | Fixed Delay (5s) | `push_dlq` |
| `email_dlq` | Permanently failed email delivery payloads | 0 (Storage only) | None (Manual / Cron Replay) | N/A |
| `push_dlq` | Permanently failed push delivery payloads | 0 (Storage only) | None (Manual / Cron Replay) | N/A |

- **Underlying Engine**: Powered by BullMQ using Redis Streams, Redis Sorted Sets (for delayed/scheduled jobs), and Redis Hashes for job data persistence.

---

### 6. Autonomous Worker Fleet & Cluster Runner
- **Source Files**: [`src/workers/email-critical.worker.ts`](file:///Users/rishankkesharwani/Documents/Personal/notification-service/src/workers/email-critical.worker.ts), [`src/workers/email-bulk.worker.ts`](file:///Users/rishankkesharwani/Documents/Personal/notification-service/src/workers/email-bulk.worker.ts), [`src/workers/push-critical.worker.ts`](file:///Users/rishankkesharwani/Documents/Personal/notification-service/src/workers/push-critical.worker.ts), [`src/workers/push-bulk.worker.ts`](file:///Users/rishankkesharwani/Documents/Personal/notification-service/src/workers/push-bulk.worker.ts), [`src/workers/worker-runner.ts`](file:///Users/rishankkesharwani/Documents/Personal/notification-service/src/workers/worker-runner.ts)
- **Role & Purpose**: Dedicated background worker pools that consume jobs from specific BullMQ queues, invoke channel strategies, handle backoff retries, and route poison jobs to the DLQ.
- **Internal Mechanics**:
  - **Dedicated Worker Classes**: Each queue has an independent class instance (`EmailCriticalWorker`, `EmailBulkWorker`, etc.) configured with channel-appropriate concurrency and rate settings.
  - **Failure Interception (`onFailed`)**: When a job exhausts all attempts (`job.attemptsMade >= job.opts.attempts`), the worker automatically captures the error stack and moves the job into the corresponding Dead Letter Queue (`email_dlq` or `push_dlq`).
  - **Worker Runner (`worker-runner.ts`)**: Supports selective target booting via CLI arguments (`npm run worker:email:critical`, `npm run worker:push:critical`, or `TARGET=all`), enabling seamless deployment across independent Kubernetes pods or autoscaling groups.

---

### 7. Channel Strategy Layer
- **Source Files**: [`src/patterns/strategy/notification-strategy.interface.ts`](file:///Users/rishankkesharwani/Documents/Personal/notification-service/src/patterns/strategy/notification-strategy.interface.ts), [`src/patterns/strategy/strategy.registry.ts`](file:///Users/rishankkesharwani/Documents/Personal/notification-service/src/patterns/strategy/strategy.registry.ts), [`src/patterns/strategy/email.strategy.ts`](file:///Users/rishankkesharwani/Documents/Personal/notification-service/src/patterns/strategy/email.strategy.ts), [`src/patterns/strategy/push.strategy.ts`](file:///Users/rishankkesharwani/Documents/Personal/notification-service/src/patterns/strategy/push.strategy.ts)
- **Role & Purpose**: Implements the **Strategy Design Pattern** to decouple message processing, rate limiting, and vendor dispatch from worker loops and business logic.
- **Internal Mechanics**:
  - **Uniform Interface (`INotificationChannelStrategy<T>`)**: Enforces a standard `process(jobData: T): Promise<ProviderResponse>` signature.
  - **Encapsulated Resiliency**: Each strategy instance encapsulates its own rate limiters, primary provider, fallback providers, and circuit breakers.
  - **Extensibility**: Adding a new notification channel (e.g. `SMS`, `WHATSAPP`, `WEBHOOK`, `SLACK`) requires only implementing `INotificationChannelStrategy` and registering it with `NotificationStrategyRegistry`.

---

### 8. Distributed Sliding-Window Rate Limiter
- **Source File**: [`src/services/rate-limiter.service.ts`](file:///Users/rishankkesharwani/Documents/Personal/notification-service/src/services/rate-limiter.service.ts)
- **Role & Purpose**: Enforces sliding-window rate limits across all distributed worker nodes to strictly comply with vendor API quotas (e.g., Resend 100 req/sec, FCM 500 req/sec).
- **Algorithmic Implementation (Redis Lua Script)**:
  ```lua
  local key = KEYS[1]
  local now = tonumber(ARGV[1])
  local windowMs = tonumber(ARGV[2])
  local maxLimit = tonumber(ARGV[3])
  local memberId = ARGV[4]
  local clearBefore = now - windowMs

  -- 1. Remove timestamps outside current sliding window
  redis.call('ZREMRANGEBYSCORE', key, '-inf', clearBefore)

  -- 2. Count active requests within current window
  local currentCount = redis.call('ZCARD', key)

  if currentCount < maxLimit then
    -- 3. Add current timestamp with unique member ID
    redis.call('ZADD', key, now, memberId)
    redis.call('PEXPIRE', key, windowMs * 2)
    return {1, 0, currentCount + 1}
  else
    -- 4. Calculate exact retry delay based on oldest element
    local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
    local retryAfterMs = windowMs
    if oldest and oldest[2] then
      retryAfterMs = math.max(1, (tonumber(oldest[2]) + windowMs) - now)
    end
    return {0, retryAfterMs, currentCount}
  end
  ```
- **Smooth Backpressure**: If rate limit is reached, workers delay the job using BullMQ's `job.moveToDelayed(Date.now() + retryAfterMs)` without dropping the request or spinning in CPU-heavy wait loops.

---

### 9. Multi-Tier Resilient Circuit Breakers
- **Source File**: [`src/patterns/circuit-breaker/circuit-breaker.ts`](file:///Users/rishankkesharwani/Documents/Personal/notification-service/src/patterns/circuit-breaker/circuit-breaker.ts)
- **Role & Purpose**: Prevents cascading timeouts and worker thread starvation when external delivery vendors experience 5xx server outages, SSL timeouts, or network degradation.
- **State Machine Mechanics**:
  - **`CLOSED`**: Normal operation. Requests flow directly to the provider. Every consecutive failure increments `failureCount`.
  - **`OPEN`**: Tripped when `failureCount >= failureThreshold` (e.g. 3 consecutive failures). All incoming requests **immediately fail-fast** without calling the vendor API or waiting for network timeouts.
  - **`HALF_OPEN`**: Entered automatically when `recoveryTimeoutMs` (e.g., 20–30 seconds) elapses. Allows a single probe request through:
    - If probe succeeds $\rightarrow$ Transitions back to `CLOSED` and resets failure counters.
    - If probe fails $\rightarrow$ Re-trips to `OPEN` for another full cooldown duration.
- **Provider Protection Instances**:
  - `Resend_API_Breaker`: Failure threshold 3, cooldown 20s.
  - `Nodemailer_SMTP_Breaker`: Failure threshold 3, cooldown 30s.
  - `Firebase_FCM_Breaker`: Failure threshold 4, cooldown 25s.

---

### 10. Downstream Provider Adapters & Multi-Provider Failover
- **Source Files**: [`src/providers/email/resend.provider.ts`](file:///Users/rishankkesharwani/Documents/Personal/notification-service/src/providers/email/resend.provider.ts), [`src/providers/email/smtp.provider.ts`](file:///Users/rishankkesharwani/Documents/Personal/notification-service/src/providers/email/smtp.provider.ts), [`src/providers/push/fcm.provider.ts`](file:///Users/rishankkesharwani/Documents/Personal/notification-service/src/providers/push/fcm.provider.ts)
- **Role & Purpose**: Encapsulates external SDK protocols, converts internal job data into provider-compliant network payloads, and manages multi-provider failover chains.
- **Email Primary-Fallback Chain**:
  1. `ResendEmailProvider` (Primary via Resend REST API SDK): High deliverability, attachments support, structured response tracking.
  2. If Resend fails or `Resend_API_Breaker` is `OPEN`, execution **instantly shifts** to `SmtpEmailProvider` (Nodemailer SMTP Transport).
  3. Returns standard `ProviderResponse` (`{ success: true, messageId, provider: 'smtp' }`) with full telemetry.
- **Push Provider**:
  - `FcmPushProvider` (Firebase Admin FCM): Dispatches push notifications to specific device tokens or multicast topics with custom data payloads and priority flags.

---

### 11. Dead Letter Queue (DLQ) & Poison Pill Isolation
- **Source Files**: [`src/queues/queue.registry.ts`](file:///Users/rishankkesharwani/Documents/Personal/notification-service/src/queues/queue.registry.ts), [`src/workers/email-critical.worker.ts`](file:///Users/rishankkesharwani/Documents/Personal/notification-service/src/workers/email-critical.worker.ts), [`src/workers/push-critical.worker.ts`](file:///Users/rishankkesharwani/Documents/Personal/notification-service/src/workers/push-critical.worker.ts)
- **Role & Purpose**: Quarantines permanently un-deliverable or malformed notification jobs after all retries and fallback providers have failed.
- **Internal Mechanics**:
  - `email_dlq` and `push_dlq` store the complete original notification payload along with:
    - `failedAt`: ISO timestamp of terminal failure.
    - `attemptsMade`: Number of retries attempted.
    - `errorStack`: Full stack trace and error message from the last failed attempt.
    - `lastProviderAttempted`: Identifier of the last provider executed.
  - **Poison Pill Protection**: Prevents bad data (e.g., malformed recipient tokens or deleted email domains) from repeatedly crashing worker threads or blocking subsequent jobs.

---

### 12. Real-Time Observability, Diagnostics & Health Monitoring
- **Source Files**: [`src/controllers/notification.controller.ts`](file:///Users/rishankkesharwani/Documents/Personal/notification-service/src/controllers/notification.controller.ts), [`src/config/logger.ts`](file:///Users/rishankkesharwani/Documents/Personal/notification-service/src/config/logger.ts)
- **Role & Purpose**: Exposes real-time system telemetry, queue waiting/active counts, circuit breaker states, and Kubernetes liveness/readiness probes.
- **Diagnostic Capabilities**:
  - **`GET /metrics`**: Queries BullMQ queue metrics across all 6 queues (`waiting`, `active`, `completed`, `failed`, `delayed`) and aggregates live diagnostics for every Circuit Breaker (`state`, `failureCount`, `successCount`, `nextAttemptAt`).
  - **`GET /health`**: Tests Express server status and validates the IORedis connection (`PING` $\rightarrow$ `PONG`).
  - **Structured JSON Logging (Pino)**: Every request and worker event logs `notificationId`, `channel`, `priority`, `durationMs`, and `idempotencyKey` for unified ingestion into Datadog, Grafana Loki, or AWS CloudWatch.

---

## Design Patterns Implemented

### 1. Strategy Design Pattern
The **Strategy Pattern** is used to encapsulate channel-specific delivery mechanisms, rate limiting, and vendor dispatching behaviors behind a common interface (`INotificationChannelStrategy`).

#### Class Structure
```mermaid
classDiagram
    class INotificationChannelStrategy {
        <<interface>>
        +channel: NotificationChannel
        +process(jobData: ChannelJobData): Promise~ProviderResponse~
    }

    class EmailNotificationStrategy {
        +channel: NotificationChannel
        +resendCircuitBreaker: CircuitBreaker
        +smtpCircuitBreaker: CircuitBreaker
        +process(jobData: EmailJobData): Promise~ProviderResponse~
    }

    class PushNotificationStrategy {
        +channel: NotificationChannel
        +fcmCircuitBreaker: CircuitBreaker
        +process(jobData: PushJobData): Promise~ProviderResponse~
    }

    class NotificationStrategyRegistry {
        -strategies: Map
        +registerStrategy(strategy)
        +getStrategy(channel)
        +getEmailStrategy()
        +getPushStrategy()
        +getAllCircuitBreakerDiagnostics()
    }

    INotificationChannelStrategy <|.. EmailNotificationStrategy
    INotificationChannelStrategy <|.. PushNotificationStrategy
    NotificationStrategyRegistry o-- INotificationChannelStrategy
```

---

### 2. Circuit Breaker Design Pattern
The **Circuit Breaker Pattern** safeguards the service against cascading failures and thread/connection exhaustion when downstream third-party vendors (Resend API, Gmail SMTP, Firebase FCM) experience outages, severe rate limits, or network timeouts.

#### State Machine

```mermaid
stateDiagram-v2
    [*] --> CLOSED: Initial State
    
    CLOSED --> OPEN: Failure threshold reached (e.g. 3 consecutive errors)
    note right of CLOSED
      Requests pass through to vendor.
      Failures increment counter.
    end note
    
    OPEN --> HALF_OPEN: Cooldown timer expires (e.g. 20-30s)
    note right of OPEN
      Fast-fail immediately.
      Do not hit downstream vendor.
      Route immediately to fallback.
    end note
    
    HALF_OPEN --> CLOSED: Probe request succeeds (target met)
    HALF_OPEN --> OPEN: Probe request fails
    note right of HALF_OPEN
      Allow single probe request
      to test vendor recovery.
    end note
```

---

### 3. Producer-Consumer & Channel Segregation Pattern
To prevent a single busy channel or marketing blast from choking time-sensitive notifications (e.g., OTPs or password resets), the system uses **isolated BullMQ queues and worker clusters**:
- **Critical Queues** (`email_critical`, `push_critical`): Processed by high-concurrency workers with immediate exponential backoff retries.
- **Bulk Queues** (`email_bulk`, `push_bulk`): Processed with controlled concurrency to prevent vendor rate-limit throttling.
- **Dead Letter Queues** (`email_dlq`, `push_dlq`): Retains permanently failed messages for inspection, alerting, or manual reprocessing.

---

### 4. Distributed Idempotent Receiver Pattern
- Implemented via atomic Redis `SET idempotency:<key> <payload> NX EX 300`.
- If an identical payload is submitted within the 5-minute TTL window, the service returns **HTTP 202 Accepted** with the cached job status without scheduling duplicate deliveries.

---

## Zero-Bottleneck Worker Segregation

> [!IMPORTANT]
> **Why 4 Independent Worker Clusters?**
> If Critical (OTPs/Security) and Bulk (Newsletters/Promos) jobs were processed by a shared generic worker pool, a batch of 50,000 marketing emails would consume all worker threads and Redis connections, delaying critical OTPs by minutes.
> 
> In this architecture:
> 1. **`EmailCriticalWorker`**: Dedicated process pool (concurrency: 25). Instant OTP delivery.
> 2. **`EmailBulkWorker`**: Isolated throttled pool (concurrency: 5). Prevents vendor rate bans.
> 3. **`PushCriticalWorker`**: Dedicated process pool (concurrency: 30). Real-time 2FA/fraud alerts.
> 4. **`PushBulkWorker`**: Isolated throttled pool (concurrency: 10). Background marketing pushes.
> 
> Each worker cluster can be scaled, deployed, and restarted **independently** across separate server nodes or Kubernetes pods.

---

## Tech Stack

| Technology | Purpose |
| :--- | :--- |
| **Node.js & TypeScript** | Strict-typed asynchronous runtime environment |
| **Express.js** | Non-blocking HTTP ingestion server |
| **BullMQ** | Distributed queue engine backed by Redis Streams |
| **IORedis** | High-performance Redis client with Lua script execution support |
| **Zod** | Runtime schema validation with conditional cross-field validation |
| **Resend SDK** | Primary transactional email delivery service |
| **Nodemailer (SMTP)** | Fallback email transport provider |
| **Firebase Admin (FCM)** | Push notifications delivery engine |
| **Pino & Pino-Pretty** | Low-overhead structured JSON logging |
| **Helmet & CORS** | HTTP security headers and CORS protection |

---

## Project Directory Structure

```
.
├── docker-compose.yml              # Redis service container
├── package.json                    # Dependencies and scripts
├── tsconfig.json                   # Strict TypeScript compiler options
├── .env.example                    # Environment variable template
├── .gitignore                      # Git exclusion rules (.env, node_modules)
└── src
    ├── app.ts                      # Express application setup
    ├── server.ts                   # Main server entry point
    ├── config
    │   ├── env.ts                  # Zod-validated environment config
    │   ├── logger.ts               # Structured Pino logger
    │   └── redis.ts                # IORedis connection pools & factory
    ├── controllers
    │   └── notification.controller.ts # Ingestion, metrics & preference controllers
    ├── middlewares
    │   ├── auth.middleware.ts      # API Key & Bearer JWT authentication
    │   ├── error.middleware.ts     # Centralized error handler
    │   └── validate.middleware.ts  # Zod schema validation middleware
    ├── patterns
    │   ├── circuit-breaker
    │   │   └── circuit-breaker.ts  # CLOSED, OPEN, HALF_OPEN Circuit Breaker
    │   └── strategy
    │       ├── email.strategy.ts   # Email strategy with dual circuit breakers
    │       ├── push.strategy.ts    # Push strategy with FCM circuit breaker
    │       ├── notification-strategy.interface.ts # Strategy contract
    │       └── strategy.registry.ts # Strategy Context / Registry
    ├── providers
    │   ├── email
    │   │   ├── email.interface.ts  # IEmailProvider contract
    │   │   ├── email-fallback.service.ts # Primary-Fallback orchestrator
    │   │   ├── resend.provider.ts  # Resend SDK provider
    │   │   └── smtp.provider.ts    # Nodemailer SMTP fallback provider
    │   └── push
    │       ├── push.interface.ts   # IPushProvider contract
    │       └── fcm.provider.ts     # Firebase FCM provider
    ├── queues
    │   └── queue.registry.ts       # BullMQ queue definitions & metrics
    ├── routes
    │   └── notification.routes.ts  # API route definitions
    ├── services
    │   ├── idempotency.service.ts  # Atomic Redis SET NX EX idempotency
    │   ├── preference.service.ts   # User opt-out & preference caching
    │   └── rate-limiter.service.ts # Lua sliding-window vendor rate limiter
    ├── types
    │   ├── notification.ts         # TypeScript interfaces & types
    │   └── zod-schemas.ts          # Zod validation schemas
    └── workers
        ├── email-critical.worker.ts # Dedicated Email Critical BullMQ worker (OTPs, Security)
        ├── email-bulk.worker.ts     # Dedicated Email Bulk BullMQ worker (Newsletters, Promos)
        ├── push-critical.worker.ts  # Dedicated Push Critical BullMQ worker (2FA, Fraud)
        ├── push-bulk.worker.ts      # Dedicated Push Bulk BullMQ worker (Marketing, Updates)
        ├── index.ts                 # Worker registry exports
        └── worker-runner.ts         # Multi-target standalone worker cluster runner
```

---

## Getting Started

### Prerequisites
- **Node.js**: v18.0.0 or higher
- **Redis**: v6.0 or higher (or Docker)

### 1. Start Redis
```bash
# Start Redis with Docker Compose
docker compose up -d

# Or with Homebrew (macOS)
brew services start redis
```

### 2. Configure Environment Variables
Copy `.env.example` to `.env` and configure your credentials:
```bash
cp .env.example .env
```

Key environment variables:
```ini
PORT=3000
NODE_ENV=development
API_KEY=test-api-key-12345
JWT_SECRET=super-secret-jwt-key

REDIS_HOST=localhost
REDIS_PORT=6379

# Email Provider Config
RESEND_API_KEY=re_your_resend_api_key
EMAIL_FROM=onboarding@resend.dev
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=your_email@gmail.com
SMTP_PASS=your_app_password
SMTP_FROM="Notification Service <your_email@gmail.com>"

# Push Provider Config
FCM_PROJECT_ID=your-project-id
FCM_CLIENT_EMAIL=your-sa@iam.gserviceaccount.com
FCM_PRIVATE_KEY="-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n"
```

### 3. Install Dependencies & Build
```bash
npm install
npm run build
```

### 4. Running the Service

#### Mode A: Unified Server (API + Embedded Workers)
Ideal for single-container deployments and local development:
```bash
npm run dev        # Development mode (ts-node-dev with hot-reload)
npm start          # Production mode (compiled JS from dist/)
```

#### Mode B: Decoupled Independent Worker Clusters
Ideal for horizontally scalable production architectures where each queue has its own dedicated autoscaling group or pod:

```bash
# Ingestion API Nodes (Zero worker processing overhead)
WORKERS_EMBEDDED=false npm run dev

# Independent Worker Nodes:
npm run worker:email:critical   # Dedicated Email Critical Worker (OTPs, Security)
npm run worker:email:bulk       # Dedicated Email Bulk Worker (Newsletters, Promos)
npm run worker:push:critical    # Dedicated Push Critical Worker (Security, 2FA)
npm run worker:push:bulk        # Dedicated Push Bulk Worker (Marketing, Updates)

# Or run all 4 worker clusters in one process:
npm run dev:workers
```

---

## API Reference & cURL Examples

All protected endpoints require either:
- Header `x-api-key: <API_KEY>` (e.g. `test-api-key-12345`)
- Header `Authorization: Bearer <JWT_TOKEN>`

### 1. Ingest Dual-Channel Notification (Email + Push)
`POST /v1/notifications`

```bash
curl -X POST http://localhost:3000/v1/notifications \
  -H "Content-Type: application/json" \
  -H "x-api-key: test-api-key-12345" \
  -d '{
    "idempotencyKey": "order_checkout_8899",
    "priority": "CRITICAL",
    "channels": ["EMAIL", "PUSH"],
    "recipient": {
      "userId": "usr_alpha_1",
      "email": "customer@example.com",
      "pushToken": "fcm_device_token_sample"
    },
    "email": {
      "subject": "Order #8899 Confirmation",
      "html": "<h2>Payment Received!</h2><p>Your invoice is attached.</p>",
      "attachments": [
        {
          "filename": "invoice_8899.pdf",
          "path": "https://s3.amazonaws.com/my-bucket/invoices/inv_8899.pdf"
        }
      ]
    },
    "push": {
      "title": "Order #8899 Dispatched",
      "body": "Your package is now out for delivery."
    },
    "metadata": {
      "orderId": "8899",
      "orderTotal": 149.99
    }
  }'
```

**Response (HTTP 200 OK):**
```json
{
  "success": true,
  "message": "Notification enqueued successfully",
  "notificationId": "notif_1725267890123_4a9b",
  "idempotencyStatus": "PROCESSED",
  "enqueuedChannels": [
    {
      "channel": "EMAIL",
      "queue": "email_critical",
      "jobId": "send_email_notif_1725267890123_4a9b"
    },
    {
      "channel": "PUSH",
      "queue": "push_critical",
      "jobId": "send_push_notif_1725267890123_4a9b"
    }
  ]
}
```

---

### 2. Distributed Idempotency Verification
Re-sending the identical request with `idempotencyKey: "order_checkout_8899"` returns **HTTP 202 Accepted** immediately:

```json
{
  "success": true,
  "message": "Notification enqueued successfully",
  "notificationId": "notif_1725267890123_4a9b",
  "idempotencyStatus": "PROCESSED",
  "enqueuedChannels": [
    { "channel": "EMAIL", "queue": "email_critical", "jobId": "send_email_notif_1725267890123_4a9b" },
    { "channel": "PUSH", "queue": "push_critical", "jobId": "send_push_notif_1725267890123_4a9b" }
  ]
}
```

---

### 3. User Preference & Opt-Out Filtering

#### Set User Preferences
`PUT /v1/preferences/:userId`

```bash
curl -X PUT http://localhost:3000/v1/preferences/usr_alpha_1 \
  -H "Content-Type: application/json" \
  -H "x-api-key: test-api-key-12345" \
  -d '{
    "bulkOptOut": true,
    "pushOptOut": false
  }'
```

#### Fetch User Preferences
`GET /v1/preferences/:userId`

```bash
curl http://localhost:3000/v1/preferences/usr_alpha_1 \
  -H "x-api-key: test-api-key-12345"
```

---

### 4. Queue & Circuit Breaker Diagnostics Endpoint
`GET /metrics`

Provides real-time visibility into queue depths and circuit breaker health:

```bash
curl http://localhost:3000/metrics \
  -H "x-api-key: test-api-key-12345"
```

**Response:**
```json
{
  "success": true,
  "timestamp": "2026-09-02T09:35:00.000Z",
  "queues": {
    "email_critical": { "waiting": 0, "active": 0, "completed": 24, "failed": 0, "delayed": 0 },
    "email_bulk": { "waiting": 0, "active": 0, "completed": 105, "failed": 0, "delayed": 0 },
    "email_dlq": { "waiting": 0, "active": 0, "completed": 0, "failed": 0, "delayed": 0 },
    "push_critical": { "waiting": 0, "active": 0, "completed": 18, "failed": 0, "delayed": 0 },
    "push_bulk": { "waiting": 0, "active": 0, "completed": 50, "failed": 0, "delayed": 0 },
    "push_dlq": { "waiting": 0, "active": 0, "completed": 0, "failed": 0, "delayed": 0 }
  },
  "circuitBreakers": [
    {
      "name": "Resend_API_Breaker",
      "state": "CLOSED",
      "failureCount": 0,
      "successCount": 0,
      "failureThreshold": 3,
      "recoveryTimeoutMs": 20000,
      "nextAttemptAt": null
    },
    {
      "name": "Nodemailer_SMTP_Breaker",
      "state": "CLOSED",
      "failureCount": 0,
      "successCount": 0,
      "failureThreshold": 3,
      "recoveryTimeoutMs": 30000,
      "nextAttemptAt": null
    },
    {
      "name": "Firebase_FCM_Breaker",
      "state": "CLOSED",
      "failureCount": 0,
      "successCount": 0,
      "failureThreshold": 4,
      "recoveryTimeoutMs": 25000,
      "nextAttemptAt": null
    }
  ]
}
```

---

### 5. Health Check Endpoint
`GET /health` (Public)

```bash
curl http://localhost:3000/health
```

**Response:**
```json
{
  "status": "UP",
  "timestamp": "2026-09-02T09:35:00.000Z",
  "services": {
    "api": "UP",
    "redis": "HEALTHY"
  }
}
```

---

## Resiliency & Error Handling Matrix

| Failure Scenario | Component Handling | Recovery / Mitigation Mechanism |
| :--- | :--- | :--- |
| **Client sends rapid duplicate requests** | `IdempotencyService` | Atomic Redis `SET NX EX 300` returns cached HTTP 202 without re-queueing. |
| **User opted out of marketing emails** | `UserPreferenceService` | Evaluates opt-out cache and skips `EMAIL` for `BULK` priority; allows `CRITICAL`. |
| **Downstream vendor quota reached** | `VendorRateLimiterService` | Sliding-window Lua script delays the job via BullMQ `job.moveToDelayed()` without message drop. |
| **Primary Email (Resend) 5xx / Outage** | `ResendCircuitBreaker` + `EmailStrategy` | Breaker trips to `OPEN` after 3 failures; requests immediately route to `SmtpEmailProvider`. |
| **Both Primary and Fallback providers fail** | Worker Retry Loop | Retries job 3 times with exponential backoff (`1s`, `2s`, `4s`). |
| **Max retries exhausted (Poison Pill)** | Worker `onFailed` Hook | Moves job to `email_dlq` or `push_dlq` with full stack trace for operational triage. |
| **Redis cluster connection blip** | `IORedis` Reconnect | Auto-reconnect with exponential backoff; health check returns `DOWN` until restored. |

---

## License
MIT
