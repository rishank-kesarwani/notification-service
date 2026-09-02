# Production-Grade Dual-Channel Notification Engine (Email & Push)

A high-performance, non-blocking, and resilient multi-channel notification engine built with **Node.js, TypeScript, Express, Redis, BullMQ, and Zod**.

---

## Table of Contents
- [Architecture Overview](#architecture-overview)
- [Design Patterns Implemented](#design-patterns-implemented)
  - [1. Strategy Design Pattern](#1-strategy-design-pattern)
  - [2. Circuit Breaker Design Pattern](#2-circuit-breaker-design-pattern)
  - [3. Producer-Consumer & Channel Segregation Pattern](#3-producer-consumer--channel-segregation-pattern)
  - [4. Distributed Idempotent Receiver Pattern](#4-distributed-idempotent-receiver-pattern)
- [Queue & Channel Segregation Topology](#queue--channel-segregation-topology)
- [Tech Stack](#tech-stack)
- [Project Directory Structure](#project-directory-structure)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [1. Start Redis](#1-start-redis)
  - [2. Configure Environment Variables](#2-configure-environment-variables)
  - [3. Install Dependencies & Build](#3-install-dependencies--build)
  - [4. Running the Service](#4-running-the-service)
- [API Reference & cURL Examples](#api-reference--curl-examples)
  - [1. Ingest Dual-Channel Notification](#1-ingest-dual-channel-notification-email--push)
  - [2. Distributed Idempotency Verification](#2-distributed-idempotency-verification)
  - [3. User Preference & Opt-Out Filtering](#3-user-preference--opt-out-filtering)
  - [4. Queue & Circuit Breaker Diagnostics Endpoint](#4-queue--circuit-breaker-diagnostics-endpoint)
  - [5. Health Check Endpoint](#5-health-check-endpoint)
- [Resiliency & Error Handling](#resiliency--error-handling)
- [License](#license)

---

## Architecture Overview

```mermaid
flowchart TD
    Client["Client / Microservices"] -->|POST /v1/notifications| API["Express API Server"]
    API --> Auth["Auth Middleware (API Key / JWT)"]
    Auth --> Val["Zod Payload Validation"]
    Val --> Idem["Idempotency Service (Redis SET NX EX)"]
    Idem --> Pref["User Preferences Service (Redis Cache)"]
    Pref --> Router["Notification Router / Dispatcher"]
    
    subgraph Queues ["Dedicated BullMQ Queues"]
        ECQ[("email_critical")]
        EBQ[("email_bulk")]
        PCQ[("push_critical")]
        PBQ[("push_bulk")]
        EDLQ[("email_dlq")]
        PDLQ[("push_dlq")]
    end

    Router -->|Email OTP/Security| ECQ
    Router -->|Email Promo/Newsletter| EBQ
    Router -->|Push Security/Alert| PCQ
    Router -->|Push Update/Marketing| PBQ

    subgraph EmailWorkers ["Email Worker Cluster"]
        EW["Email Processor"] --> ES["Email Strategy"]
        ES --> ERL{"Redis Rate Limit"}
        ERL -->|Allowed| ECB{"Resend Circuit Breaker"}
        ECB -->|CLOSED / HALF-OPEN| Resend["Resend SDK (Primary)"]
        Resend -->|Error / Timeout / Breaker OPEN| SCB{"SMTP Circuit Breaker"}
        SCB -->|CLOSED / HALF-OPEN| SMTP["Nodemailer SMTP (Fallback)"]
        SCB -->|Max Retries Exceeded| EDLQ
    end

    subgraph PushWorkers ["Push Worker Cluster"]
        PW["Push Processor"] --> PS["Push Strategy"]
        PS --> PRL{"Redis Rate Limit"}
        PRL -->|Allowed| FCB{"FCM Circuit Breaker"}
        FCB -->|CLOSED / HALF-OPEN| FCM["Firebase Admin FCM"]
        FCB -->|Max Retries Exceeded| PDLQ
    end

    ECQ --> EW
    EBQ --> EW
    PCQ --> PW
    PBQ --> PW
```

---

## Design Patterns Implemented

### 1. Strategy Design Pattern
The **Strategy Pattern** is used to encapsulate channel-specific delivery mechanisms, rate limiting, and vendor dispatching behaviors behind a common interface (`INotificationChannelStrategy`).

#### Motivation
- Decouples queue consumers and workers from the underlying provider APIs.
- Enables adding new channels (e.g. `SMS`, `WHATSAPP`, `SLACK`, `WEBHOOK`) by creating a new Strategy class without modifying existing worker or controller logic (**Open/Closed Principle**).
- Strategies encapsulate their own rate limits, circuit breakers, and multi-provider failover chains.

#### Class Structure
```mermaid
classDiagram
    class INotificationChannelStrategy {
        <<interface>>
        +channel: NotificationChannel
        +process(jobData: ChannelJobData): Promise
    }

    class EmailNotificationStrategy {
        +channel: NotificationChannel
        +resendCircuitBreaker: CircuitBreaker
        +smtpCircuitBreaker: CircuitBreaker
        +process(jobData: EmailJobData): Promise
    }

    class PushNotificationStrategy {
        +channel: NotificationChannel
        +fcmCircuitBreaker: CircuitBreaker
        +process(jobData: PushJobData): Promise
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

#### Key Characteristics in this Service:
- **Resend Circuit Breaker**: If Resend throws 3 consecutive 5xx errors or network timeouts, the breaker trips to `OPEN`. Subsequent email requests **instantly divert to Nodemailer SMTP fallback** without wasting 10–30 seconds waiting for Resend to time out!
- **SMTP Circuit Breaker**: Guards fallback SMTP against server connection bans.
- **Firebase FCM Breaker**: Prevents hammering FCM when Google Cloud endpoints are unreachable.
- **Live Diagnostics**: Circuit states and failure counts are exposed in real-time via `GET /metrics`.

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

## Tech Stack

| Technology | Purpose |
| :--- | :--- |
| **Node.js & TypeScript** | Strict-typed asynchronous runtime environment |
| **Express.js** | Non-blocking HTTP ingestion server |
| **BullMQ** | Distributed queue engine backed by Redis Streams |
| **IORedis** | High-performance Redis client with Lua script support |
| **Zod** | Runtime schema validation with conditional cross-field validation |
| **Resend SDK** | Primary transactional email service |
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
        ├── email.worker.ts         # Dedicated Email BullMQ worker
        ├── push.worker.ts          # Dedicated Push BullMQ worker
        └── worker-runner.ts        # Standalone worker cluster runner
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

#### Mode B: Decoupled Worker Clusters
Ideal for horizontally scalable production architectures:
```bash
# Terminal 1: Ingestion API Nodes (Zero worker overhead)
WORKERS_EMBEDDED=false npm run dev

# Terminal 2: Dedicated Worker Cluster Nodes
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
          "path": "https://s3.amazonaws.com/my-bucket/invoices/inv_8899.pdf?AWSAccessKeyId=...&Signature=..."
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

## Resiliency & Error Handling

1. **Vendor Rate-Limiting**: Every strategy checks a Redis sliding window counter before dispatching. If the limit is reached, jobs are smoothly delayed using BullMQ `job.moveToDelayed()` without dropping packets.
2. **Provider Failover**: If Resend fails, the `EmailNotificationStrategy` captures the failure, checks the `Nodemailer_SMTP_Breaker`, and immediately delivers through Nodemailer SMTP fallback.
3. **Exponential Backoff**: BullMQ retries critical jobs 3 times with exponential backoff (1s, 2s, 4s).
4. **Dead Letter Queue (DLQ)**: If all retries and fallback providers fail, the job is routed to `email_dlq` or `push_dlq` with detailed error telemetry and failure timestamps for manual replay or operational inspection.

---

## License
MIT
