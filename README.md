# Production-Grade Multi-Channel Notification Engine (Email, Push & SMS)

A high-performance, non-blocking, and resilient multi-channel notification engine built with **Node.js, TypeScript, Express, Redis, BullMQ, and Zod**.

---

## Table of Contents
- [Architecture Overview & High-Level Design (HLD)](#architecture-overview--high-level-design-hld)
  - [1. ASCII System Architecture Overview](#1-ascii-system-architecture-overview)
  - [2. Visual High-Level Design (HLD) Architecture Diagram (Mermaid)](#2-visual-high-level-design-hld-architecture-diagram-mermaid)
  - [3. End-to-End Sequence & Request Lifecycle Diagram](#3-end-to-end-sequence--request-lifecycle-diagram)
- [Application Ecosystem & Portfolio Integrations](#application-ecosystem--portfolio-integrations)
  - [Portfolio Consumer Applications Matrix](#portfolio-consumer-applications-matrix)
  - [Dynamic Credential Isolation Model](#dynamic-credential-isolation-model)
  - [Integration Client Pattern](#integration-client-pattern)
- [Deep-Dive Component Explanations](#deep-dive-component-explanations)
  - [1. API Ingestion Gateway & Multi-Tenant Authentication](#1-api-ingestion-gateway--multi-tenant-authentication)
  - [2. Distributed Idempotency Engine](#2-distributed-idempotency-engine)
  - [3. User Preference & Channel Opt-Out Engine](#3-user-preference--channel-opt-out-engine)
  - [4. Notification Router & Queue Dispatcher](#4-notification-router--queue-dispatcher)
  - [5. Priority & Channel-Isolated Queue Topology (9 BullMQ Queues)](#5-priority--channel-isolated-queue-topology-9-bullmq-queues)
  - [6. Autonomous Worker Fleet & Cluster Runner (6 Worker Clusters)](#6-autonomous-worker-fleet--cluster-runner-6-worker-clusters)
  - [7. Channel Strategy Layer](#7-channel-strategy-layer)
  - [8. Distributed Sliding-Window Rate Limiter](#8-distributed-sliding-window-rate-limiter)
  - [9. Multi-Tier Resilient Circuit Breakers (5 Breakers)](#9-multi-tier-resilient-circuit-breakers-5-breakers)
  - [10. Downstream Provider Adapters & Multi-Provider Failover](#10-downstream-provider-adapters--multi-provider-failover)
  - [11. Dead Letter Queue (DLQ) & Poison Pill Isolation](#11-dead-letter-queue-dlq--poison-pill-isolation)
  - [12. Real-Time Observability, Diagnostics & Health Monitoring](#12-real-time-observability-diagnostics--health-monitoring)
- [Design Patterns Implemented](#design-patterns-implemented)
  - [1. Strategy Design Pattern](#1-strategy-design-pattern)
  - [2. Circuit Breaker Design Pattern](#2-circuit-breaker-design-pattern)
  - [3. Producer-Consumer & Priority Queue Isolation Pattern](#3-producer-consumer--priority-queue-isolation-pattern)
  - [4. Distributed Idempotent Receiver Pattern](#4-distributed-idempotent-receiver-pattern)
- [Dedicated Worker Segregation & Workload Isolation](#dedicated-worker-segregation--workload-isolation)
- [Tech Stack](#tech-stack)
- [Project Directory Structure](#project-directory-structure)
- [Environment Configuration & Variables Matrix](#environment-configuration--variables-matrix)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [1. Start Redis](#1-start-redis)
  - [2. Configure Environment Variables](#2-configure-environment-variables)
  - [3. Install Dependencies, Validate & Build](#3-install-dependencies-validate--build)
  - [4. Running the Service](#4-running-the-service)
- [API Reference & cURL Examples](#api-reference--curl-examples)
  - [1. Ingest Multi-Channel Notification (Email + Push)](#1-ingest-multi-channel-notification-email--push)
  - [2. Ingest SMS Notification](#2-ingest-sms-notification)
  - [3. Distributed Idempotency Verification](#3-distributed-idempotency-verification)
  - [4. User Preference & Opt-Out Filtering](#4-user-preference--opt-out-filtering)
  - [5. Queue & Circuit Breaker Diagnostics Endpoint](#5-queue--circuit-breaker-diagnostics-endpoint)
  - [6. Health Check Endpoint](#6-health-check-endpoint)
- [Resiliency & Error Handling Matrix](#resiliency--error-handling-matrix)
- [Testing & Quality Assurance](#testing--quality-assurance)
- [License](#license)

---

## Architecture Overview & High-Level Design (HLD)

### 1. ASCII System Architecture Overview

```text
=============================================================================================================================
                                             HIGH-LEVEL DESIGN (HLD) ARCHITECTURE
=============================================================================================================================

 [ 1. Upstream AI Portfolio Applications & Client Tier ]
 +-----------------------+   +---------------------+   +---------------------+   +------------------------------------------+
 |  AI Travel Planner    |   |  AI Movie Matcher   |   |  AI Sports Tracker  |   | 10 Other Portfolio AI Services           |
 |  (Itineraries & Trips)|   |  (Recommendations)  |   |  (Live Match Alerts)|   | (Study Spot, Hiking, Pipeline Ops, etc.) |
 +-----------+-----------+   +----------+----------+   +----------+----------+   +--------------------+---------------------+
             |                          |                         |                               |
             +--------------------------+-------------------------+-------------------------------+
                                        | (HTTPS POST /v1/notifications with x-api-key)
                                        v
 [ 2. API Ingestion Gateway Tier ]
 +--------------------------------------------------------------------------------------------------------------------------+
 |  Express.js API Server                                                                                                   |
 |  ├── Security: Helmet (Secure HTTP Headers), CORS Filter, Express JSON Body Parser                                       |
 |  ├── Authentication: Multi-Tenant Key Discovery (`NOTIFICATION_<SERVICE>_API_KEY`, `API_KEY`, RFC 6750 Bearer JWT)       |
 |  ├── Context Enrichment: Resolves calling client (`req.user = { apiKeyUsed: true, role: 'service', serviceName }`)      |
 |  ├── Schema Validation: Zod Cross-Field Validation (Payload conditional requirements for EMAIL, PUSH, SMS)                |
 |  └── Ingestion Controller: UUID Generation (`notif_<timestamp>_<randomHex>`), Idempotency Guard, Preference Filtering     |
 +-------+------------------------------------------+---------------------------------------------------+-------------------+
         |                                          |                                                   |
         | 1. Check & Acquire Lock                  | 2. Query User Opt-Outs                            | 3. Enqueue Jobs
         v                                          v                                                   v
 [ 3. Distributed State Layer - Redis / IORedis ]                           [ 4. Queue Isolation Tier - BullMQ ]
 +------------------------------------+  +-------------------------------+  +-----------------------------------------------+
 | Distributed Idempotency Engine     |  | Preference & Opt-Out Cache    |  | Priority & Channel Segregated Queues (9 Total)|
 | ├── Key: `idempotency:<key>`       |  | ├── Key: `user:preferences:<id|  | ├── Email: `email_critical`, `email_bulk`     |
 | ├── Command: SET NX EX 300 (5 min) |  | ├── Default TTL: 3600 seconds |  | ├── Push:  `push_critical`, `push_bulk`       |
 | └── States: PENDING / PROCESSED    |  | └── Priority Bypass:          |  | ├── SMS:   `sms_critical`, `sms_bulk`         |
 +------------------------------------+  |     CRITICAL bypasses bulk opt|  | └── DLQs:  `email_dlq`, `push_dlq`, `sms_dlq` |
                                         +-------------------------------+  +-----------------------+-----------------------+
                                                                                                    |
                                                                                                    | Dequeued by
                                                                                                    v
 [ 5. Dedicated Autonomous Worker Fleet Tier ]
 +--------------------------------------------------------------------------------------------------------------------------+
 |  Independent Scalable Worker Clusters (Decoupled process pools & zero cross-queue contention)                            |
 |  ├── EmailCriticalWorker (Concurrency: 25 | Priority: Real-time  | Backoff: Exponential 1s, 2s, 4s | Attempts: 3)         |
 |  ├── EmailBulkWorker     (Concurrency: 5  | Limiter: 10/1000ms   | Backoff: Exponential 3s, 6s, 12s| Attempts: 3)         |
 |  ├── PushCriticalWorker  (Concurrency: 30 | Priority: Real-time  | Backoff: Exponential 1s, 2s, 4s | Attempts: 3)         |
 |  ├── PushBulkWorker      (Concurrency: 10 | Limiter: 20/1000ms   | Backoff: Exponential 3s, 6s, 12s| Attempts: 3)         |
 |  ├── SmsCriticalWorker   (Concurrency: 25 | Priority: Real-time  | Backoff: Exponential 1s, 2s, 4s | Attempts: 3)         |
 |  └── SmsBulkWorker       (Concurrency: 5  | Limiter: 10/1000ms   | Backoff: Exponential 3s, 6s, 12s| Attempts: 3)         |
 +-------+----------------------------------------------------------------------------------------------+-------------------+
         |                                                                                              |
         v (Executes Channel Strategy)                                                                  v
 [ 6. Channel Strategy & Resiliency Engine ]
 +--------------------------------------------------------------------------------------------------------------------------+
 |  Notification Strategy Registry (`StrategyRegistry`)                                                                     |
 |                                                                                                                          |
 |  [ EMAIL STRATEGY ]                     [ PUSH STRATEGY ]                       [ SMS STRATEGY ]                         |
 |  ├── 1. Redis Sliding Window Rate Check ├── 1. Redis Sliding Window Rate Check   ├── 1. Redis Sliding Window Rate Check   |
 |  ├── 2. Resend API Circuit Breaker      ├── 2. Firebase FCM Circuit Breaker     ├── 2. httpSMS API Circuit Breaker       |
 |  │      - Threshold: 3 | Cooldown: 20s  │      - Threshold: 4 | Cooldown: 25s   │      - Threshold: 3 | Cooldown: 30s    |
 |  └── 3. Nodemailer SMTP Circuit Breaker └── 3. FCM Native Multicast / Unicast   └── 3. TextBee Gateway Circuit Breaker   |
 |         - Threshold: 3 | Cooldown: 30s                                                 - Threshold: 3 | Cooldown: 30s    |
 +-------+-----------------------------------+-------------------------------------+--------------------+-------------------+
         |                                   |                                     |                    |
         | (Primary Resend / Fallback SMTP)  | (FCM Dispatch)                      | (Primary httpSMS / | (Fatal Errors /
         v                                   v                                     v  Fallback TextBee) v  Retries Exhausted)
 [ 7. Downstream Third-Party Vendor Tier ]                                                [ 8. Dead Letter Queue Tier ]
 +--------------------+  +--------------------+  +-------------------+  +---------------+ +---------------------------------+
 | Primary Email:     |  | Fallback Email:    |  | Push Provider:    |  | SMS Providers:| | BullMQ Dead Letter Queues (DLQ) |
 | Resend REST API    |  | Nodemailer SMTP    |  | Firebase Admin    |  | 1. httpSMS API| | ├── `email_dlq`                 |
 | (HTML/Attachments) |  | (RFC 5322 Server)  |  | (Google Cloud FCM)|  | 2. TextBee GW | | ├── `push_dlq`                  |
 +--------------------+  +--------------------+  +-------------------+  +---------------+ | └── `sms_dlq`                   |
                                                                                          +---------------------------------+
=============================================================================================================================
```

---

### 2. Visual High-Level Design (HLD) Architecture Diagram (Mermaid)

```mermaid
flowchart TD
    %% Client Tier
    subgraph ClientTier ["1. AI Applications & Upstream Client Tier"]
        ClientTravel["AI Travel Planner"]
        ClientMovie["AI Movie Matcher"]
        ClientSports["AI Sports Tracker"]
        ClientStudy["AI Study Spot Finder"]
        ClientOthers["9 Other AI Portfolio Apps\n(Hiking, Pipeline Ops, PR Review, etc.)"]
    end

    %% Ingestion & API Gateway Tier
    subgraph IngestionTier ["2. Ingestion & API Gateway Tier"]
        ExpressApp["Express.js Server\n(Helmet, CORS, JSON Parser)"]
        AuthMid["Auth Middleware\n(Multi-App x-api-key / Bearer JWT)"]
        ZodVal["Zod Schema Validator\n(Cross-Field Conditional Validation)"]
        NotifController["Notification Ingestion Controller\n(UUID Generation & Route Dispatch)"]
    end

    %% Distributed State & Redis Store
    subgraph RedisTier ["3. Distributed State Layer (Redis / IORedis)"]
        IdemStore[("Idempotency Store\nSET NX EX 300\nKey: idempotency:<key>")]
        PrefStore[("User Preferences Cache\nKey: user:preferences:<userId>\nTTL: 3600s")]
        RateLimitStore[("Sliding Window Sorted Sets\nKey: ratelimit:<vendor>\nRedis Lua Script")]
        BullMQStreams[("BullMQ Stream Storage\nQueue Keys, Delayed Sets, Hashes")]
    end

    %% Routing & Queue Segregation Tier
    subgraph QueueTier ["4. Isolated Priority & Channel Queues (9 BullMQ Queues)"]
        ECQ[("email_critical\n(OTPs, 2FA, Receipts)")]
        EBQ[("email_bulk\n(Newsletters, Digests)")]
        PCQ[("push_critical\n(Security & Urgent Alerts)")]
        PBQ[("push_bulk\n(Marketing, Updates)")]
        SCQ[("sms_critical\n(Urgent OTPs, Security SMS)")]
        SBQ[("sms_bulk\n(Informational SMS, Updates)")]
        EDLQ[("email_dlq\n(Exhausted Retries)")]
        PDLQ[("push_dlq\n(Exhausted Retries)")]
        SDLQ[("sms_dlq\n(Exhausted Retries)")]
    end

    %% Autonomous Worker Processing Tier
    subgraph WorkerTier ["5. Dedicated Autonomous Worker Fleet"]
        ECW["Email Critical Worker\nConcurrency: 25 | Exponential Backoff\n(1s, 2s, 4s)"]
        EBW["Email Bulk Worker\nConcurrency: 5 | Limiter: 10/1s\n(3s, 6s, 12s)"]
        PCW["Push Critical Worker\nConcurrency: 30 | Exponential Backoff\n(1s, 2s, 4s)"]
        PBW["Push Bulk Worker\nConcurrency: 10 | Limiter: 20/1s\n(3s, 6s, 12s)"]
        SCW["SMS Critical Worker\nConcurrency: 25 | Exponential Backoff\n(1s, 2s, 4s)"]
        SBW["SMS Bulk Worker\nConcurrency: 5 | Limiter: 10/1s\n(3s, 6s, 12s)"]
    end

    %% Strategy & Resiliency Engine
    subgraph StrategyTier ["6. Channel Strategy & Resiliency Engine"]
        StratRegistry["Notification Strategy Registry"]
        
        subgraph EmailPipeline ["Email Strategy Pipeline"]
            EmailStrat["Email Strategy"]
            EmailRateLimit{"Sliding Window\nRate Check"}
            ResendBreaker{"Resend Breaker\n(Thresh: 3 | Cool: 20s)"}
            SmtpBreaker{"SMTP Breaker\n(Thresh: 3 | Cool: 30s)"}
        end

        subgraph PushPipeline ["Push Strategy Pipeline"]
            PushStrat["Push Strategy"]
            PushRateLimit{"Sliding Window\nRate Check"}
            FcmBreaker{"FCM Breaker\n(Thresh: 4 | Cool: 25s)"}
        end

        subgraph SmsPipeline ["SMS Strategy Pipeline"]
            SmsStrat["SMS Strategy"]
            SmsRateLimit{"Sliding Window\nRate Check"}
            HttpSmsBreaker{"httpSMS Breaker\n(Thresh: 3 | Cool: 30s)"}
            TextBeeBreaker{"TextBee Breaker\n(Thresh: 3 | Cool: 30s)"}
        end
    end

    %% Downstream Delivery Providers
    subgraph ProviderTier ["7. Downstream Delivery Providers"]
        ResendSDK["Resend API SDK\n(Primary Email)"]
        SmtpTransport["Nodemailer SMTP\n(Fallback Email)"]
        FirebaseAdmin["Firebase Admin SDK\n(FCM Push)"]
        HttpSmsSDK["httpSMS API\n(Primary SMS)"]
        TextBeeSDK["TextBee Gateway\n(Fallback SMS)"]
    end

    %% Observability & Diagnostics
    subgraph TelemetryTier ["8. Observability & Operational Diagnostics"]
        MetricsRoute["GET /metrics (Protected)\n(9 Queue Depths & 5 Breaker States)"]
        HealthRoute["GET /health (Public)\n(Express & Redis Ping)"]
        PinoLogger["Structured Pino JSON Logger"]
    end

    %% Connectors - Ingestion
    ClientTravel -->|POST /v1/notifications| ExpressApp
    ClientMovie -->|POST /v1/notifications| ExpressApp
    ClientSports -->|POST /v1/notifications| ExpressApp
    ClientStudy -->|POST /v1/notifications| ExpressApp
    ClientOthers -->|POST /v1/notifications| ExpressApp

    ExpressApp --> AuthMid
    AuthMid --> ZodVal
    ZodVal --> NotifController

    %% Connectors - Controller to State
    NotifController <-->|1. Check & Acquire Lock| IdemStore
    NotifController <-->|2. Fetch Opt-out Flags| PrefStore

    %% Connectors - Queue Ingestion
    NotifController -->|EMAIL + CRITICAL| ECQ
    NotifController -->|EMAIL + BULK| EBQ
    NotifController -->|PUSH + CRITICAL| PCQ
    NotifController -->|PUSH + BULK| PBQ
    NotifController -->|SMS + CRITICAL| SCQ
    NotifController -->|SMS + BULK| SBQ
    NotifController -.->|3. Mark Processed & Save Response| IdemStore

    %% Connectors - Worker Consumption
    ECQ --> ECW
    EBQ --> EBW
    PCQ --> PCW
    PBQ --> PBW
    SCQ --> SCW
    SBQ --> SBW

    %% Worker to Strategy
    ECW --> StratRegistry
    EBW --> StratRegistry
    PCW --> StratRegistry
    PBW --> StratRegistry
    SCW --> StratRegistry
    SBW --> StratRegistry

    StratRegistry --> EmailStrat
    StratRegistry --> PushStrat
    StratRegistry --> SmsStrat

    %% Email Strategy Dispatch
    EmailStrat <-->|Atomic Lua Eval| RateLimitStore
    EmailStrat --> EmailRateLimit
    EmailRateLimit -->|Allowed| ResendBreaker
    ResendBreaker -->|CLOSED / HALF-OPEN| ResendSDK
    ResendSDK -.->|Success| ResendBreaker
    ResendSDK -.->|Failure / Circuit OPEN| SmtpBreaker
    SmtpBreaker -->|CLOSED / HALF-OPEN| SmtpTransport
    SmtpTransport -.->|Terminal Failure| EDLQ

    %% Push Strategy Dispatch
    PushStrat <-->|Atomic Lua Eval| RateLimitStore
    PushStrat --> PushRateLimit
    PushRateLimit -->|Allowed| FcmBreaker
    FcmBreaker -->|CLOSED / HALF-OPEN| FirebaseAdmin
    FirebaseAdmin -.->|Terminal Failure| PDLQ

    %% SMS Strategy Dispatch
    SmsStrat <-->|Atomic Lua Eval| RateLimitStore
    SmsStrat --> SmsRateLimit
    SmsRateLimit -->|Allowed| HttpSmsBreaker
    HttpSmsBreaker -->|CLOSED / HALF-OPEN| HttpSmsSDK
    HttpSmsSDK -.->|Success| HttpSmsBreaker
    HttpSmsSDK -.->|Failure / Circuit OPEN| TextBeeBreaker
    TextBeeBreaker -->|CLOSED / HALF-OPEN| TextBeeSDK
    TextBeeSDK -.->|Terminal Failure| SDLQ

    %% Observability Wiring
    MetricsRoute -.-> BullMQStreams
    MetricsRoute -.-> StratRegistry
    HealthRoute -.-> ExpressApp
    HealthRoute -.-> RedisTier
```

---

### 3. End-to-End Sequence & Request Lifecycle Diagram

```mermaid
sequenceDiagram
    autonumber
    actor Client as AI Portfolio Application
    participant Gateway as Express Gateway & Auth
    participant Zod as Zod Schema Validator
    participant Idem as Redis Idempotency Service
    participant Pref as User Preference Cache
    participant Queues as BullMQ Redis Queues (9 Queues)
    participant Worker as Autonomous Worker Fleet (6 Workers)
    participant Limiter as Redis Lua Rate Limiter
    participant Breaker as Circuit Breaker Engine (5 Breakers)
    participant Primary as Primary Provider (Resend / FCM / httpSMS)
    participant Fallback as Fallback Provider (SMTP / TextBee)
    participant DLQ as BullMQ Dead Letter Queue

    %% Ingestion Phase
    Client->>Gateway: POST /v1/notifications (x-api-key: NOTIFICATION_<SERVICE>_API_KEY)
    Gateway->>Gateway: Validate API key & attach calling serviceName
    Gateway->>Zod: Validate Request Body Structure & Conditional Channel Constraints
    Zod-->>Gateway: Validation Passed

    Gateway->>Idem: checkAndAcquire(idempotencyKey, notificationId)
    alt Idempotency Key Exists (Duplicate Request)
        Idem-->>Gateway: isDuplicate = true (Cached Result)
        Gateway-->>Client: HTTP 202 Accepted (Cached Response Payload)
    else First Ingestion (Lock Acquired via SET NX EX 300)
        Idem-->>Gateway: isDuplicate = false
        
        loop For each channel in [EMAIL, PUSH, SMS]
            Gateway->>Pref: isChannelAllowed(userId, channel, priority)
            alt User Opted-Out
                Pref-->>Gateway: allowed = false (reason: opt-out)
                Gateway->>Gateway: Record in skippedChannels array
            else Allowed
                Pref-->>Gateway: allowed = true
                Gateway->>Queues: enqueue(channel_priority_queue, jobPayload)
                Queues-->>Gateway: Enqueued (queueName, jobId generated)
            end
        end

        Gateway->>Idem: markProcessed(idempotencyKey, notificationId, responsePayload)
        Gateway-->>Client: HTTP 200 OK (notificationId, enqueuedChannels, skippedChannels)
    end

    %% Asynchronous Worker Processing Phase
    Note over Queues,Worker: Asynchronous Consumer Decoupling
    Queues->>Worker: Dequeue Job (e.g., email_critical, push_critical, or sms_critical)
    Worker->>Limiter: checkRateLimit(VENDOR_KEY, max, windowMs)

    alt Rate Limit Exceeded
        Limiter-->>Worker: allowed = false (retryAfterMs)
        Worker->>Queues: Delay Job (job.moveToDelayed)
    else Rate Limit Permitted
        Limiter-->>Worker: allowed = true
        Worker->>Breaker: execute(primaryProvider.send)
        
        alt Primary Circuit Breaker is OPEN
            Breaker-->>Worker: Fast-Fail (Trip immediately to fallback)
            Worker->>Fallback: fallbackProvider.send(options)
            Fallback-->>Worker: Delivery Succeeded
        else Primary Circuit Breaker is CLOSED / HALF-OPEN
            Breaker->>Primary: primaryProvider.send(options)
            alt Primary Delivery Succeeds
                Primary-->>Breaker: Success
                Breaker-->>Worker: Ack Success
            else Primary Delivery Fails / Times out
                Primary-->>Breaker: Error (5xx / Socket Timeout)
                Breaker->>Breaker: Record Failure (Trip to OPEN if threshold reached)
                Breaker-->>Worker: Primary Failed
                Worker->>Fallback: fallbackProvider.send(options)
                alt Fallback Delivery Succeeds
                    Fallback-->>Worker: Success
                else Fallback Delivery Fails
                    Fallback-->>Worker: Fallback Error
                    Worker->>Worker: Retry with Exponential Backoff (3 attempts)
                    alt Max Retries Exhausted
                        Worker->>DLQ: Route to DLQ (email_dlq, push_dlq, sms_dlq) with errorStack
                    end
                end
            end
        end
    end
```

---

## Application Ecosystem & Portfolio Integrations

The notification service acts as the central, unified notification backbone for a comprehensive ecosystem of AI applications. Each application integrates with the notification backend using application-specific service API keys, allowing decoupled delivery, isolated failure domains, and clear operational telemetry.

### Portfolio Consumer Applications Matrix

The following table documents all 13 verified consumer applications actively configured to consume the notification service:

| Application Name | Public Production URL | Purpose / Core Functionality | Supported Notification Use Cases |
| :--- | :--- | :--- | :--- |
| **AI Travel Planner** | [travel-planner.rishankkesharwani.com/plan-trip](https://travel-planner.rishankkesharwani.com/plan-trip) | LLM-powered multi-city itinerary generation, budget scheduling, and attraction planning. | Trip generation confirmations, full itinerary PDFs/HTML delivery (`EMAIL`), departure reminders (`PUSH`). |
| **AI Movie Matcher** | [movie-matcher.rishankkesharwani.com](https://movie-matcher.rishankkesharwani.com/) | Semantic movie recommendation engine with real-time watchlist synchronization. | Weekly curated film digests (`EMAIL`), collaborative watchparty matching alerts (`PUSH`). |
| **AI Sports Tracker** | [sports-tracker.rishankkesharwani.com](https://sports-tracker.rishankkesharwani.com/) | Live game score tracker, sports telemetry analytics, and game momentum monitors. | Urgent breaking score alerts (`SMS`, `PUSH`), match summary recaps (`EMAIL`). |
| **AI Study Spot Finder** | [study-spot-finder.rishankkesharwani.com](https://study-spot-finder.rishankkesharwani.com/) | Intelligent geo-location recommender for quiet cafes, libraries, and workspaces. | Spot reservation confirmations (`EMAIL`), quiet study session reminders (`PUSH`). |
| **AI Hiking Explorer** | [hiking-explorer.rishankkesharwani.com](https://hiking-explorer.rishankkesharwani.com/) | AI-driven trail discovery, elevation profiling, and hike difficulty assessment. | Critical trail hazard & weather advisory alerts (`SMS`), route summaries (`PUSH`). |
| **AI Pipeline Observability** | [pipeline-observability.rishankkesharwani.com](https://pipeline-observability.rishankkesharwani.com/) | Telemetry monitoring, trace analysis, and failure anomaly detection for ML data pipelines. | Pipeline stall & SLA breach alerts (`EMAIL`, `PUSH`), daily pipeline health digests (`EMAIL`). |
| **AI Model Regression Detection**| [model-regression.rishankkesharwani.com](https://model-regression.rishankkesharwani.com/) | Continuous benchmark evaluation detecting latency, drift, and accuracy regressions in LLMs. | Critical regression threshold breach alerts (`EMAIL`, `PUSH`), weekly model stability reports (`EMAIL`). |
| **AI PR Review Platform** | [ai-pr-review.rishankkesharwani.com/reviews](https://ai-pr-review.rishankkesharwani.com/reviews) | Automated pull request code review, security vulnerability detection, and diff analysis. | Code review completion notifications (`EMAIL`), high-severity CVE alerts (`PUSH`). |
| **AI Cost Routing Autopilot** | [ai-cost-routing-autopilot.rishankkesharwani.com](https://ai-cost-routing-autopilot.rishankkesharwani.com/) | Dynamic routing of LLM inferences across providers to optimize token cost and latency. | Critical spend ceiling & budget exhaustion alerts (`EMAIL`, `SMS`), failover warnings (`PUSH`). |
| **AI Document Intelligence** | [document-intelligence.rishankkesharwani.com](https://document-intelligence.rishankkesharwani.com/) | Multi-modal OCR parsing, structural data extraction, and entity recognition from documents. | Batch OCR completion notifications (`EMAIL`), document parsing failure triage (`EMAIL`). |
| **AI Output Arbitration** | [ai-output-arbitration.rishankkesharwani.com](https://ai-output-arbitration.rishankkesharwani.com/) | Multi-model consensus scoring, hallucination arbitration, and LLM output evaluation. | Judge consensus failure alerts (`EMAIL`), arbitration anomaly notices (`PUSH`). |
| **AI RAG Quality Ops** | [ai-rag-quality-ops.rishankkesharwani.com](https://ai-rag-quality-ops.rishankkesharwani.com/) | Evaluation of retrieval-augmented generation pipelines for context relevance and groundedness. | Retrieval failure spikes (`EMAIL`), hallucination drift notifications (`PUSH`). |
| **AI Prompt Experiment Manager** | [prompt-experiment.rishankkesharwani.com](https://prompt-experiment.rishankkesharwani.com/) | Systematic A/B testing, versioning, and benchmarking of prompt variations against datasets. | Benchmark experiment batch completion alerts (`EMAIL`), winner evaluation summaries (`EMAIL`). |

### Dynamic Credential Isolation Model

To guarantee strict isolation between portfolio applications, the service implements **Dynamic Service API Key Discovery** in [`src/middlewares/auth.middleware.ts`](src/middlewares/auth.middleware.ts):

1. **Automatic Pattern Discovery**: Any environment variable following the naming convention `NOTIFICATION_<SERVICE>_API_KEY` (e.g. `NOTIFICATION_TRAVEL_API_KEY`, `NOTIFICATION_PR_REVIEW_API_KEY`, `NOTIFICATION_RESUME_API_KEY`) is automatically indexed at runtime.
2. **Identity Attachment**: When an application authenticates using `x-api-key: <key>` or `Authorization: ApiKey <key>`, the middleware resolves the matching service name and attaches it to the request context:
   ```typescript
   req.user = {
     apiKeyUsed: true,
     role: 'service',
     serviceName: 'travel', // e.g. 'travel', 'movie', 'sports', 'pipeline', etc.
   };
   ```
3. **No Code Changes for New Consumers**: Adding a new portfolio application requires only setting `NOTIFICATION_<NEWAPP>_API_KEY=secret_key` in the environment. The service immediately accepts the key and tracks its identity without server recompilation or code updates.
4. **Migration Fallback**: The legacy `API_KEY` variable is maintained as a fallback mapped to service name `'legacy'`.

### Integration Client Pattern

Upstream applications call the notification service asynchronously over HTTPS. Below is the standard integration client pattern used across the portfolio:

```typescript
// Shared notification dispatch helper in portfolio client
async function dispatchNotification(payload: {
  idempotencyKey: string;
  priority: 'CRITICAL' | 'BULK';
  channels: ('EMAIL' | 'PUSH' | 'SMS')[];
  recipient: { userId: string; email?: string; pushToken?: string; phone?: string };
  email?: { subject: string; html?: string; text?: string };
  push?: { title: string; body: string };
  sms?: { message: string; to?: string };
}) {
  const response = await fetch(`${process.env.NOTIFICATION_SERVICE_URL}/v1/notifications`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-api-key': process.env.NOTIFICATION_SERVICE_API_KEY!,
    },
    body: JSON.stringify(payload),
  });

  if (!response.ok && response.status !== 202) {
    throw new Error(`Notification dispatch failed with HTTP ${response.status}`);
  }

  return await response.json();
}
```

---

## Deep-Dive Component Explanations

### 1. API Ingestion Gateway & Multi-Tenant Authentication
- **Source Files**: [`src/app.ts`](src/app.ts), [`src/middlewares/auth.middleware.ts`](src/middlewares/auth.middleware.ts), [`src/middlewares/validate.middleware.ts`](src/middlewares/validate.middleware.ts), [`src/types/zod-schemas.ts`](src/types/zod-schemas.ts)
- **Role & Purpose**: Unified entry point for all upstream applications. Enforces security headers, rate checks, multi-tenant credential isolation, and cross-field request validation.
- **Internal Mechanics**:
  - **Security Shield**: Configured with `helmet` for secure HTTP headers, `cors` for cross-origin management, and express body parsing with size limits.
  - **Multi-Tenant Authentication**:
    - `x-api-key: <key>`: Primary service-to-service header. Matches dynamically discovered `NOTIFICATION_<SERVICE>_API_KEY` or fallback `API_KEY`.
    - `Authorization: ApiKey <key>`: RFC-compatible service key alternative.
    - `Authorization: Bearer <JWT>`: Validates standard JSON Web Tokens against `JWT_SECRET`.
  - **Zod Runtime Schema Validation**:
    - Validates payload conditional rules before touching controllers or queues:
      - If `EMAIL` is requested, `recipient.email` and `email` object (`subject`, `html` or `text`) are required.
      - If `PUSH` is requested, `recipient.pushToken` and `push` object (`title`, `body`) are required.
      - If `SMS` is requested, a phone number (`recipient.phone` or `sms.to`) and `sms.message` are required.
  - **Security Boundaries**: Protected endpoints (`/metrics`, `POST /v1/notifications`, `GET/PUT /v1/preferences/:userId`) require authentication. The `/health` liveness probe remains intentionally public.

---

### 2. Distributed Idempotency Engine
- **Source File**: [`src/services/idempotency.service.ts`](src/services/idempotency.service.ts)
- **Role & Purpose**: Provides distributed at-least-once deduplication semantics, preventing duplicate emails, SMS, or pushes caused by client network retries or message replay storms.
- **Internal Mechanics**:
  - **Atomic Acquisition (`checkAndAcquire`)**: Uses Redis `SET idempotency:<key> <payload> NX EX 300` (5-minute TTL).
  - **Two-Phase Lifecycle**:
    1. `PENDING`: Initial lock acquired. Prevents concurrent requests with the identical key from enqueueing duplicate jobs.
    2. `PROCESSED`: Once jobs are enqueued in BullMQ, the response payload is written back to Redis with the remaining TTL.
  - **Duplicate Handling**: If a duplicate key arrives within the 300-second window, the service immediately returns **HTTP 202 Accepted** with the cached job IDs, avoiding redundant queueing.

---

### 3. User Preference & Channel Opt-Out Engine
- **Source File**: [`src/services/preference.service.ts`](src/services/preference.service.ts)
- **Role & Purpose**: Evaluates recipient channel-level opt-outs and bulk marketing preferences prior to queue dispatch.
- **Internal Mechanics**:
  - **Cache Lookup**: Queries Redis key `user:preferences:<userId>` with a 3600-second (1 hour) TTL. Defaults to un-opted state (`false`) on cache miss and caches default preferences.
  - **Filtering Rules**:
    - `emailOptOut: true` $\rightarrow$ Drops `EMAIL` channel.
    - `pushOptOut: true` $\rightarrow$ Drops `PUSH` channel.
    - `smsOptOut: true` $\rightarrow$ Drops `SMS` channel.
    - `bulkOptOut: true` $\rightarrow$ Drops messages with `priority == 'BULK'`, but **never blocks** `CRITICAL` priority messages (e.g. OTPs, security alerts, fraud warnings).
  - **Skipped Channel Feedback**: Any dropped channels are returned in the `skippedChannels` array with human-readable diagnostic reasons (e.g., `"User has opted out of all SMS notifications"`).

---

### 4. Notification Router & Queue Dispatcher
- **Source Files**: [`src/controllers/notification.controller.ts`](src/controllers/notification.controller.ts), [`src/queues/queue.registry.ts`](src/queues/queue.registry.ts)
- **Role & Purpose**: Unpacks validated multi-channel notification requests, creates unique notification IDs (`notif_<timestamp>_<randomHex>`), and routes each channel job to its dedicated priority queue.
- **Internal Mechanics**:
  - **Channel Decoupling**: A single incoming request requesting `["EMAIL", "PUSH", "SMS"]` generates three independent queue jobs, ensuring that a slow email provider never impedes SMS or push delivery.
  - **Priority Queue Resolution**:
    - `EMAIL` + `CRITICAL` $\rightarrow$ `email_critical`
    - `EMAIL` + `BULK` $\rightarrow$ `email_bulk`
    - `PUSH` + `CRITICAL` $\rightarrow$ `push_critical`
    - `PUSH` + `BULK` $\rightarrow$ `push_bulk`
    - `SMS` + `CRITICAL` $\rightarrow$ `sms_critical`
    - `SMS` + `BULK` $\rightarrow$ `sms_bulk`

---

### 5. Priority & Channel-Isolated Queue Topology (9 BullMQ Queues)
- **Source File**: [`src/queues/queue.registry.ts`](src/queues/queue.registry.ts)
- **Role & Purpose**: Implements queue isolation across 3 delivery channels and 2 priority tiers, preventing marketing blasts from causing priority inversion for transactional messages.
- **Queue Inventory**:

| Queue Name | Channel | Priority Tier | Max Attempts | Backoff Strategy | DLQ Target |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `email_critical` | Email | CRITICAL | 3 | Exponential (1s, 2s, 4s) | `email_dlq` |
| `email_bulk` | Email | BULK | 3 | Exponential (3s, 6s, 12s)| `email_dlq` |
| `email_dlq` | Email | Dead Letter | N/A | Storage only / Operational triage | N/A |
| `push_critical` | Push | CRITICAL | 3 | Exponential (1s, 2s, 4s) | `push_dlq` |
| `push_bulk` | Push | BULK | 3 | Exponential (3s, 6s, 12s)| `push_dlq` |
| `push_dlq` | Push | Dead Letter | N/A | Storage only / Operational triage | N/A |
| `sms_critical` | SMS | CRITICAL | 3 | Exponential (1s, 2s, 4s) | `sms_dlq` |
| `sms_bulk` | SMS | BULK | 3 | Exponential (3s, 6s, 12s)| `sms_dlq` |
| `sms_dlq` | SMS | Dead Letter | N/A | Storage only / Operational triage | N/A |

---

### 6. Autonomous Worker Fleet & Cluster Runner (6 Worker Clusters)
- **Source Files**: [`src/workers/`](src/workers/), [`src/workers/worker-runner.ts`](src/workers/worker-runner.ts)
- **Role & Purpose**: Dedicated worker processes that consume jobs from specific BullMQ queues, invoke channel strategies, and handle retry/dead-letter mechanics.
- **Worker Specifications**:
  - **`EmailCriticalWorker`**: Concurrency: 25. High-throughput for transactional emails.
  - **`EmailBulkWorker`**: Concurrency: 5. Rate-paced with BullMQ rate limiter: `10 jobs / 1000ms`.
  - **`PushCriticalWorker`**: Concurrency: 30. High-concurrency for immediate push dispatches.
  - **`PushBulkWorker`**: Concurrency: 10. Rate-paced with BullMQ rate limiter: `20 jobs / 1000ms`.
  - **`SmsCriticalWorker`**: Concurrency: 25. Real-time processing for urgent 2FA and verification SMS.
  - **`SmsBulkWorker`**: Concurrency: 5. Rate-paced with BullMQ rate limiter: `10 jobs / 1000ms`.
- **Standalone Runner Support**: `src/workers/worker-runner.ts` supports running all workers or targeting individual workers using `WORKER_TYPE=<name>` (e.g. `WORKER_TYPE=sms_critical npm run start:workers`), enabling horizontal autoscaling per queue.

---

### 7. Channel Strategy Layer
- **Source Files**: [`src/patterns/strategy/`](src/patterns/strategy/)
- **Role & Purpose**: Implements the **Strategy Design Pattern** via `INotificationChannelStrategy<T>`, standardizing channel execution and encapsulating rate checks, primary dispatch, and fallback failover.
- **Registered Strategies**:
  - **`EmailNotificationStrategy`**: Sliding window rate check $\rightarrow$ Resend API (Primary) $\rightarrow$ Nodemailer SMTP (Fallback).
  - **`PushNotificationStrategy`**: Sliding window rate check $\rightarrow$ Firebase Admin FCM.
  - **`SmsNotificationStrategy`**: Sliding window rate check $\rightarrow$ httpSMS (Primary) $\rightarrow$ TextBee Gateway (Fallback).

---

### 8. Distributed Sliding-Window Rate Limiter
- **Source File**: [`src/services/rate-limiter.service.ts`](src/services/rate-limiter.service.ts)
- **Role & Purpose**: Enforces sliding-window rate limits across all distributed worker nodes to comply with vendor quotas (`EMAIL_VENDOR`, `PUSH_VENDOR`, `SMS_VENDOR`).
- **Algorithmic Implementation**:
  - Implemented via an atomic Redis Lua script operating on Redis Sorted Sets (`ZREMRANGEBYSCORE`, `ZCARD`, `ZADD`, `PEXPIRE`).
  - When a rate limit is exceeded, the limiter calculates `retryAfterMs` based on the oldest element in the window.
  - The worker delays the job using BullMQ's `job.moveToDelayed(Date.now() + retryAfterMs)`, providing smooth backpressure without dropping messages.

---

### 9. Multi-Tier Resilient Circuit Breakers (5 Breakers)
- **Source Files**: [`src/patterns/circuit-breaker/circuit-breaker.ts`](src/patterns/circuit-breaker/circuit-breaker.ts), [`src/patterns/strategy/strategy.registry.ts`](src/patterns/strategy/strategy.registry.ts)
- **Role & Purpose**: Protects workers from thread starvation and cascading timeouts when third-party provider APIs experience latency or server outages.
- **Circuit Breaker Specifications**:

| Breaker Name | Protected Provider | Failure Threshold | Cooldown Duration (`recoveryTimeoutMs`) | Success Threshold (Half-Open Probe) |
| :--- | :--- | :--- | :--- | :--- |
| `Resend_API_Breaker` | Resend REST API | 3 failures | 20,000 ms (20s) | 2 consecutive successes |
| `Nodemailer_SMTP_Breaker` | Nodemailer SMTP | 3 failures | 30,000 ms (30s) | 2 consecutive successes |
| `Firebase_FCM_Breaker` | Firebase Admin FCM | 4 failures | 25,000 ms (25s) | 2 consecutive successes |
| `HttpSMS_Breaker` | httpSMS API Gateway | 3 failures | 30,000 ms (30s) | 2 consecutive successes |
| `TextBee_Breaker` | TextBee Device Gateway | 3 failures | 30,000 ms (30s) | 2 consecutive successes |

---

### 10. Downstream Provider Adapters & Multi-Provider Failover
- **Source Files**: [`src/providers/`](src/providers/)
- **Email Delivery Chain**:
  1. Primary: **Resend REST API SDK** (`ResendEmailProvider`). High deliverability with HTML and attachment support.
  2. Fallback: **Nodemailer SMTP** (`SmtpEmailProvider`). Activated automatically if Resend fails or `Resend_API_Breaker` is `OPEN`.
- **Push Delivery**:
  - **Firebase Cloud Messaging** (`FcmPushProvider`). Uses Google Service Account credentials to dispatch unicast or multicast notifications.
- **SMS Delivery Chain**:
  1. Primary: **httpSMS** (`HttpSmsProvider`). Calls `https://api.httpsms.com/v1/messages/send` via Android SIM Gateway & Firebase FCM.
  2. Fallback: **TextBee** (`TextBeeSmsProvider`). Calls `https://api.textbee.dev/api/v1/gateway/devices/{deviceId}/send-sms`. Activated if httpSMS fails or `HttpSms_Breaker` is `OPEN`.

---

### 11. Dead Letter Queue (DLQ) & Poison Pill Isolation
- **Source Files**: [`src/queues/queue.registry.ts`](src/queues/queue.registry.ts), [`src/workers/`](src/workers/)
- **Role & Purpose**: Isolates permanently un-deliverable jobs after all 3 retries and fallback providers have failed.
- **Internal Mechanics**:
  - Terminally failed jobs are moved to `email_dlq`, `push_dlq`, or `sms_dlq`.
  - Retains full diagnostics: original payload, `failedAt` timestamp, `attemptsMade`, `lastProviderAttempted`, and full `errorStack`.
  - Quarantines poison pills (e.g. invalid recipient numbers, malformed MIME types) so they do not block subsequent queue messages.

---

### 12. Real-Time Observability, Diagnostics & Health Monitoring
- **Source Files**: [`src/controllers/notification.controller.ts`](src/controllers/notification.controller.ts), [`src/config/logger.ts`](src/config/logger.ts), [`src/routes/notification.routes.ts`](src/routes/notification.routes.ts)
- **Monitoring Endpoints**:
  - **`GET /health` (Public)**: Checks API server status and executes an active `redisClient.ping()`. Returns HTTP 200 `UP` when healthy or HTTP 503 `DEGRADED` (`redis: DISCONNECTED`) if Redis is unavailable.
  - **`GET /metrics` (Protected)**: Returns live queue metrics across all 9 queues (`waiting`, `active`, `completed`, `failed`, `delayed`) and diagnostics for all 5 circuit breakers (`state`, `failureCount`, `successCount`, `nextAttemptAt`).
  - **Structured Pino JSON Logging**: Emits machine-readable logs with `notificationId`, `serviceName`, `channel`, `priority`, `durationMs`, and `idempotencyKey` for centralized log ingestion.

---

## Design Patterns Implemented

### 1. Strategy Design Pattern
Decouples channel delivery execution from worker loops. Each notification channel implements a uniform contract.

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

    class SmsNotificationStrategy {
        +channel: NotificationChannel
        +httpSmsCircuitBreaker: CircuitBreaker
        +textBeeCircuitBreaker: CircuitBreaker
        +process(jobData: SmsJobData): Promise~ProviderResponse~
    }

    class NotificationStrategyRegistry {
        -strategies: Map
        +registerStrategy(strategy)
        +getStrategy(channel)
        +getEmailStrategy()
        +getPushStrategy()
        +getSmsStrategy()
        +getAllCircuitBreakerDiagnostics()
    }

    INotificationChannelStrategy <|.. EmailNotificationStrategy
    INotificationChannelStrategy <|.. PushNotificationStrategy
    INotificationChannelStrategy <|.. SmsNotificationStrategy
    NotificationStrategyRegistry o-- INotificationChannelStrategy
```

---

### 2. Circuit Breaker Design Pattern
Protects the service against cascading failures and thread starvation during downstream provider outages.

#### State Machine

```mermaid
stateDiagram-v2
    [*] --> CLOSED: Initial State
    
    CLOSED --> OPEN: Failure threshold reached (e.g. 3 consecutive errors)
    note right of CLOSED
      Normal operation.
      Requests pass to provider.
      Failures increment counter.
    end note
    
    OPEN --> HALF_OPEN: Cooldown timer expires (20s - 30s)
    note right of OPEN
      Fast-fail immediately.
      Bypasses failed provider.
      Routes directly to fallback.
    end note
    
    HALF_OPEN --> CLOSED: Probe request succeeds (target met)
    HALF_OPEN --> OPEN: Probe request fails
    note right of HALF_OPEN
      Allows single probe request
      to test provider recovery.
    end note
```

---

### 3. Producer-Consumer & Priority Queue Isolation Pattern
Prevents priority inversion where large-volume promotional blasts block critical authentication or security alerts:
- **Critical Queues** (`email_critical`, `push_critical`, `sms_critical`): Processed with high concurrency (25-30 workers) and rapid exponential backoff.
- **Bulk Queues** (`email_bulk`, `push_bulk`, `sms_bulk`): Processed with controlled concurrency (5-10 workers) and rate-limit backpressure.
- **Dead Letter Queues** (`email_dlq`, `push_dlq`, `sms_dlq`): Quarantines terminally failed messages.

---

### 4. Distributed Idempotent Receiver Pattern
- Implemented via Redis `SET idempotency:<key> <payload> NX EX 300`.
- Protects downstream notification providers from duplicate requests caused by network timeouts or client retries.

---

## Dedicated Worker Segregation & Workload Isolation

> [!NOTE]
> **Why 6 Independent Worker Clusters?**
> A common flaw in notification engines is a single shared worker pool. If 50,000 bulk marketing emails are enqueued, all worker threads and Redis connections would be occupied, causing critical 2FA OTPs or security alerts to queue behind promotional traffic.
> 
> In this architecture:
> 1. **`EmailCriticalWorker`**: Dedicated process pool (concurrency: 25). Prioritizes transactional receipts, password resets, and verification emails.
> 2. **`EmailBulkWorker`**: Rate-paced pool (concurrency: 5, limiter: 10/1s). Prevents vendor rate bans during bulk marketing campaigns.
> 3. **`PushCriticalWorker`**: Dedicated process pool (concurrency: 30). Prioritizes immediate security and transaction alerts.
> 4. **`PushBulkWorker`**: Rate-paced pool (concurrency: 10, limiter: 20/1s). Dispatches informational app updates.
> 5. **`SmsCriticalWorker`**: Dedicated process pool (concurrency: 25). Prioritizes 2FA codes and urgent emergency alerts.
> 6. **`SmsBulkWorker`**: Rate-paced pool (concurrency: 5, limiter: 10/1s). Dispatches informational SMS notifications.

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
| **httpSMS API** | Primary SMS delivery gateway (Android SIM + FCM) |
| **TextBee API** | Fallback SMS delivery gateway |
| **Pino & Pino-Pretty** | Low-overhead structured JSON logging |
| **Helmet & CORS** | HTTP security headers and cross-origin protection |

---

## Project Directory Structure

```
.
├── docker-compose.yml              # Redis service container definition
├── package.json                    # Dependencies, scripts, and build tasks
├── tsconfig.json                   # Strict TypeScript compiler options
├── .env.example                    # Environment variable template
├── .gitignore                      # Git exclusion rules
├── tests
│   └── auth-and-redis.test.ts      # Multi-app auth & Redis connection test suite
└── src
    ├── app.ts                      # Express application setup
    ├── server.ts                   # Main server entry point (API + optional embedded workers)
    ├── config
    │   ├── env.ts                  # Zod-validated environment config
    │   ├── logger.ts               # Structured Pino logger
    │   └── redis.ts                # IORedis connection pools & factory
    ├── controllers
    │   └── notification.controller.ts # Ingestion, metrics & preference controllers
    ├── middlewares
    │   ├── auth.middleware.ts      # Multi-app API key discovery & JWT auth
    │   ├── error.middleware.ts     # Centralized error handler
    │   └── validate.middleware.ts  # Zod schema validation middleware
    ├── patterns
    │   ├── circuit-breaker
    │   │   └── circuit-breaker.ts  # CLOSED, OPEN, HALF_OPEN Circuit Breaker
    │   └── strategy
    │       ├── email.strategy.ts   # Email strategy with dual circuit breakers
    │       ├── push.strategy.ts    # Push strategy with FCM circuit breaker
    │       ├── sms.strategy.ts     # SMS strategy with dual circuit breakers
    │       ├── notification-strategy.interface.ts # Strategy contract
    │       └── strategy.registry.ts # Strategy Registry & circuit breaker aggregator
    ├── providers
    │   ├── email
    │   │   ├── email.interface.ts  # IEmailProvider contract
    │   │   ├── email-fallback.service.ts # Primary-Fallback orchestrator
    │   │   ├── resend.provider.ts  # Resend SDK provider
    │   │   └── smtp.provider.ts    # Nodemailer SMTP fallback provider
    │   ├── push
    │   │   ├── push.interface.ts   # IPushProvider contract
    │   │   └── fcm.provider.ts     # Firebase FCM provider
    │   └── sms
    │       ├── sms.interface.ts    # ISmsProvider contract
    │       ├── sms-fallback.service.ts # httpSMS to TextBee failover service
    │       ├── httpsms.provider.ts # httpSMS primary provider
    │       └── textbee.provider.ts # TextBee fallback provider
    ├── queues
    │   └── queue.registry.ts       # BullMQ definitions for 9 dedicated queues
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
        ├── email-critical.worker.ts # Dedicated Email Critical BullMQ worker
        ├── email-bulk.worker.ts     # Dedicated Email Bulk BullMQ worker
        ├── push-critical.worker.ts  # Dedicated Push Critical BullMQ worker
        ├── push-bulk.worker.ts      # Dedicated Push Bulk BullMQ worker
        ├── sms-critical.worker.ts   # Dedicated SMS Critical BullMQ worker
        ├── sms-bulk.worker.ts       # Dedicated SMS Bulk BullMQ worker
        ├── index.ts                 # Worker registry exports
        └── worker-runner.ts         # Multi-target standalone worker cluster runner
```

---

## Environment Configuration & Variables Matrix

Configuration is validated on startup via Zod in [`src/config/env.ts`](src/config/env.ts).

| Variable Name | Required | Default | Description |
| :--- | :--- | :--- | :--- |
| `PORT` | Optional | `3000` | HTTP server port. |
| `NODE_ENV` | Optional | `development` | Environment mode (`development`, `production`, `test`). |
| `JWT_SECRET` | Optional | Development key | Secret key used for verifying Bearer JWT tokens. |
| `NOTIFICATION_TRAVEL_API_KEY` | Production* | `undefined` | Service key for AI Travel Planner. |
| `NOTIFICATION_MOVIE_API_KEY` | Production* | `undefined` | Service key for AI Movie Matcher. |
| `NOTIFICATION_SPORTS_API_KEY` | Production* | `undefined` | Service key for AI Sports Tracker. |
| `NOTIFICATION_STUDY_API_KEY` | Production* | `undefined` | Service key for AI Study Spot Finder. |
| `NOTIFICATION_<SERVICE>_API_KEY` | Optional | `undefined` | Dynamically discovered key for any portfolio service. |
| `API_KEY` | Optional | `test-api-key-12345` | Fallback legacy API key during migration. |
| `REDIS_URL` | Recommended | `undefined` | Canonical Redis connection URL (e.g. `redis://user:pass@host:port/0`). |
| `REDIS_HOST` | Fallback | `localhost` | Redis host when `REDIS_URL` is omitted. |
| `REDIS_PORT` | Fallback | `6379` | Redis port when `REDIS_URL` is omitted. |
| `REDIS_PASSWORD` | Fallback | `undefined` | Redis password when `REDIS_URL` is omitted. |
| `RESEND_API_KEY` | Optional | `""` | Primary email API key for Resend. |
| `EMAIL_FROM` | Optional | `onboarding@resend.dev` | Default outbound email sender address. |
| `SMTP_HOST` | Optional | `smtp.gmail.com` | Fallback SMTP server hostname. |
| `SMTP_PORT` | Optional | `587` | Fallback SMTP port. |
| `SMTP_USER` | Optional | `""` | Fallback SMTP authentication username. |
| `SMTP_PASS` | Optional | `""` | Fallback SMTP authentication password. |
| `SMTP_FROM` | Optional | `""` | Outbound sender name/address for SMTP. |
| `FCM_PROJECT_ID` | Optional | `""` | Firebase Cloud Messaging project identifier. |
| `FCM_CLIENT_EMAIL` | Optional | `""` | Google Service Account client email for FCM. |
| `FCM_PRIVATE_KEY` | Optional | `""` | Google Service Account private key for FCM. |
| `HTTPSMS_API_KEY` | Optional | `""` | Primary SMS API key for httpSMS. |
| `HTTPSMS_FROM_NUMBER` | Optional | `""` | Sender phone number for httpSMS. |
| `TEXTBEE_API_KEY` | Optional | `""` | Fallback SMS API key for TextBee. |
| `TEXTBEE_DEVICE_ID` | Optional | `""` | Device identifier for TextBee gateway. |
| `EMAIL_RATE_LIMIT_MAX` | Optional | `10` | Max email requests per rate window. |
| `EMAIL_RATE_LIMIT_WINDOW_MS` | Optional | `1000` | Email sliding rate window in milliseconds. |
| `PUSH_RATE_LIMIT_MAX` | Optional | `50` | Max push requests per rate window. |
| `PUSH_RATE_LIMIT_WINDOW_MS` | Optional | `1000` | Push sliding rate window in milliseconds. |
| `SMS_RATE_LIMIT_MAX` | Optional | `20` | Max SMS requests per rate window. |
| `SMS_RATE_LIMIT_WINDOW_MS` | Optional | `1000` | SMS sliding rate window in milliseconds. |
| `IDEMPOTENCY_TTL_SECONDS` | Optional | `300` | Redis TTL for idempotency locks (5 minutes). |

*\*In production, at least one application service key (`NOTIFICATION_<SERVICE>_API_KEY`) or a non-default `API_KEY` is required.*

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

### 3. Install Dependencies, Validate & Build
```bash
# Install dependencies
npm install

# Run static type checking
npm run typecheck

# Run test suite
npm test

# Compile TypeScript to dist/
npm run build
```

### 4. Running the Service

#### Mode A: Unified Server (API + Embedded Workers)
Ideal for local development and single-container deployments:
```bash
npm run dev        # Development mode with hot-reload (ts-node-dev)
npm start          # Production mode (compiled JS from dist/)
```

#### Mode B: Decoupled Independent Worker Clusters
Ideal for horizontally scalable production architectures:

```bash
# Ingestion API Nodes (workers disabled)
WORKERS_EMBEDDED=false npm run dev

# Run all 6 worker clusters in a dedicated process:
npm run dev:workers        # Development mode
npm run start:workers      # Production mode

# Or run individual dedicated worker nodes:
npm run worker:email:critical   # Dedicated Email Critical Worker
npm run worker:email:bulk       # Dedicated Email Bulk Worker
npm run worker:push:critical    # Dedicated Push Critical Worker
npm run worker:push:bulk        # Dedicated Push Bulk Worker
npm run worker:sms:critical     # Dedicated SMS Critical Worker
npm run worker:sms:bulk         # Dedicated SMS Bulk Worker
```

---

## API Reference & cURL Examples

All protected endpoints require either:
- Header `x-api-key: replace-with-your-api-key`
- Header `Authorization: ApiKey replace-with-your-api-key`
- Header `Authorization: Bearer <JWT_TOKEN>`

### 1. Ingest Multi-Channel Notification (Email + Push)
`POST /v1/notifications`

```bash
curl -X POST http://localhost:3000/v1/notifications \
  -H "Content-Type: application/json" \
  -H "x-api-key: replace-with-your-api-key" \
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
      "html": "<h2>Payment Received!</h2><p>Your travel itinerary is confirmed.</p>",
      "attachments": [
        {
          "filename": "itinerary_8899.pdf",
          "path": "https://example.com/docs/itinerary_8899.pdf"
        }
      ]
    },
    "push": {
      "title": "Booking Confirmed",
      "body": "Your itinerary is ready for viewing."
    },
    "metadata": {
      "bookingId": "8899"
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

### 2. Ingest SMS Notification
`POST /v1/notifications`

```bash
curl -X POST http://localhost:3000/v1/notifications \
  -H "Content-Type: application/json" \
  -H "x-api-key: replace-with-your-api-key" \
  -d '{
    "idempotencyKey": "sms_auth_otp_9921",
    "priority": "CRITICAL",
    "channels": ["SMS"],
    "recipient": {
      "userId": "usr_beta_2",
      "phone": "+1234567890"
    },
    "sms": {
      "message": "Your verification code is 492019. Expires in 5 minutes."
    }
  }'
```

**Response (HTTP 200 OK):**
```json
{
  "success": true,
  "message": "Notification enqueued successfully",
  "notificationId": "notif_1725267920456_8c1f",
  "idempotencyStatus": "PROCESSED",
  "enqueuedChannels": [
    {
      "channel": "SMS",
      "queue": "sms_critical",
      "jobId": "send_sms_notif_1725267920456_8c1f"
    }
  ]
}
```

---

### 3. Distributed Idempotency Verification
Re-submitting the identical request with `idempotencyKey: "sms_auth_otp_9921"` returns **HTTP 202 Accepted** immediately with the cached payload without re-enqueuing:

```json
{
  "success": true,
  "message": "Notification enqueued successfully",
  "notificationId": "notif_1725267920456_8c1f",
  "idempotencyStatus": "PROCESSED",
  "enqueuedChannels": [
    {
      "channel": "SMS",
      "queue": "sms_critical",
      "jobId": "send_sms_notif_1725267920456_8c1f"
    }
  ]
}
```

---

### 4. User Preference & Opt-Out Filtering

#### Set User Preferences
`PUT /v1/preferences/:userId`

```bash
curl -X PUT http://localhost:3000/v1/preferences/usr_alpha_1 \
  -H "Content-Type: application/json" \
  -H "x-api-key: replace-with-your-api-key" \
  -d '{
    "bulkOptOut": true,
    "smsOptOut": true,
    "emailOptOut": false
  }'
```

#### Fetch User Preferences
`GET /v1/preferences/:userId`

```bash
curl http://localhost:3000/v1/preferences/usr_alpha_1 \
  -H "x-api-key: replace-with-your-api-key"
```

---

### 5. Queue & Circuit Breaker Diagnostics Endpoint
`GET /metrics` (Protected)

Provides real-time visibility into all 9 queue depths and live diagnostics for all 5 circuit breakers:

```bash
curl http://localhost:3000/metrics \
  -H "x-api-key: replace-with-your-api-key"
```

**Sample Response:**
```json
{
  "success": true,
  "timestamp": "2026-10-22T09:35:00.000Z",
  "queues": {
    "email_critical": { "waiting": 0, "active": 0, "completed": 24, "failed": 0, "delayed": 0 },
    "email_bulk":     { "waiting": 0, "active": 0, "completed": 105, "failed": 0, "delayed": 0 },
    "email_dlq":      { "waiting": 0, "active": 0, "completed": 0, "failed": 0, "delayed": 0 },
    "push_critical":  { "waiting": 0, "active": 0, "completed": 18, "failed": 0, "delayed": 0 },
    "push_bulk":      { "waiting": 0, "active": 0, "completed": 50, "failed": 0, "delayed": 0 },
    "push_dlq":       { "waiting": 0, "active": 0, "completed": 0, "failed": 0, "delayed": 0 },
    "sms_critical":   { "waiting": 0, "active": 0, "completed": 12, "failed": 0, "delayed": 0 },
    "sms_bulk":       { "waiting": 0, "active": 0, "completed": 35, "failed": 0, "delayed": 0 },
    "sms_dlq":        { "waiting": 0, "active": 0, "completed": 0, "failed": 0, "delayed": 0 }
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
    },
    {
      "name": "HttpSMS_Breaker",
      "state": "CLOSED",
      "failureCount": 0,
      "successCount": 0,
      "failureThreshold": 3,
      "recoveryTimeoutMs": 30000,
      "nextAttemptAt": null
    },
    {
      "name": "TextBee_Breaker",
      "state": "CLOSED",
      "failureCount": 0,
      "successCount": 0,
      "failureThreshold": 3,
      "recoveryTimeoutMs": 30000,
      "nextAttemptAt": null
    }
  ]
}
```

---

### 6. Health Check Endpoint
`GET /health` (Public)

```bash
curl http://localhost:3000/health
```

**Response (HTTP 200 OK when healthy):**
```json
{
  "status": "UP",
  "timestamp": "2026-10-22T09:35:00.000Z",
  "services": {
    "api": "UP",
    "redis": "HEALTHY"
  }
}
```

*(Note: Returns HTTP 503 with `"status": "DEGRADED"` and `"redis": "DISCONNECTED"` if Redis connectivity is interrupted.)*

---

## Resiliency & Error Handling Matrix

| Failure Scenario | Component Handling | Recovery / Mitigation Mechanism |
| :--- | :--- | :--- |
| **Client sends rapid duplicate requests** | `IdempotencyService` | Atomic Redis `SET NX EX 300` returns cached HTTP 202 without re-queueing. |
| **User opted out of SMS notifications** | `UserPreferenceService` | Evaluates opt-out cache and drops `SMS` channel; valid sibling channels still process. |
| **User opted out of marketing campaigns** | `UserPreferenceService` | Skips `BULK` priority notifications; allows all `CRITICAL` priority notifications. |
| **Downstream vendor quota reached** | `VendorRateLimiterService` | Sliding-window Lua script delays the job via BullMQ `job.moveToDelayed()` without message drop. |
| **Primary Email (Resend) 5xx / Outage** | `Resend_API_Breaker` | Breaker trips to `OPEN` after 3 consecutive errors; immediately fails over to Nodemailer SMTP. |
| **Primary SMS (httpSMS) Outage** | `HttpSMS_Breaker` | Breaker trips to `OPEN` after 3 consecutive errors; immediately fails over to TextBee Gateway. |
| **Both Primary and Fallback providers fail** | Worker Retry Loop | Retries job 3 times using channel exponential backoff. |
| **Max retries exhausted (Poison Pill)** | Worker `onFailed` Hook | Moves job to `email_dlq`, `push_dlq`, or `sms_dlq` with full stack trace and metadata. |
| **Redis connection blip** | `IORedis` Reconnect | Auto-reconnect with exponential backoff; `/health` returns HTTP 503 until restored. |

---

## Testing & Quality Assurance

The repository includes test suites and static analysis tools to verify authentication, dynamic key discovery, and Redis configuration:

```bash
# Run test suite (Node.js test runner via ts-node/register)
npm test

# Run strict TypeScript compilation check (no emit)
npm run typecheck

# Clean and compile project
npm run build
```

The test suite in [`tests/auth-and-redis.test.ts`](tests/auth-and-redis.test.ts) exercises 16 test cases, verifying:
- Individual application API key validation (`travel`, `movie`, `sports`, `study`).
- Dynamic key discovery (`NOTIFICATION_<SERVICE>_API_KEY`) without code changes.
- Unauthorized rejection (HTTP 401) without secret credential leakage.
- Public availability of `/health` alongside protected boundaries on `/metrics` and `/v1/notifications`.
- `REDIS_URL` canonical precedence and fallback host/port/password resolution.

---

## License
MIT
