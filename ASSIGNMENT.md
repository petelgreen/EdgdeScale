# Senior Backend Engineer Home Assignment
## Large Scale Event Processor — EdgeScale

---

## 1. Project Overview

**EdgeScale** manages millions of edge devices globally. Devices send:
- High-frequency telemetry
- Text for NLP analysis
- Large binary files

**Goal:** Design and develop a high-scale ingestion and processing pipeline that remains resilient under heavy load.

**Architecture:** Microservices — an **Ingestion Service** receives gRPC calls from agents and offloads work to a **Message Broker** for asynchronous processing by **Worker Services**.

---

## 2. Technical Stack Requirements

| Concern | Technology |
|---|---|
| Protocol | gRPC over HTTP/2 |
| Serialization | Protocol Buffers (Protobuf) |
| Language | Python or any backend-optimized language |
| Messaging | Kafka, RabbitMQ, Redis Streams, etc. |

---

## 3. Functional Requirements

### Service A — Ingestion Service

| RPC Method | gRPC Pattern | Behavior |
|---|---|---|
| `Heartbeat()` | Fire-and-Forget (Unary) | Accept heartbeat, no response needed |
| `AnalyzeText(text)` | Req/Resp over Async Broker | Assign unique ID, publish to broker, wait for result, return `word_count` |
| `UploadAndAnalyzeFile()` | Client-to-Server Streaming → Async | Receive 1–10MB file in chunks, stream chunks to broker, wait for result, return `word_count` |

### Service B — Worker Pool

| Worker | Consumes From | Behavior |
|---|---|---|
| **Text Worker** | Text Tasks Topic | Calculate word count, publish `{ID, word_count}` to Text Results Topic |
| **File Worker** | File Tasks Topic | Consume chunks, calculate running word count, detect EOS, publish final `{ID, word_count}` to File Results Topic |

---

## 4. Message Broker — Topics

```
Text Tasks Topic   →  [Text Worker]   →  Text Results Topic
File Tasks Topic   →  [File Worker]   →  File Results Topic
```

**Request-Response over Async Broker:**
1. Service A assigns a **unique correlation ID** to each request
2. Publishes task (with ID) to the relevant task topic
3. Subscribes to the results topic
4. **Waits** for the result message matching the correlation ID
5. Returns the result to the gRPC client (holding the connection open throughout)

---

## 5. Architectural Expectations

### Concurrency
- Service A must handle many simultaneous gRPC calls
- Each call waits on its own async result without blocking others

### Backpressure
- When the broker/buffer is full, Service A must respond with a gRPC error:
  - `RESOURCE_EXHAUSTED` (rate limit) or `UNAVAILABLE`
- Prevents cascade failures under heavy load

### Efficiency
- File chunks must be **streamed directly** to the broker
- No full file buffering in memory (supports 1–10MB payloads)

### Observability
- Structured logging throughout both services
- Logs must support request tracing (correlation ID, timestamps, service name, status)

---

## 6. Deliverables

- [ ] Clean, documented source code repository
- [ ] `README.md` with setup and run instructions
- [ ] System diagram + written explanation of the Request-Response over Async Broker pattern
- [ ] Test script simulating concurrent agents, including resilience/stress testing

### Bonus
- [ ] `docker-compose.yml` / `Makefile` / script for **single-command deploy**

---

## 7. System Flow Summary

```
Edge Device (Agent)
        │
        │  gRPC over HTTP/2 + Protobuf
        ▼
┌───────────────────┐
│  Service A        │  ← Ingestion Service
│  (gRPC Server)    │
│                   │
│  Heartbeat()      │──► accept & drop (fire-and-forget)
│                   │
│  AnalyzeText()    │──► publish {ID, text} to Text Tasks Topic
│      │ waits      │◄── consume {ID, word_count} from Text Results Topic
│      └─────────── │──► return word_count to client
│                   │
│  UploadFile()     │──► stream chunks {ID, chunk} to File Tasks Topic
│      │ waits      │◄── consume {ID, word_count} from File Results Topic
│      └─────────── │──► return word_count to client
└───────────────────┘
        │
        │  Kafka / RabbitMQ / Redis Streams
        ▼
┌───────────────────────────────┐
│        Message Broker         │
│                               │
│  Text Tasks Topic             │
│  Text Results Topic           │
│  File Tasks Topic             │
│  File Results Topic           │
└───────────────────────────────┘
        │
        ▼
┌───────────────────┐
│  Service B        │  ← Worker Pool
│                   │
│  Text Worker      │──► word count → Text Results Topic
│  File Worker      │──► running word count → File Results Topic (on EOS)
└───────────────────┘
```

---

## 8. Key Design Challenge

The **Request-Response over Async Broker** pattern is the core complexity:

- The gRPC call is **synchronous from the client's perspective** — it blocks waiting for `word_count`
- Under the hood, Service A uses a **non-blocking async wait** (e.g., `asyncio.Event`, `Future`, or a pending-requests map keyed by correlation ID)
- The result consumer loop resolves the waiting request when the matching ID arrives
- This must work correctly under **high concurrency** with many in-flight requests simultaneously
