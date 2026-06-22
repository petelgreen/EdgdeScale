# EdgeScale

Processes text and files sent from devices over gRPC, using RabbitMQ to hand the work off to a background worker.

## What's in here

- **Service A** — gRPC server. Talks to devices, forwards jobs to the queue.
- **Service B** — background worker. Picks up jobs, counts words, sends results back.
- **RabbitMQ** — the broker in the middle.
- **Telemetry** — gRPC log collector. Both services send structured logs to it; it writes them to a persistent file with a traceID so you can follow a request across services.
- **lib/** — shared code: `config.py` (env vars), `consts.py` (queue names/timeouts), `telemetry_client.py` (log sender).

## How the async part works

gRPC is synchronous (the device waits for a reply), but the actual work happens in a separate process.

1. A request comes in. Service A generates a unique ID for it and stashes a `Future` (kind of a placeholder for the answer).
2. It drops the job on a RabbitMQ queue with that ID attached, then waits.
3. Service B picks it up, does the work, and sends the result back to a reply queue — with the same ID.
4. Service A sees the result, matches the ID to the waiting `Future`, fills it in, and the gRPC response goes back to the device.

The device sees a normal request/response, and it actually went through a queue and back.

```
Device → Service A → [text_tasks queue] → Service B
                  ←  [service_a_<id>_text_results] ←
```

File uploads are chunked. Service A streams each chunk through a consistent-hash exchange so that all chunks of one file land on the same partition queue, consumed by the same worker. Service B accumulates the running word count and sends one final result when the end-of-stream marker arrives.

```
Device → Service A → [file_tasks_hash_exchange] (routing_key=correlation_id)
                           ↓ consistent hash
               [file_tasks_0] [file_tasks_1] [file_tasks_2] [file_tasks_3]
                    ↓               ↓               ↓               ↓
               worker_0        worker_1        worker_2        worker_3
                    └─── one final result ──→ [service_a_<id>_file_results] ──→ Service A
```

### Queue layout

| Queue / Exchange | Type | Purpose |
|-----------------|------|---------|
| `text_tasks` | queue, durable | all Service B workers compete for text jobs |
| `file_tasks_hash_exchange` | x-consistent-hash exchange | routes file chunks by `correlation_id` |
| `file_tasks_0` … `file_tasks_3` | queues, durable | one worker per partition — ensures all chunks of one file reach the same worker |
| `service_a_<instance_id>_text_results` | private, exclusive, auto-delete | text results back to the originating Service A instance |
| `service_a_<instance_id>_file_results` | private, exclusive, auto-delete | single final file result back to the originating Service A instance |

**Why consistent-hash exchange for files?** Service B keeps a running word count in memory keyed by `correlation_id`. For that to work, every chunk of the same upload must reach the same worker process. Using `correlation_id` as the routing key on a consistent-hash exchange guarantees this — RabbitMQ hashes the key and always routes it to the same partition queue, which has exactly one consumer.

**Why per-instance result queues?** Multiple Service A instances would each receive results from RabbitMQ round-robin if they shared one result queue. A result could land on the wrong instance — the one with no `Future` waiting for that `correlation_id`. Each instance generates a UUID at startup and stamps every task message with `reply_to`, so workers send results directly back to the right instance.

**Word count accuracy:** Service A splits chunks at word boundaries before publishing (`split_at_word_boundary` + `leftover`). Words that span a raw gRPC chunk boundary are carried into the next published message, so they are never double-counted. The final end-of-stream message flushes any remaining text.

## File structure

```
EdgeScale/
├── proto/
│   ├── edgescale.proto        # service A contract
│   └── telemetry.proto        # telemetry contract
├── service_a/
│   ├── server.py              # gRPC server entry point
│   └── handlers/
│       ├── text.py            # text analysis
│       ├── file.py            # file upload (chunking + aggregation)
│       ├── errors.py          # error handling + gRPC abort helpers
│       └── common.py          # shared helpers
├── service_b/
│   └── worker.py              # RabbitMQ consumer, word counting
├── telemetry/
│   └── server.py              # log collector, writes to file
├── lib/
│   ├── config.py              # env vars
│   ├── consts.py              # queue names, timeouts
│   └── telemetry_client.py    # log sender
├── test/
│   └── test_client.py         # manual smoke test
├── edgescale_pb2.py           # generated
├── edgescale_pb2_grpc.py      # generated
├── telemetry_pb2.py           # generated
├── telemetry_pb2_grpc.py      # generated
└── docker-compose.yml
```

## Run it

```bash
docker compose up --build
```

- gRPC endpoint: `localhost:50051`
- RabbitMQ UI: `localhost:15672` (guest / guest)

## Test it

```bash
pip install -r test/requirements.txt
PYTHONPATH=. PYTHONUTF8=1 python test/test_client.py
```

Sends a heartbeat, some text, a 1 MB file, and 20 concurrent requests.

## Stop it

```bash
docker compose down
```
