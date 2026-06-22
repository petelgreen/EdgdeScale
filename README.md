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

File uploads are chunked. Each chunk is a separate message; any worker can process any chunk. Service A collects the per-chunk word counts and sums them when all chunks are accounted for.

```
Device → Service A → [file_tasks] (one msg per chunk) → Service B (any worker)
                  ←  [service_a_<id>_file_results] (one result per chunk) ←
```

### Queue layout

| Queue | Type | Who reads it |
|-------|------|-------------|
| `text_tasks` | shared, durable | all Service B workers |
| `file_tasks` | shared, durable | all Service B workers |
| `service_a_<instance_id>_text_results` | private, exclusive, auto-delete | the one Service A instance that created it |
| `service_a_<instance_id>_file_results` | private, exclusive, auto-delete | the one Service A instance that created it |

**Why per-instance result queues?** When multiple Service A instances run, RabbitMQ would round-robin results across all consumers of a shared result queue. A result could land on the wrong instance — the one with no `Future` waiting for that `correlation_id` — and the original request would time out. Each instance generates a UUID at startup, creates its own exclusive result queues, and stamps every task with `reply_to`. Workers send results straight back to the originating instance.

**Word count accuracy:** chunks are split at word boundaries by carrying any partial word at the end of each chunk into the next one (`split_at_word_boundary` + `leftover` in `service_a/handlers/file.py`). Words that span a raw gRPC chunk boundary are not double-counted.

## Design decision: stateless Service B for file uploads

The assignment describes a File Worker that accumulates the running word count and signals one final result. This branch deliberately deviates from that description. The `stateful-file-worker` branch implements it literally if you want to compare.

The reason: **stateful workers and standard RabbitMQ queues are incompatible at scale.** With a shared queue and multiple workers, RabbitMQ distributes messages round-robin. No single worker ever sees all chunks of one file, so no single worker can keep an accurate running total. Making workers stateful forces you to also solve sticky routing — which means adding a consistent-hash exchange plugin, partition queues, and a partition index env var on every worker instance. That is a significant amount of infrastructure complexity added purely to satisfy a design choice that doesn't survive horizontal scaling.

This design instead keeps workers completely stateless and moves aggregation into Service A, where it naturally belongs:

- **Service A owns the request context.** It generated the `correlation_id`, it holds the waiting `Future`, it knows the timeout. Aggregating results there requires no new infrastructure — just an in-memory dict keyed by `correlation_id` that is cleaned up when the request completes.
- **Any worker can process any chunk.** No sticky routing, no partitions, no plugins. Adding a worker instance immediately increases throughput without any reconfiguration.
- **No state to lose on a worker crash.** A stateful worker that dies mid-file takes the running word count with it and the upload times out. Stateless workers restart cleanly — unprocessed chunks stay in RabbitMQ and are redelivered to the next available worker.
- **No stale-state cleanup needed.** Stateful workers need a TTL sweep to purge incomplete file state from crashed or timed-out uploads. Stateless workers have nothing to clean up.
- **Out-of-order chunk results are handled for free.** The aggregator in Service A tracks received chunk indices in a set and resolves the Future when all are accounted for — regardless of arrival order. A stateful worker relying on in-order delivery would need the same logic anyway.

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
