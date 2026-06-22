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

**Why stateless workers for file uploads?** The previous design had each worker accumulate chunk state in memory (`_file_state`). With multiple workers, RabbitMQ distributes chunks round-robin, so no single worker ever sees the full file. The fix moves aggregation to Service A: each chunk message carries a `chunk_index`; any worker processes it independently and returns the chunk word count; Service A sums chunk results by `correlation_id` and resolves the request when all `total_chunks` results are in. Different file uploads are still processed by different workers in parallel.

**Word count accuracy:** chunks are split at word boundaries by carrying any partial word at the end of each chunk into the next one (`split_at_word_boundary` + `leftover` in `service_a/handlers/file.py`). Words that span a raw gRPC chunk boundary are not double-counted.

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
