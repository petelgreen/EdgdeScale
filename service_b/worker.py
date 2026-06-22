import asyncio
import os
import time

import aio_pika

from lib.config import RABBITMQ_URL, FILE_WORKER_PARTITION_INDEX
from lib.consts import TEXT_TASKS_QUEUE, FILE_TASKS_QUEUE_PREFIX, FILE_STATE_TTL
import lib.telemetry_client as tel

SERVICE = "service_b"

PREFETCH_COUNT = int(os.getenv("TEXT_WORKER_CONCURRENCY", "10"))

# running word count state keyed by correlation_id — lives only in this worker process
_file_state: dict[str, dict] = {}


async def handle_text(message: aio_pika.IncomingMessage, exchange: aio_pika.Exchange) -> None:
    cid = message.correlation_id
    word_count = len(message.body.decode("utf-8", errors="ignore").split())
    tel.log(SERVICE, cid, "text_worker", detail=f"word_count={word_count}")
    await exchange.publish(
        aio_pika.Message(body=word_count.to_bytes(4, "big"), correlation_id=cid),
        routing_key=message.reply_to,
    )
    await message.ack()


async def handle_file_chunk(message: aio_pika.IncomingMessage, exchange: aio_pika.Exchange) -> None:
    cid = message.correlation_id
    chunk_index = message.headers.get("chunk_index", 0)
    is_last = message.headers.get("is_last", 0)

    if cid not in _file_state:
        _file_state[cid] = {"wc": 0, "created_at": time.monotonic()}
        tel.log(SERVICE, cid, "file_stream", "start", detail=f"partition={FILE_WORKER_PARTITION_INDEX}")

    if not is_last:
        chunk_wc = len(message.body.decode("utf-8", errors="ignore").split())
        _file_state[cid]["wc"] += chunk_wc
        tel.log(SERVICE, cid, "chunk_processed",
                detail=f"chunk_index={chunk_index} chunk_wc={chunk_wc} running_wc={_file_state[cid]['wc']}")
    else:
        # EOS — count any leftover text in the body, then publish final result
        if message.body:
            leftover_wc = len(message.body.decode("utf-8", errors="ignore").split())
            _file_state[cid]["wc"] += leftover_wc
        final_wc = _file_state.pop(cid)["wc"]
        tel.log(SERVICE, cid, "file_complete", detail=f"final_word_count={final_wc}")
        await exchange.publish(
            aio_pika.Message(body=final_wc.to_bytes(4, "big"), correlation_id=cid),
            routing_key=message.reply_to,
        )
        tel.log(SERVICE, cid, "file_result_published", detail=f"word_count={final_wc}")

    await message.ack()


async def _periodic_cleanup(interval: float = 30.0) -> None:
    while True:
        await asyncio.sleep(interval)
        now = time.monotonic()
        stale = [cid for cid, s in _file_state.items() if now - s["created_at"] > FILE_STATE_TTL]
        for cid in stale:
            tel.log(SERVICE, cid, "file_state_cleanup", "timeout", level="WARN")
            del _file_state[cid]


async def main() -> None:
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=PREFETCH_COUNT)
    exchange = channel.default_exchange

    text_queue = await channel.declare_queue(TEXT_TASKS_QUEUE, durable=True)

    file_queue_name = f"{FILE_TASKS_QUEUE_PREFIX}_{FILE_WORKER_PARTITION_INDEX}"
    file_queue = await channel.declare_queue(file_queue_name, durable=True)

    tel.log(SERVICE, tel.new_trace(), "startup",
            detail=f"partition={FILE_WORKER_PARTITION_INDEX} file_queue={file_queue_name}")

    await text_queue.consume(lambda msg: handle_text(msg, exchange))
    await file_queue.consume(lambda msg: handle_file_chunk(msg, exchange))

    asyncio.create_task(_periodic_cleanup())

    await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
