import asyncio
import os

import aio_pika

from lib.config import RABBITMQ_URL
from lib.consts import TEXT_TASKS_QUEUE, FILE_TASKS_QUEUE
import lib.telemetry_client as tel

SERVICE = "service_b"

PREFETCH_COUNT = int(os.getenv("TEXT_WORKER_CONCURRENCY", "10"))


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
    chunk_wc = len(message.body.decode("utf-8", errors="ignore").split())
    tel.log(SERVICE, cid, "chunk_processed", detail=f"chunk_index={chunk_index} wc={chunk_wc}")
    await exchange.publish(
        aio_pika.Message(
            body=chunk_wc.to_bytes(4, "big"),
            correlation_id=cid,
            headers={"chunk_index": chunk_index},
        ),
        routing_key=message.reply_to,
    )
    tel.log(SERVICE, cid, "chunk_result_published", detail=f"chunk_index={chunk_index}")
    await message.ack()


async def main() -> None:
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=PREFETCH_COUNT)
    exchange = channel.default_exchange

    text_queue, file_queue = await asyncio.gather(
        channel.declare_queue(TEXT_TASKS_QUEUE, durable=True),
        channel.declare_queue(FILE_TASKS_QUEUE, durable=True),
    )

    await text_queue.consume(lambda msg: handle_text(msg, exchange))
    await file_queue.consume(lambda msg: handle_file_chunk(msg, exchange))

    tel.log(SERVICE, tel.new_trace(), "startup")
    await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
