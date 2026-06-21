import asyncio
import uuid
import aio_pika

SERVICE = "service_a"


def new_pending(pending: dict) -> tuple[str, asyncio.Future]:
    cid = str(uuid.uuid4())
    future = asyncio.get_running_loop().create_future()
    pending[cid] = future
    return cid, future


async def queue_depth(channel: aio_pika.Channel, queue_name: str) -> int:
    q = await channel.declare_queue(queue_name, passive=True)
    return q.declaration_result.message_count
