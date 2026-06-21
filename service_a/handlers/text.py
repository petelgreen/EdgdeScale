import asyncio
import aio_pika
import grpc
import edgescale_pb2
from lib.consts import TEXT_TASKS_QUEUE, TEXT_ANALYZE_TIMEOUT
import lib.telemetry_client as tel
from handlers.common import SERVICE, new_pending
from handlers import errors as err


async def _publish_and_wait(channel, reply_q, cid, future, request):
    await channel.default_exchange.publish(
        aio_pika.Message(body=request.text.encode(), correlation_id=cid, reply_to=reply_q),
        routing_key=TEXT_TASKS_QUEUE,
    )
    tel.log(SERVICE, cid, "text_task_published")
    word_count = await asyncio.wait_for(future, timeout=TEXT_ANALYZE_TIMEOUT)
    tel.log(SERVICE, cid, "analyze_text", detail=f"word_count={word_count}")
    return edgescale_pb2.AnalyzeTextResponse(word_count=word_count)


async def analyze_text(
    channel: aio_pika.Channel,
    reply_q: str,
    pending: dict,
    request,
    context,
) -> edgescale_pb2.AnalyzeTextResponse:
    cid, future = new_pending(pending)
    tel.log(SERVICE, cid, "analyze_text", "start", device_id=request.device_id)

    if not request.text or not request.text.strip():
        await err.abort(context, grpc.StatusCode.INVALID_ARGUMENT, err.INVALID_INPUT,
                        service=SERVICE, cid=cid, operation="analyze_text")

    try:
        return await err.handle_rpc(context, SERVICE, cid, "analyze_text",
                                    _publish_and_wait(channel, reply_q, cid, future, request))
    finally:
        pending.pop(cid, None)


async def on_text_result(message: aio_pika.IncomingMessage, pending: dict) -> None:
    cid = message.correlation_id
    future = pending.get(cid)
    if future and not future.done():
        tel.log(SERVICE, cid, "text_result_received")
        future.set_result(int.from_bytes(message.body, "big"))
    else:
        tel.log(SERVICE, cid or "unknown", "text_result_received", "error",
                error="unknown correlation_id", level="WARN")
