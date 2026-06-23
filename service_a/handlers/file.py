import asyncio
import aio_pika
import edgescale_pb2
from lib.consts import FILE_ANALYZE_TIMEOUT, MAX_CHUNK_BYTES
import lib.telemetry_client as tel
from handlers.common import SERVICE, new_pending
from handlers import errors as err


async def _stream_and_wait(file_exchange, reply_q, cid, future, request_iterator):
    chunk_index = 0

    async for chunk in request_iterator:
        if len(chunk.data) > MAX_CHUNK_BYTES:
            raise err.ChunkTooLargeError()
        await file_exchange.publish(
            aio_pika.Message(
                body=chunk.data,
                correlation_id=cid,
                reply_to=reply_q,
                headers={"chunk_index": chunk_index, "is_last": 0},
            ),
            routing_key=cid,
        )
        tel.log(SERVICE, cid, "chunk_published", detail=f"chunk_index={chunk_index}")
        chunk_index += 1

    await file_exchange.publish(
        aio_pika.Message(
            body=b"",
            correlation_id=cid,
            reply_to=reply_q,
            headers={"chunk_index": chunk_index, "is_last": 1},
        ),
        routing_key=cid,
    )
    tel.log(SERVICE, cid, "eos_published", detail=f"total_chunks={chunk_index}")

    word_count = await asyncio.wait_for(future, timeout=FILE_ANALYZE_TIMEOUT)
    tel.log(SERVICE, cid, "upload_file", detail=f"word_count={word_count}")
    return edgescale_pb2.AnalyzeFileResponse(word_count=word_count)


async def upload_and_analyze_file(
    file_exchange: aio_pika.Exchange,
    reply_q: str,
    pending: dict,
    request_iterator,
    context,
) -> edgescale_pb2.AnalyzeFileResponse:
    cid, future = new_pending(pending)
    tel.log(SERVICE, cid, "file_stream", "start")
    try:
        return await err.handle_rpc(context, SERVICE, cid, "upload_file",
                                    _stream_and_wait(file_exchange, reply_q, cid, future, request_iterator))
    finally:
        pending.pop(cid, None)


async def on_file_result(message: aio_pika.IncomingMessage, pending: dict) -> None:
    cid = message.correlation_id
    future = pending.get(cid)
    if future and not future.done():
        tel.log(SERVICE, cid, "file_result_received")
        future.set_result(int.from_bytes(message.body, "big"))
    else:
        tel.log(SERVICE, cid or "unknown", "file_result_received", "error",
                error="unknown correlation_id", level="WARN")
