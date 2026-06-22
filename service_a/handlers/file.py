import asyncio
import aio_pika
import edgescale_pb2
from lib.consts import FILE_ANALYZE_TIMEOUT
import lib.telemetry_client as tel
from handlers.common import SERVICE, new_pending
from handlers import errors as err


def split_at_word_boundary(text: str) -> tuple[str, str]:
    i = len(text)
    while i > 0 and not text[i - 1].isspace():
        i -= 1
    return text[:i], text[i:]


async def _stream_and_wait(file_exchange, reply_q, cid, future, request_iterator):
    chunk_index = 0
    leftover = ""
    total_bytes = 0

    async for chunk in request_iterator:
        total_bytes += len(chunk.data)
        if total_bytes > err.MAX_FILE_BYTES:
            raise err.FileTooLargeError()
        if len(chunk.data) > err.MAX_CHUNK_BYTES:
            raise err.ChunkTooLargeError()
        text = leftover + chunk.data.decode("utf-8")
        chunk_text, leftover = split_at_word_boundary(text)
        if not chunk_text:
            continue
        await file_exchange.publish(
            aio_pika.Message(
                body=chunk_text.encode(),
                correlation_id=cid,
                reply_to=reply_q,
                headers={"chunk_index": chunk_index, "is_last": 0},
            ),
            routing_key=cid,
        )
        tel.log(SERVICE, cid, "chunk_published", detail=f"chunk_index={chunk_index}")
        chunk_index += 1

    # EOS — flush any leftover text and signal end-of-stream
    await file_exchange.publish(
        aio_pika.Message(
            body=leftover.encode(),
            correlation_id=cid,
            reply_to=reply_q,
            headers={"chunk_index": chunk_index, "is_last": 1},
        ),
        routing_key=cid,
    )
    tel.log(SERVICE, cid, "eos_published", detail=f"total_data_chunks={chunk_index}")

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
