import asyncio
import aio_pika
import edgescale_pb2
from lib.consts import FILE_TASKS_QUEUE, FILE_ANALYZE_TIMEOUT
import lib.telemetry_client as tel
from handlers.common import SERVICE, new_pending
from handlers import errors as err


def split_at_word_boundary(text: str) -> tuple[str, str]:
    i = len(text)
    while i > 0 and not text[i - 1].isspace():
        i -= 1
    return text[:i], text[i:]


async def _stream_and_wait(channel, reply_q, cid, future, file_agg, request_iterator):
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
        await channel.default_exchange.publish(
            aio_pika.Message(
                body=chunk_text.encode(),
                correlation_id=cid,
                reply_to=reply_q,
                headers={"chunk_index": chunk_index},
            ),
            routing_key=FILE_TASKS_QUEUE,
        )
        tel.log(SERVICE, cid, "chunk_published", detail=f"chunk_index={chunk_index}")
        chunk_index += 1

    if leftover:
        await channel.default_exchange.publish(
            aio_pika.Message(
                body=leftover.encode(),
                correlation_id=cid,
                reply_to=reply_q,
                headers={"chunk_index": chunk_index},
            ),
            routing_key=FILE_TASKS_QUEUE,
        )
        tel.log(SERVICE, cid, "chunk_published", detail=f"chunk_index={chunk_index} leftover_flush=true")
        chunk_index += 1

    agg = file_agg[cid]
    agg["total"] = chunk_index
    tel.log(SERVICE, cid, "file_stream", "ended", detail=f"total_chunks={chunk_index}")

    if not future.done() and len(agg["received"]) == agg["total"]:
        tel.log(SERVICE, cid, "file_stream", "complete", detail=f"final_word_count={agg['wc']}")
        future.set_result(agg["wc"])

    word_count = await asyncio.wait_for(future, timeout=FILE_ANALYZE_TIMEOUT)
    tel.log(SERVICE, cid, "upload_file", detail=f"word_count={word_count}")
    return edgescale_pb2.AnalyzeFileResponse(word_count=word_count)


async def upload_and_analyze_file(
    channel: aio_pika.Channel,
    reply_q: str,
    file_agg: dict,
    pending: dict,
    request_iterator,
    context,
) -> edgescale_pb2.AnalyzeFileResponse:
    cid, future = new_pending(pending)
    file_agg[cid] = {"total": None, "received": set(), "wc": 0}
    tel.log(SERVICE, cid, "file_stream", "start")
    try:
        return await err.handle_rpc(context, SERVICE, cid, "upload_file",
                                    _stream_and_wait(channel, reply_q, cid, future, file_agg, request_iterator))
    finally:
        pending.pop(cid, None)
        file_agg.pop(cid, None)


async def on_file_chunk_result(
    message: aio_pika.IncomingMessage,
    file_agg: dict,
    pending: dict,
) -> None:
    cid = message.correlation_id
    chunk_index = message.headers.get("chunk_index")
    chunk_wc = int.from_bytes(message.body, "big")

    if chunk_index is None:
        tel.log(SERVICE, cid or "unknown", "file_chunk_result", "error",
                error="missing chunk_index header", level="WARN")
        return

    agg = file_agg.get(cid)
    if agg is None:
        tel.log(SERVICE, cid or "unknown", "file_chunk_result", "error",
                error="unknown correlation_id", level="WARN")
        return

    if chunk_index in agg["received"]:
        tel.log(SERVICE, cid, "file_chunk_result", "duplicate", detail=f"chunk_index={chunk_index}")
        return

    agg["received"].add(chunk_index)
    agg["wc"] += chunk_wc
    tel.log(SERVICE, cid, "file_chunk_result", detail=f"chunk_index={chunk_index} wc={chunk_wc}")

    if agg["total"] is not None and len(agg["received"]) == agg["total"]:
        tel.log(SERVICE, cid, "file_stream", "complete", detail=f"final_word_count={agg['wc']}")
        future = pending.get(cid)
        if future and not future.done():
            future.set_result(agg["wc"])
