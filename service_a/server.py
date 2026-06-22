import asyncio
import uuid
import aio_pika
import grpc
import edgescale_pb2
import edgescale_pb2_grpc
from lib.config import RABBITMQ_URL, GRPC_PORT, MAX_QUEUE_DEPTH, FILE_TASK_PARTITIONS
from lib.consts import TEXT_TASKS_QUEUE, FILE_TASKS_EXCHANGE, FILE_TASKS_QUEUE_PREFIX
from handlers import text, file as file_handler, errors as err
from handlers.common import SERVICE, queue_depth
import lib.telemetry_client as tel


class ServiceA(edgescale_pb2_grpc.ServiceAServicer):
    def __init__(self, channel: aio_pika.Channel, text_reply_q: str, file_reply_q: str,
                 file_exchange: aio_pika.Exchange) -> None:
        self._channel = channel
        self._pending: dict[str, asyncio.Future] = {}
        self._text_reply_q = text_reply_q
        self._file_reply_q = file_reply_q
        self._file_exchange = file_exchange

    async def _check_backpressure(self, queue_name: str, context) -> bool:
        depth = await queue_depth(self._channel, queue_name)
        if depth >= MAX_QUEUE_DEPTH:
            tel.log(SERVICE, "N/A", "backpressure", "warn", detail=f"queue={queue_name} depth={depth}", level="WARN")
            await context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, f"queue_full queue={queue_name} depth={depth} limit={MAX_QUEUE_DEPTH}")
            return False
        return True

    async def Heartbeat(self, request, context):
        tid = tel.new_trace()
        tel.log(SERVICE, tid, "heartbeat", device_id=request.device_id)
        return edgescale_pb2.HeartbeatResponse()

    async def AnalyzeText(self, request, context):
        if not await self._check_backpressure(TEXT_TASKS_QUEUE, context):
            return
        return await text.analyze_text(self._channel, self._text_reply_q, self._pending, request, context)

    async def UploadAndAnalyzeFile(self, request_iterator, context):
        if not await self._check_backpressure(f"{FILE_TASKS_QUEUE_PREFIX}_0", context):
            return
        return await file_handler.upload_and_analyze_file(
            self._file_exchange, self._file_reply_q, self._pending, request_iterator, context
        )

    async def on_text_result(self, message: aio_pika.IncomingMessage) -> None:
        await err.consume(message, SERVICE, "text_result", text.on_text_result, self._pending)

    async def on_file_result(self, message: aio_pika.IncomingMessage) -> None:
        await err.consume(message, SERVICE, "file_result", file_handler.on_file_result, self._pending)


async def main() -> None:
    instance_id = str(uuid.uuid4())
    text_reply_q = f"service_a_{instance_id}_text_results"
    file_reply_q = f"service_a_{instance_id}_file_results"

    tel.log(SERVICE, tel.new_trace(), "startup", detail=f"instance_id={instance_id}")

    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    channel = await connection.channel(publisher_confirms=True)

    # text queue
    await channel.declare_queue(TEXT_TASKS_QUEUE, durable=True)

    # consistent-hash exchange for file chunks
    file_exchange = await channel.declare_exchange(
        FILE_TASKS_EXCHANGE, type="x-consistent-hash", durable=True
    )
    tel.log(SERVICE, tel.new_trace(), "exchange_declared", detail=f"exchange={FILE_TASKS_EXCHANGE}")

    # partition queues — each bound with equal weight ("1")
    for i in range(FILE_TASK_PARTITIONS):
        q_name = f"{FILE_TASKS_QUEUE_PREFIX}_{i}"
        q = await channel.declare_queue(q_name, durable=True)
        await q.bind(file_exchange, routing_key="1")
        tel.log(SERVICE, tel.new_trace(), "partition_bound", detail=f"queue={q_name}")

    # ponytail: exclusive+auto_delete = queue lives only while this instance is connected
    text_result_q, file_result_q = await asyncio.gather(
        channel.declare_queue(text_reply_q, exclusive=True, auto_delete=True),
        channel.declare_queue(file_reply_q, exclusive=True, auto_delete=True),
    )

    servicer = ServiceA(channel, text_reply_q, file_reply_q, file_exchange)

    await asyncio.gather(
        text_result_q.consume(servicer.on_text_result),
        file_result_q.consume(servicer.on_file_result),
    )

    server = grpc.aio.server()
    edgescale_pb2_grpc.add_ServiceAServicer_to_server(servicer, server)
    server.add_insecure_port(f"[::]:{GRPC_PORT}")

    tel.log(SERVICE, tel.new_trace(), "startup", detail=f"port={GRPC_PORT}")
    await server.start()
    await server.wait_for_termination()


if __name__ == "__main__":
    asyncio.run(main())
