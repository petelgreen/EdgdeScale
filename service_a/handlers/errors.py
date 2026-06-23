import asyncio
import aio_pika
import aio_pika.exceptions
import grpc
import lib.telemetry_client as tel

# gRPC detail strings — all error strings live here, no magic strings in handlers
WORKER_TIMEOUT     = "worker_timeout"
BROKER_UNAVAILABLE = "broker_unavailable"
PUBLISH_FAILED     = "publish_failed"
CLIENT_CANCELLED   = "client_cancelled"
INVALID_INPUT      = "invalid_input"
CHUNK_TOO_LARGE    = "chunk_too_large"
DECODE_ERROR       = "decode_error"
INTERNAL_ERROR     = "internal_error"


class ChunkTooLargeError(Exception):
    pass


async def consume(
    message: aio_pika.IncomingMessage,
    service: str,
    operation: str,
    fn,
    *args,
) -> None:
    """Global consumer wrapper: ack on success, log + nack on any exception."""
    try:
        await fn(message, *args)
        await message.ack()
    except Exception as e:
        tel.log(service, message.correlation_id or "unknown", operation, "error",
                error=repr(e), level="ERROR")
        await message.nack(requeue=False)


async def handle_rpc(context, service: str, cid: str, operation: str, coro) -> None:
    """Global gRPC error handler: catches all exceptions, logs to telemetry, aborts context."""
    try:
        return await coro
    except ChunkTooLargeError:
        await abort(context, grpc.StatusCode.INVALID_ARGUMENT, CHUNK_TOO_LARGE,
                    service=service, cid=cid, operation=operation)
    except UnicodeDecodeError:
        await abort(context, grpc.StatusCode.INVALID_ARGUMENT, DECODE_ERROR,
                    service=service, cid=cid, operation=operation)
    except asyncio.TimeoutError:
        await abort(context, grpc.StatusCode.DEADLINE_EXCEEDED, WORKER_TIMEOUT,
                    service=service, cid=cid, operation=operation)
    except asyncio.CancelledError:
        tel.log(service, cid, operation, "cancelled", level="WARN")
        raise
    except aio_pika.exceptions.AMQPConnectionError:
        await abort(context, grpc.StatusCode.UNAVAILABLE, BROKER_UNAVAILABLE,
                    service=service, cid=cid, operation=operation)
    except aio_pika.exceptions.DeliveryError:
        await abort(context, grpc.StatusCode.UNAVAILABLE, PUBLISH_FAILED,
                    service=service, cid=cid, operation=operation)
    except grpc.aio.AbortError:
        raise
    except Exception:
        await abort(context, grpc.StatusCode.INTERNAL, INTERNAL_ERROR,
                    service=service, cid=cid, operation=operation)


async def abort(
    context,
    code: grpc.StatusCode,
    msg: str,
    *,
    service: str,
    cid: str,
    operation: str,
    level: str = "ERROR",
) -> None:
    """Log then abort the gRPC call. Always raises grpc.aio.AbortError."""
    tel.log(service, cid, operation, "error", error=msg, level=level)
    await context.abort(code, msg)
