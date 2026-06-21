import json
import os
import queue
import threading
import uuid
import grpc
import telemetry_pb2
import telemetry_pb2_grpc

_q: queue.SimpleQueue = queue.SimpleQueue()

def _worker():
    stub = telemetry_pb2_grpc.TelemetryStub(
        grpc.insecure_channel(os.getenv("TELEMETRY_HOST", "telemetry:50051"))
    )
    while True:
        req = _q.get()
        if req is None:
            break
        try:
            stub.WriteLog(req, wait_for_ready=True)
        except Exception:
            pass

threading.Thread(target=_worker, daemon=True).start()


def new_trace() -> str:
    return uuid.uuid4().hex[:8]


def log(
    service: str,
    trace_id: str,
    operation: str,
    status: str = "ok",
    *,
    duration_ms: float | None = None,
    device_id: str | None = None,
    error: str | None = None,
    detail: str | None = None,
    level: str = "INFO",
) -> None:
    payload: dict = {"operation": operation, "status": status}
    if duration_ms is not None:
        payload["duration_ms"] = duration_ms
    if device_id is not None:
        payload["device_id"] = device_id
    if error is not None:
        payload["error"] = error
    if detail is not None:
        payload["detail"] = detail
    _q.put(telemetry_pb2.LogRequest(service=service, trace_id=trace_id, level=level, message=json.dumps(payload)))
