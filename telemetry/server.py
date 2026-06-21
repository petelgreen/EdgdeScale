import logging
import os
from concurrent import futures

import grpc
import telemetry_pb2
import telemetry_pb2_grpc

_log = logging.getLogger("telemetry")
_log.setLevel(logging.INFO)
_log.propagate = False
_h = logging.FileHandler(os.getenv("LOG_FILE", "/logs/app.log"))
_h.setFormatter(logging.Formatter(
    "%(asctime)s [%(trace_id)s] %(service)-12s %(levelname)s  %(message)s"
))
_log.addHandler(_h)

_LEVELS = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
}


class TelemetryServicer(telemetry_pb2_grpc.TelemetryServicer):
    def WriteLog(self, request, context):
        _log.log(
            _LEVELS.get(request.level, logging.INFO),
            request.message,
            extra={"trace_id": request.trace_id, "service": request.service},
        )
        return telemetry_pb2.LogResponse(ok=True)


if __name__ == "__main__":
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    telemetry_pb2_grpc.add_TelemetryServicer_to_server(TelemetryServicer(), server)
    server.add_insecure_port("[::]:50051")
    server.start()
    print("Telemetry on :50051 →", os.getenv("LOG_FILE", "/logs/app.log"))
    server.wait_for_termination()
