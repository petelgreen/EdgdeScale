FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir grpcio grpcio-tools protobuf
COPY proto/telemetry.proto ./proto/telemetry.proto
RUN python -m grpc_tools.protoc -Iproto --python_out=. --grpc_python_out=. proto/telemetry.proto
COPY telemetry/server.py ./server.py
CMD ["python", "server.py"]
