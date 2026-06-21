FROM python:3.11-slim
WORKDIR /app
COPY service_b/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY proto/telemetry.proto ./proto/telemetry.proto
RUN python -m grpc_tools.protoc -Iproto --python_out=. --grpc_python_out=. proto/telemetry.proto
COPY lib/ ./lib/
COPY service_b/worker.py ./worker.py
CMD ["python", "worker.py"]
