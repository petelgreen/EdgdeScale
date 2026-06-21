FROM python:3.11-slim
WORKDIR /app
COPY service_a/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY proto/ ./proto/
RUN python -m grpc_tools.protoc -Iproto --python_out=. --grpc_python_out=. proto/edgescale.proto proto/telemetry.proto
COPY lib/ ./lib/
COPY service_a/handlers/ ./handlers/
COPY service_a/server.py ./server.py
CMD ["python", "server.py"]
