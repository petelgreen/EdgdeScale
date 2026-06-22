import os

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
GRPC_PORT = int(os.getenv("GRPC_PORT", "50051"))
MAX_QUEUE_DEPTH = int(os.getenv("MAX_QUEUE_DEPTH", "100"))
FILE_TASK_PARTITIONS = int(os.getenv("FILE_TASK_PARTITIONS", "4"))
FILE_WORKER_PARTITION_INDEX = int(os.getenv("FILE_WORKER_PARTITION_INDEX", "0"))
