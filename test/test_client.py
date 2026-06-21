#!/usr/bin/env python3
import asyncio
import os
import random
import subprocess
import sys
import time
from pathlib import Path


_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(_ROOT))


def _ensure_proto():
    if (_ROOT / "edgescale_pb2.py").exists():
        return
    subprocess.check_call([
        sys.executable, "-m", "grpc_tools.protoc",
        f"-I{_ROOT / 'proto'}",
        f"--python_out={_ROOT}",
        f"--grpc_python_out={_ROOT}",
        str(_ROOT / "proto" / "edgescale.proto"),
    ])

_ensure_proto()

import grpc
import edgescale_pb2
import edgescale_pb2_grpc

HOST = os.getenv("GRPC_HOST", "localhost:50051")
SAMPLE_FILE = Path(__file__).parent / "sample.txt"

WORDS = [
    "edge", "device", "telemetry", "cloud", "scale", "pipeline",
    "stream", "data", "network", "sensor", "metrics", "gateway",
]


async def test_heartbeat(stub):
    print("\n── Heartbeat ──")
    await stub.Heartbeat(edgescale_pb2.HeartbeatRequest(device_id="dev-1", timestamp=int(time.time())))
    print("  OK")


async def test_text(stub):
    print("\n── AnalyzeText ──")
    expected = 20
    text = " ".join(random.choices(WORDS, k=expected))
    resp = await stub.AnalyzeText(edgescale_pb2.AnalyzeTextRequest(device_id="dev-1", text=text))
    assert resp.word_count == expected, f"expected {expected}, got {resp.word_count}"
    print(f"  word_count={resp.word_count}  OK")


async def _stream_file(stub, path: Path):
    async def single_chunk():
        yield edgescale_pb2.FileRequest(device_id="dev-1", data=path.read_bytes())

    return await stub.UploadAndAnalyzeFile(single_chunk())


async def test_file(stub):
    print("\n── UploadAndAnalyzeFile ──")
    expected = len(SAMPLE_FILE.read_text().split())
    resp = await _stream_file(stub, SAMPLE_FILE)
    assert resp.word_count == expected, f"expected {expected}, got {resp.word_count}"
    print(f"  word_count={resp.word_count}  OK")


async def test_concurrent(stub, n=20):
    print(f"\n── Concurrent ({n} parallel AnalyzeText) ──")
    t0 = time.perf_counter()
    results = await asyncio.gather(*[
        stub.AnalyzeText(edgescale_pb2.AnalyzeTextRequest(
            device_id=f"dev-{i}",
            text=" ".join(random.choices(WORDS, k=10)),
        ))
        for i in range(n)
    ], return_exceptions=True)
    errors = [r for r in results if isinstance(r, Exception)]
    print(f"  success={n - len(errors)}  errors={len(errors)}  time={time.perf_counter() - t0:.2f}s")


async def main():
    async with grpc.aio.insecure_channel(HOST) as channel:
        stub = edgescale_pb2_grpc.ServiceAStub(channel)
        await test_heartbeat(stub)
        await test_text(stub)
        await test_file(stub)
        await test_concurrent(stub, n=20)
    print("\nAll tests passed.")


if __name__ == "__main__":
    asyncio.run(main())
