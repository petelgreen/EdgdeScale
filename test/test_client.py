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


async def test_file_multi_chunk(stub, chunk_size=512):
    print(f"\n── UploadAndAnalyzeFile (multi-chunk, chunk_size={chunk_size}B) ──")
    raw = SAMPLE_FILE.read_bytes()
    expected = len(raw.decode("utf-8").split())

    async def chunked():
        for i in range(0, len(raw), chunk_size):
            yield edgescale_pb2.FileRequest(device_id="dev-mc", data=raw[i:i + chunk_size])

    resp = await stub.UploadAndAnalyzeFile(chunked())
    n_chunks = -(-len(raw) // chunk_size)  # ceiling div
    assert resp.word_count == expected, f"expected {expected}, got {resp.word_count}"
    print(f"  chunks={n_chunks}  word_count={resp.word_count}  OK")


async def test_concurrent_files(stub, n=5, chunk_size=512):
    print(f"\n── Concurrent file uploads ({n} parallel, multi-chunk) ──")
    raw = SAMPLE_FILE.read_bytes()
    expected = len(raw.decode("utf-8").split())

    async def chunked(device_id):
        for i in range(0, len(raw), chunk_size):
            yield edgescale_pb2.FileRequest(device_id=device_id, data=raw[i:i + chunk_size])

    t0 = time.perf_counter()
    results = await asyncio.gather(*[
        stub.UploadAndAnalyzeFile(chunked(f"dev-cf{i}"))
        for i in range(n)
    ], return_exceptions=True)
    errors = [r for r in results if isinstance(r, Exception)]
    for r in results:
        if not isinstance(r, Exception):
            assert r.word_count == expected, f"expected {expected}, got {r.word_count}"
    print(f"  success={n - len(errors)}  errors={len(errors)}  time={time.perf_counter() - t0:.2f}s")
    assert not errors, f"unexpected errors: {errors}"


async def test_backpressure(stub, n=200):
    """Flood with n requests (default limit=100). All failures must be graceful RESOURCE_EXHAUSTED."""
    print(f"\n── Backpressure resilience ({n} concurrent, limit=100) ──")
    results = await asyncio.gather(*[
        stub.AnalyzeText(edgescale_pb2.AnalyzeTextRequest(
            device_id=f"dev-bp-{i}",
            text=" ".join(random.choices(WORDS, k=5)),
        ))
        for i in range(n)
    ], return_exceptions=True)
    ok = sum(1 for r in results if not isinstance(r, Exception))
    bp = sum(1 for r in results if isinstance(r, grpc.aio.AioRpcError) and r.code() == grpc.StatusCode.RESOURCE_EXHAUSTED)
    other = [r for r in results if isinstance(r, Exception) and not (isinstance(r, grpc.aio.AioRpcError) and r.code() == grpc.StatusCode.RESOURCE_EXHAUSTED)]
    print(f"  success={ok}  backpressure={bp}  unexpected_errors={len(other)}")
    assert not other, f"unexpected errors: {other}"
    print("  OK — all failures were graceful RESOURCE_EXHAUSTED")


async def main():
    async with grpc.aio.insecure_channel(HOST) as channel:
        stub = edgescale_pb2_grpc.ServiceAStub(channel)
        await test_heartbeat(stub)
        await test_text(stub)
        await test_file(stub)
        await test_file_multi_chunk(stub)
        await test_concurrent(stub, n=20)
        await test_concurrent_files(stub, n=5)
        await test_backpressure(stub, n=200)
    print("\nAll tests passed.")


if __name__ == "__main__":
    asyncio.run(main())
