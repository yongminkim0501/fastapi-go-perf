"""
테스트용 더미 업스트림.

Go 서버를 만들기 전에 게이트웨이만 먼저 측정하고 싶을 때 쓴다.
지연시간을 쿼리로 주입할 수 있어서 '느린 업스트림' 상황을 재현할 수 있다.

실행:
    uvicorn upstream_stub:app --port 9001

사용:
    GET /             -> 즉시 응답
    GET /?delay_ms=50 -> 50ms 후 응답 (async sleep, 이벤트 루프 안 막음)
    GET /?block_ms=50 -> 50ms 동안 이벤트 루프를 실제로 블로킹 (나쁜 서버 재현용)
"""

import asyncio
import time
import os
from fastapi import FastAPI, Request

app = FastAPI()
NAME = os.getenv("STUB_NAME", "stub")


@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
async def echo(request: Request, path: str):
    delay_ms = float(request.query_params.get("delay_ms", 0))
    block_ms = float(request.query_params.get("block_ms", 0))

    if delay_ms:
        await asyncio.sleep(delay_ms / 1000)

    if block_ms:
        # 의도적 블로킹. 이걸 켜고 부하를 주면 이벤트 루프가 막히는 걸
        # 눈으로 확인할 수 있다. p99가 어떻게 튀는지 꼭 한 번 보길 권함.
        end = time.perf_counter() + block_ms / 1000
        while time.perf_counter() < end:
            pass

    return {
        "service": NAME,
        "path": "/" + path,
        "request_id": request.headers.get("x-request-id"),
    }