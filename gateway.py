import os
import time
import uuid
import logging

import httpx
from fastapi import FastAPI, Request, Response
from contextlib import asynccontextmanager

ROUTES: dict[str, str] = {
    "/api/orders": os.getenv("UPSTREAM_ORDERS", "http://127.0.0.1:9001"),
    "/api/users": os.getenv("UPSTREAM_USERS", "http://127.0.0.1:9002"),
}

# 커넥션 풀 — 여기가 병목 실험의 1번 후보다.
# max_connections 를 일부러 작게 잡으면 인위적으로 병목을 만들 수 있다.
MAX_CONNECTIONS = int(os.getenv("MAX_CONNECTIONS", "100"))
MAX_KEEPALIVE = int(os.getenv("MAX_KEEPALIVE", "20"))
KEEPALIVE_EXPIRY = float(os.getenv("KEEPALIVE_EXPIRY", "5.0"))

# 타임아웃 — connect / read 를 분리해서 잡는 게 중요하다.
# 하나로 뭉쳐놓으면 "연결이 안 되는 것"과 "응답이 느린 것"을 구분할 수 없다.
CONNECT_TIMEOUT = float(os.getenv("CONNECT_TIMEOUT", "2.0"))
READ_TIMEOUT = float(os.getenv("READ_TIMEOUT", "10.0"))

# 요청 단위로 지나가면 안 되는 헤더 (RFC 9110 hop-by-hop)
# end to end에서 필요한 정보와 hop by hop 에서 필요한 정보를 분리하여 네트워크 중간 노드 들이 각자의 구간을 효율적이고 안전하게 관리할 때 사용
HOP_BY_HOP = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailers", "transfer-encoding", "upgrade", "host",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("gateway")

# ------------------------------------------------------------------------------
# 앱 수명 주기 - 클라이언트를 한 번만 만들고 재사용
# ------------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    limits = httpx.Limits(
        max_connections=MAX_CONNECTIONS,
        max_keepalive_connections=MAX_KEEPALIVE,
        keepalive_expiry=KEEPALIVE_EXPIRY,
    )
    timeout = httpx.Timeout(
        connect=CONNECT_TIMEOUT,
        read=READ_TIMEOUT,
        write=READ_TIMEOUT,
        pool=None,  # None = 풀에서 커넥션 기다리는 시간 무제한.
        # 실험할 때 이걸 숫자로 바꾸면 "풀 대기"가 타임아웃으로 드러난다.
    )
    app.state.client = httpx.AsyncClient(limits=limits, timeout=timeout)
    log.info(
        "gateway up | max_conn=%d keepalive=%d routes=%s",
        MAX_CONNECTIONS, MAX_KEEPALIVE, list(ROUTES),
    )
    yield
    await app.state.client.aclose()

app = FastAPI(lifespan=lifespan)

# ---------------------------------------------------------------------------
# 미들웨어 — 모든 요청의 지연시간을 남긴다
# ---------------------------------------------------------------------------

@app.middleware("http")
async def observability(request: Request, call_next):
    # 요청 ID: 게이트웨이에서 발급해서 업스트림까지 넘긴다.
    # 나중에 분산 트레이싱 붙일 때 이게 출발점이 된다.
    request_id = request.headers.get("x-request-id") or uuid.uuid4().hex[:16]
    request.state.request_id = request_id

    start = time.perf_counter()
    try:
        response = await call_next(request)
        status = response.status_code
    except Exception:
        status = 500
        raise
    finally:
        elapsed_ms = (time.perf_counter() - start) * 1000
        # 여기서 찍은 로그가 나중에 p50/p99 계산의 원본 데이터가 된다.
        # 파싱하기 쉽게 고정 포맷으로 남기는 게 중요.
        log.info(
            "rid=%s method=%s path=%s status=%s elapsed_ms=%.2f",
            request_id, request.method, request.url.path, status, elapsed_ms,
        )

    response.headers["x-request-id"] = request_id
    response.headers["x-gateway-elapsed-ms"] = f"{elapsed_ms:.2f}"
    return response


# ---------------------------------------------------------------------------
# 헬스체크 — 부하 테스트의 베이스라인용
# ---------------------------------------------------------------------------

@app.get("/healthz")
async def healthz():
    """업스트림을 거치지 않는 경로.

    이 엔드포인트의 처리량이 곧 '게이트웨이 자체의 상한'이다.
    프록시 경로가 이것보다 느리다면 그 차이가 업스트림+네트워크 비용이고,
    이것마저 느리다면 게이트웨이가 병목이다. 반드시 먼저 재보고 시작할 것.
    """
    return {"status": "ok"}


@app.get("/pool")
async def pool_stats():
    """커넥션 풀 상태를 들여다보는 창.

    httpx 내부 구조라 버전에 따라 깨질 수 있다. 실패하면 그냥 넘어가게 처리.
    부하 중에 이걸 폴링하면 '풀이 가득 찼는지'를 눈으로 볼 수 있다.
    """
    try:
        pool = app.state.client._transport._pool
        return {
            "max_connections": MAX_CONNECTIONS,
            "current_connections": len(pool.connections),
            "requests_waiting": len(getattr(pool, "_requests", [])),
        }
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


#---------------------------------------------------------------------------
# 프록시 본체
# ---------------------------------------------------------------------------

def resolve_upstream(path: str) -> tuple[str, str] | None:
    """경로에 맞는 업스트림을 찾는다. 긴 prefix 우선."""
    for prefix in sorted(ROUTES, key=len, reverse=True):
        if path == prefix or path.startswith(prefix + "/"):
            return ROUTES[prefix], path[len(prefix):] or "/"
    return None


@app.api_route(
    "/{full_path:path}",
    methods=["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"],
)
async def proxy(request: Request, full_path: str):
    target = resolve_upstream("/" + full_path)
    if target is None:
        return Response(content='{"error":"no route"}', status_code=404,
                        media_type="application/json")

    base, rest = target
    url = httpx.URL(base + rest, query=request.url.query.encode())

    # 헤더 정리: hop-by-hop 제거 + 추적 헤더 주입
    headers = {
        k: v for k, v in request.headers.items()
        if k.lower() not in HOP_BY_HOP
    }
    headers["x-request-id"] = request.state.request_id
    headers["x-forwarded-for"] = request.client.host if request.client else "unknown"

    body = await request.body()

    upstream_start = time.perf_counter()
    try:
        upstream = await request.app.state.client.request(
            method=request.method,
            url=url,
            headers=headers,
            content=body,
        )
    except httpx.ConnectTimeout:
        # 연결 자체가 안 됨 → 업스트림이 죽었거나 accept 큐가 꽉 참
        return Response(content='{"error":"upstream connect timeout"}',
                        status_code=504, media_type="application/json")
    except httpx.ReadTimeout:
        # 연결은 됐는데 응답이 느림 → 업스트림 내부 처리가 병목
        return Response(content='{"error":"upstream read timeout"}',
                        status_code=504, media_type="application/json")
    except httpx.HTTPError as e:
        log.warning("rid=%s upstream error: %s", request.state.request_id, e)
        return Response(content='{"error":"bad gateway"}',
                        status_code=502, media_type="application/json")
    finally:
        upstream_ms = (time.perf_counter() - upstream_start) * 1000

    out_headers = {
        k: v for k, v in upstream.headers.items()
        if k.lower() not in HOP_BY_HOP and k.lower() != "content-length"
    }
    # 게이트웨이 총 시간과 업스트림 시간을 분리해서 노출.
    # 둘의 차이가 곧 '게이트웨이 자체 오버헤드'다. 이 숫자를 계속 지켜볼 것.
    out_headers["x-upstream-elapsed-ms"] = f"{upstream_ms:.2f}"

    return Response(
        content=upstream.content,
        status_code=upstream.status_code,
        headers=out_headers,
    )
