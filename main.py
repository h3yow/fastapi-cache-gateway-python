"""FastAPI surface for the local cache gateway."""
from __future__ import annotations

from typing import Any

import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from app.errors import ScopeForbidden, UpstreamError
from app.gateway import CacheGateway, CachePolicy
from app.upstream import LocalUpstream

upstream = LocalUpstream(latency=0.05)
gateway = CacheGateway(upstream, CachePolicy(ttl=1.0, max_stale=5.0,
                                             stale_while_revalidate=True))

app = FastAPI(title="fastapi-cache-gateway")


class PutBody(BaseModel):
    value: Any


def _result_json(r) -> dict:
    return {"value": r.value, "fresh": r.fresh, "stale": r.stale,
            "from_cache": r.from_cache, "scope": r.scope, "key": r.key}


@app.get("/cache/{scope}/{key}")
async def cache_get(scope: str, key: str):
    try:
        return _result_json(await gateway.get(scope, key))
    except ScopeForbidden as e:
        raise HTTPException(status_code=403, detail=str(e))
    except UpstreamError as e:
        status = 504 if e.kind == "timeout" else 502
        raise HTTPException(status_code=status,
                            detail={"kind": e.kind, "message": str(e)})


@app.put("/cache/{scope}/{key}")
async def cache_put(scope: str, key: str, body: PutBody):
    try:
        await gateway.put(scope, key, body.value)
    except ScopeForbidden as e:
        raise HTTPException(status_code=403, detail=str(e))
    return {"ok": True}


@app.delete("/cache/{scope}/{key}")
async def cache_invalidate(scope: str, key: str):
    try:
        await gateway.invalidate(scope, key)
    except ScopeForbidden as e:
        raise HTTPException(status_code=403, detail=str(e))
    return {"ok": True}


# -- demo hooks to steer the local upstream --------------------------------

@app.post("/upstream/{key}")
async def upstream_set(key: str, body: PutBody):
    upstream.set_value(key, body.value)
    return {"ok": True}


@app.get("/upstream/stats")
async def upstream_stats():
    return {"call_count": upstream.call_count}


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
