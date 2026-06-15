"""NEDB superpower routes — exposes raw NEDB capabilities to the frontend.

Unlike the rest of the Vision API, these routes talk to ``nedbd`` *directly*
(not through ``nedb_store``) because they intentionally surface features
that aren't part of the Redis-shaped store abstraction: NQL queries,
``AS OF`` time travel, ``TRACE caused_by`` causal chains, and Merkle-root
verification.

All endpoints return ``503`` when ``settings.NEDB_URL`` is empty (Vision is
configured to run against SQLite only).
"""

from __future__ import annotations

import logging
from typing import Any, Optional

import httpx
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from ..config import settings

logger = logging.getLogger(__name__)
router = APIRouter()


# ── HTTP plumbing ───────────────────────────────────────────────────────────

_client: Optional[httpx.AsyncClient] = None


def _require_configured() -> str:
    """Return the nedbd base URL or raise 503 if NEDB is not configured."""
    if not settings.NEDB_URL:
        raise HTTPException(
            status_code=503,
            detail="NEDB is not configured (set NEDB_URL to enable these routes).",
        )
    return settings.NEDB_URL.rstrip("/")


def _get_client() -> httpx.AsyncClient:
    global _client
    if _client is None:
        headers = {}
        if settings.NEDBD_TOKEN:
            headers["Authorization"] = f"Bearer {settings.NEDBD_TOKEN}"
        _client = httpx.AsyncClient(
            base_url=_require_configured(),
            headers=headers,
            timeout=30.0,
        )
    return _client


async def _proxy_get(path: str) -> dict:
    client = _get_client()
    try:
        resp = await client.get(path)
        resp.raise_for_status()
        return resp.json()
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=e.response.text) from e
    except httpx.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"nedbd unreachable: {e}") from e


async def _proxy_post(path: str, payload: dict) -> dict:
    client = _get_client()
    try:
        resp = await client.post(path, json=payload)
        resp.raise_for_status()
        return resp.json()
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=e.response.text) from e
    except httpx.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"nedbd unreachable: {e}") from e


# ── request/response models ─────────────────────────────────────────────────


class QueryBody(BaseModel):
    nql: str
    db: Optional[str] = None


# ── routes ──────────────────────────────────────────────────────────────────


@router.get("/nedb/status")
async def nedb_status() -> dict:
    """nedbd health + selected database info."""
    _require_configured()
    health = await _proxy_get("/health")
    db_info: dict[str, Any] = {}
    try:
        db_info = await _proxy_get(f"/v1/databases/{settings.NEDB_DB_NAME}")
    except HTTPException as e:
        # 404 here means the configured database hasn't been created yet —
        # don't fail the whole status call.
        if e.status_code != 404:
            raise
        db_info = {"name": settings.NEDB_DB_NAME, "exists": False}
    return {
        "ok": bool(health.get("ok")),
        "service": health.get("service"),
        "version": health.get("version"),
        "databases": health.get("databases"),
        "encrypted": health.get("encrypted"),
        "db": db_info,
    }


@router.post("/nedb/query")
async def nedb_query(body: QueryBody) -> dict:
    """Run an arbitrary NQL query against the configured (or override) database."""
    _require_configured()
    db = body.db or settings.NEDB_DB_NAME
    if not body.nql or not body.nql.strip():
        raise HTTPException(status_code=400, detail="nql query is required")
    result = await _proxy_post(f"/v1/databases/{db}/query", {"nql": body.nql})
    return result


@router.get("/nedb/token-history/{token_id}")
async def nedb_token_history(
    token_id: str,
    as_of: Optional[int] = Query(default=None, description="Sequence number to rewind to"),
    limit: int = Query(default=200, ge=1, le=10_000),
) -> dict:
    """Return ITSL ops for ``token_id``, optionally pinned to a historical seq."""
    _require_configured()
    safe_token = token_id.replace('"', '\\"')
    as_of_clause = f" AS OF {int(as_of)}" if as_of is not None else ""
    nql = (
        f'FROM itsl_ops{as_of_clause} '
        f'WHERE token = "{safe_token}" '
        f"ORDER BY seq DESC LIMIT {int(limit)}"
    )
    return await _proxy_post(
        f"/v1/databases/{settings.NEDB_DB_NAME}/query", {"nql": nql}
    )


@router.get("/nedb/trace/{token_id}")
async def nedb_trace(
    token_id: str,
    reverse: bool = Query(default=False, description="Trace forward causality if true"),
) -> dict:
    """TRACE caused_by — walk the causal chain for a token id.

    NEDB's ``TRACE caused_by`` clause follows the ``caused_by`` pointers on
    ``itsl_ops`` documents to surface every op that contributed (transitively)
    to the current state of the token.
    """
    _require_configured()
    safe_token = token_id.replace('"', '\\"')
    direction = "TRACE caused_by REVERSE" if reverse else "TRACE caused_by"
    nql = (
        f'FROM itsl_ops WHERE token = "{safe_token}" {direction} LIMIT 1000'
    )
    return await _proxy_post(
        f"/v1/databases/{settings.NEDB_DB_NAME}/query", {"nql": nql}
    )


@router.get("/nedb/block/{height}")
async def nedb_block(
    height: int,
    as_of: Optional[int] = Query(default=None),
) -> dict:
    """Return the block document for ``height``, optionally AS OF a past seq."""
    _require_configured()
    as_of_clause = f" AS OF {int(as_of)}" if as_of is not None else ""
    nql = f"FROM blocks{as_of_clause} WHERE height = {int(height)} LIMIT 1"
    return await _proxy_post(
        f"/v1/databases/{settings.NEDB_DB_NAME}/query", {"nql": nql}
    )


@router.get("/nedb/verify")
async def nedb_verify() -> dict:
    """Verify the hash-chain integrity of the configured database.

    Returns ``{ok, seq, head, tamper_evident}``. ``tamper_evident`` is always
    ``True`` for NEDB — every write is BLAKE2b-chained — but the field is
    explicit so the frontend can label the result.
    """
    _require_configured()
    result = await _proxy_get(f"/v1/databases/{settings.NEDB_DB_NAME}/verify")
    return {
        "ok": bool(result.get("ok")),
        "seq": result.get("seq"),
        "head": result.get("head"),
        "tamper_evident": True,
    }
