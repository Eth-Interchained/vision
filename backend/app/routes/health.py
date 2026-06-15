from __future__ import annotations

import json
import logging
import sqlite3

from fastapi import APIRouter

from .. import __version__
from ..config import settings
from ..indexer.main_loop import get_indexer

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/health")
def health():
    """Fully synchronous health check — reads SQLite directly with sqlite3,
    bypassing the async event loop so it always responds instantly."""
    rpc_ok = False
    rpc_height = None
    electrum_ok = False
    db_ok = False
    indexer_height = None

    # Use the indexer's cached state
    indexer = get_indexer()
    if indexer is not None:
        rpc_ok = indexer._rpc_online

    # Read directly from SQLite (synchronous, no event loop)
    try:
        conn = sqlite3.connect(str(settings.SQLITE_DB_PATH), timeout=2)
        # Test DB is alive
        conn.execute("SELECT 1")
        db_ok = True

        cur = conn.execute("SELECT value FROM kv WHERE key = ?", ("vision:indexer:last_height",))
        row = cur.fetchone()
        if row and row[0]:
            indexer_height = int(row[0])

        cur = conn.execute("SELECT value FROM kv WHERE key = ?", ("vision:indexer:status",))
        row = cur.fetchone()
        if row and row[0]:
            status = json.loads(row[0])
            rpc_height = status.get("tip")
            if rpc_height and rpc_height > 0:
                rpc_ok = True

        conn.close()
    except Exception:
        db_ok = False

    # ElectrumX — check if the persistent TCP connection is alive
    try:
        from ..electrumx.client import get_electrumx
        ex = get_electrumx()
        electrum_ok = not ex._closed
    except Exception:
        electrum_ok = False

    # DualStore stats (nedbd primary chain)
    dual_stats: dict = {"active": False}
    try:
        from ..dual_store import get_dual_store
        ds = get_dual_store()
        if ds is not None:
            dual_stats = ds.stats()
    except Exception:
        pass

    overall = "ok" if (rpc_ok and db_ok) else "degraded"
    return {
        "status":         overall,
        "rpc":            rpc_ok,
        "rpc_height":     rpc_height,
        "electrumx":      electrum_ok,
        "db":             db_ok,
        "indexer_height": indexer_height,
        "version":        __version__,
        "dual_store":     dual_stats,
    }
