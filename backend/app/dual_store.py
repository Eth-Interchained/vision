"""dual_store.py — bi-directional, sticky dual-write store for Vision.

Wraps a SQLiteStore (primary, always reliable) and a NedbStore (secondary,
temporal, NQL-queryable). All writes go to both. Reads prefer nedbd — once a
key exists there it sticks — and fall back to SQLite transparently.

No migration required. nedbd self-populates through normal Vision operation:
every write from the block indexer, mempool poller, and token registry flows
into both stores simultaneously. SQLite is always the safety net.

Architecture
------------

    WRITE  →  SQLite (await, source of truth)
           →  nedbd  (fire-and-forget asyncio task, best-effort)

    READ   →  nedbd first (sticky: if nedbd has it, always use it)
           →  SQLite fallback on miss or nedbd unavailability

    DOWN   →  If nedbd is unreachable, Vision degrades gracefully to
              SQLite-only. All reads/writes continue without interruption.
              Background tasks drain naturally; no queue builds up.

Usage
-----

    # main.py lifespan — replaces current init_db() call:
    if settings.NEDB_URL:
        sqlite = await sqlite_store.init_db()
        nedb   = await nedb_store.init_db()
        store  = DualStore(sqlite, nedb)
        set_active_store(store)
    else:
        await sqlite_store.init_db()

    # Everywhere else — no changes needed:
    db = get_db()   # returns DualStore if nedb enabled, SQLiteStore otherwise
    await db.get("vision:tip:height")
    await db.set("vision:tip:height", 14501)
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, List, Optional, Sequence, Tuple

from .sqlite_store import SQLiteStore
from .nedb_store import NedbStore

logger = logging.getLogger(__name__)

# How many background write failures to log before suppressing (avoids log spam)
_WARN_EVERY = 100


class DualStore:
    """Bi-directional dual-write store — SQLite primary, nedbd secondary.

    The public interface is identical to SQLiteStore so it is a drop-in
    replacement everywhere Vision calls get_db().
    """

    def __init__(self, sqlite: SQLiteStore, nedb: NedbStore) -> None:
        self._sq  = sqlite
        self._nd  = nedb
        self._fail_count = 0

    # ── internal helpers ─────────────────────────────────────────────────────

    def _fire(self, coro) -> None:
        """Schedule a nedbd coroutine as a fire-and-forget task.

        Failures are logged (rate-limited) but never propagate to the caller.
        """
        async def _wrap():
            try:
                await coro
            except Exception as e:
                self._fail_count += 1
                if self._fail_count % _WARN_EVERY == 1:
                    logger.warning(
                        "nedbd write failed (count=%d, suppressing further until next %d): %s",
                        self._fail_count, _WARN_EVERY, e,
                    )

        try:
            loop = asyncio.get_running_loop()
            loop.create_task(_wrap())
        except RuntimeError:
            pass  # no running loop — skip nedbd, SQLite already written

    async def _nd_get(self, coro) -> Optional[str]:
        """Run a nedbd read, return None on any failure."""
        try:
            return await coro
        except Exception:
            return None

    # ── key / value ──────────────────────────────────────────────────────────

    async def get(self, key: str) -> Optional[str]:
        val = await self._nd_get(self._nd.get(key))
        if val is not None:
            return val
        return await self._sq.get(key)

    async def set(self, key: str, value: Any, ex: Optional[int] = None) -> None:
        await self._sq.set(key, value, ex)
        self._fire(self._nd.set(key, value, ex))

    async def delete(self, *keys: str) -> int:
        count = await self._sq.delete(*keys)
        self._fire(self._nd.delete(*keys))
        return count

    async def incr(self, key: str) -> int:
        val = await self._sq.incr(key)
        self._fire(self._nd.set(key, str(val)))
        return val

    async def expire(self, key: str, seconds: int) -> None:
        await self._sq.expire(key, seconds)
        self._fire(self._nd.expire(key, seconds))

    # ── sorted sets ──────────────────────────────────────────────────────────

    async def zadd(self, name: str, mapping: dict) -> int:
        count = await self._sq.zadd(name, mapping)
        self._fire(self._nd.zadd(name, mapping))
        return count

    async def zrevrange(
        self, name: str, start: int, stop: int, withscores: bool = False
    ) -> list:
        result = await self._nd_get(self._nd.zrevrange(name, start, stop, withscores))
        if result is not None:
            return result
        return await self._sq.zrevrange(name, start, stop, withscores)

    async def zremrangebyrank(self, name: str, start: int, stop: int) -> int:
        count = await self._sq.zremrangebyrank(name, start, stop)
        self._fire(self._nd.zremrangebyrank(name, start, stop))
        return count

    # ── sets ─────────────────────────────────────────────────────────────────

    async def sadd(self, name: str, *members: str) -> int:
        count = await self._sq.sadd(name, *members)
        self._fire(self._nd.sadd(name, *members))
        return count

    async def smembers(self, name: str) -> set:
        result = await self._nd_get(self._nd.smembers(name))
        if result is not None:
            return result
        return await self._sq.smembers(name)

    async def srem(self, name: str, *members: str) -> int:
        count = await self._sq.srem(name, *members)
        self._fire(self._nd.srem(name, *members))
        return count

    # ── domain methods — delegate entirely to SQLite ─────────────────────────
    # These use relational joins / domain-specific tables that live only in
    # SQLite. nedbd is not involved. All callers remain unchanged.

    async def utxo_add_batch(self, rows: Sequence[Tuple[str, int, str, int, int]]) -> None:
        return await self._sq.utxo_add_batch(rows)

    async def utxo_spend_batch(self, outpoints: Sequence[Tuple[str, int]]) -> List[Tuple[str, int, str, int, int]]:
        return await self._sq.utxo_spend_batch(outpoints)

    async def address_tx_add_batch(self, *args, **kwargs) -> None:
        return await self._sq.address_tx_add_batch(*args, **kwargs)

    async def commit(self) -> None:
        return await self._sq.commit()

    async def address_balance(self, address: str) -> Tuple[int, int]:
        return await self._sq.address_balance(address)

    async def address_tx_count(self, address: str) -> int:
        return await self._sq.address_tx_count(address)

    async def address_first_last_height(self, address: str) -> Tuple[Optional[int], Optional[int]]:
        return await self._sq.address_first_last_height(address)

    async def address_txs(self, *args, **kwargs) -> list:
        return await self._sq.address_txs(*args, **kwargs)

    async def address_utxos(self, address: str) -> list:
        return await self._sq.address_utxos(address)

    async def address_index_rollback(self, from_height: int) -> None:
        return await self._sq.address_index_rollback(from_height)

    async def address_index_last_height(self) -> int:
        return await self._sq.address_index_last_height()

    async def coinbase_reward_scan(self, *args, **kwargs):
        return await self._sq.coinbase_reward_scan(*args, **kwargs)

    async def pool_create(self, data: dict) -> int:
        return await self._sq.pool_create(data)

    async def pool_update(self, pool_id: int, data: dict) -> bool:
        return await self._sq.pool_update(pool_id, data)

    async def pool_get(self, pool_id: int) -> Optional[dict]:
        return await self._sq.pool_get(pool_id)

    async def pool_find_by_payout_address(self, *args, **kwargs) -> Optional[dict]:
        return await self._sq.pool_find_by_payout_address(*args, **kwargs)

    async def pool_list(self, *args, **kwargs) -> list:
        return await self._sq.pool_list(*args, **kwargs)

    async def snapshot_create(self, data: dict) -> int:
        return await self._sq.snapshot_create(data)

    async def snapshot_create_guarded(self, data: dict) -> Optional[int]:
        return await self._sq.snapshot_create_guarded(data)

    async def snapshot_delete(self, snapshot_id: int) -> bool:
        return await self._sq.snapshot_delete(snapshot_id)

    async def snapshots_delete_by_status(self, statuses: Sequence[str]) -> int:
        return await self._sq.snapshots_delete_by_status(statuses)

    async def snapshot_get(self, snapshot_id: int) -> Optional[dict]:
        return await self._sq.snapshot_get(snapshot_id)

    async def snapshot_list(self, *args, **kwargs) -> list:
        return await self._sq.snapshot_list(*args, **kwargs)

    async def snapshot_set_totals(self, *args, **kwargs) -> None:
        return await self._sq.snapshot_set_totals(*args, **kwargs)

    async def snapshot_set_status(self, *args, **kwargs) -> bool:
        return await self._sq.snapshot_set_status(*args, **kwargs)

    async def snapshot_range_exists(self, start_height: int, end_height: int) -> bool:
        return await self._sq.snapshot_range_exists(start_height, end_height)
