"""nedb_backfill.py — Generic bi-directional block backfill into nedbd.

Walks backwards from the current chain tip to genesis, writing lean block
header documents into a nedbd collection.  Simultaneously, the DualStore
writes new blocks going forward — both directions run concurrently.

Design
------
The task is SQLite-agnostic.  It depends only on a ``BlockSource`` — an
async callable ``(height: int) -> Optional[dict]`` that returns a lean
block header.  Vision wires two concrete sources:

  SqliteBlockSource   reads from Vision's existing SQLite KV block cache
                      (fast, no network round-trip).
  RpcBlockSource      falls back to the ITC node JSON-RPC for blocks not
                      yet in the SQLite cache.

Any other source (Postgres, another HTTP API, etc.) can be plugged in by
implementing the same one-method protocol.

Block header schema (what gets written to nedbd)
------------------------------------------------
{
  "height":     int,
  "hash":       str,
  "prev_hash":  str | None,
  "timestamp":  int,
  "n_tx":       int,
  "difficulty": float,
  "size":       int,
  "weight":     int | None,
}

Cursor persistence
------------------
The backfill cursor is stored in nedbd itself so restarts resume without
re-processing:

  collection ``_backfill``, doc id ``blocks``
  { "lowest_done": int, "highest_done": int, "total": int }

Usage (from main.py lifespan)
------------------------------
  task = NedbBackfillTask(
      nedb=nedb_store.get_db(),
      db=settings.NEDB_DB_NAME,
      sources=[SqliteBlockSource(get_db()), RpcBlockSource(get_rpc())],
  )
  await task.start()   # non-blocking, fires asyncio task
  await task.stop()    # graceful shutdown
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, Callable, Coroutine, List, Optional, Protocol

logger = logging.getLogger(__name__)

# ── Block header schema ─────────────────────────────────────────────────────

LEAN_FIELDS = ("height", "hash", "prev_hash", "timestamp", "n_tx",
               "difficulty", "size", "weight")


def _lean(block: dict) -> dict:
    """Strip a full block dict to the lean header fields only."""
    return {k: block.get(k) for k in LEAN_FIELDS if block.get(k) is not None}


# ── BlockSource protocol ────────────────────────────────────────────────────

class BlockSource(Protocol):
    """A single-method protocol for fetching a block header by height.

    Any object with an async ``get(height)`` method qualifies — no base
    class required.
    """
    async def get(self, height: int) -> Optional[dict]: ...

    @property
    def name(self) -> str: ...


# ── Concrete sources ─────────────────────────────────────────────────────────

class SqliteBlockSource:
    """Read lean block headers from Vision's existing SQLite KV block cache.

    Reads ``vision:block:hash:{height}`` → hash, then
    ``vision:block:hash:{hash}`` → full block JSON → strips to lean header.
    Falls back to None if the block is not in the cache.
    """
    name = "sqlite"

    def __init__(self, sqlite_store) -> None:
        self._sq = sqlite_store

    async def get(self, height: int) -> Optional[dict]:
        from ..sqlite_store import Keys
        try:
            bhash = await self._sq.get(Keys.BLOCK_BY_HEIGHT.format(height=height))
            if not bhash:
                return None
            raw = await self._sq.get(Keys.BLOCK_BY_HASH.format(hash=bhash))
            if not raw:
                return None
            block = json.loads(raw)
            header = _lean(block)
            # Normalise field names from Vision's cached shape
            if "n_tx" not in header and "nTx" in block:
                header["n_tx"] = block["nTx"]
            if "prev_hash" not in header and "previousblockhash" in block:
                header["prev_hash"] = block["previousblockhash"]
            if "height" not in header:
                header["height"] = height
            if "hash" not in header and bhash:
                header["hash"] = bhash
            return header if header.get("hash") else None
        except Exception:
            return None


class RpcBlockSource:
    """Fetch lean block headers directly from the ITC JSON-RPC node.

    Used as a fallback when SQLite doesn't have the block cached, or as
    the primary source when SQLite is not available.
    """
    name = "rpc"

    def __init__(self, rpc_client) -> None:
        self._rpc = rpc_client

    async def get(self, height: int) -> Optional[dict]:
        try:
            from ..rpc.methods import BlockchainRPC
            rpc = BlockchainRPC(self._rpc)
            bhash = await rpc.get_block_hash(height)
            block = await rpc.get_block(bhash, verbosity=1)
            return {
                "height":     height,
                "hash":       bhash,
                "prev_hash":  block.get("previousblockhash"),
                "timestamp":  block.get("time", 0),
                "n_tx":       block.get("nTx", 0),
                "difficulty": float(block.get("difficulty", 0.0)),
                "size":       block.get("size", 0),
                "weight":     block.get("weight"),
            }
        except Exception:
            return None


# ── NedbBackfillTask ─────────────────────────────────────────────────────────

class NedbBackfillTask:
    """Walks backwards from tip to genesis, writing lean block headers to nedbd.

    The DualStore simultaneously writes new blocks going forward — both
    directions run concurrently as independent asyncio tasks.

    Parameters
    ----------
    nedb        NedbStore instance (or any object with _put / _query)
    db          nedbd database name
    sources     Ordered list of BlockSource implementations; tried in order
                per block (first hit wins).
    batch_size  Blocks written per tick (default 50).
    sleep_ms    Milliseconds to sleep between batches (default 200ms —
                yields to the event loop and avoids write pressure).
    collection  nedbd collection name for block headers (default "blocks").
    """

    CURSOR_COLL = "_backfill"
    CURSOR_ID   = "blocks"

    def __init__(
        self,
        nedb,
        db: str,
        sources: List[BlockSource],
        *,
        batch_size: int = 50,
        sleep_ms:   int = 200,
        collection: str = "blocks",
    ) -> None:
        self._nd         = nedb
        self._db         = db
        self._sources    = sources
        self._batch_size = batch_size
        self._sleep_s    = sleep_ms / 1000.0
        self._collection = collection
        self._task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()

        # Status tracking (readable via /api/nedb/backfill-status)
        self.lowest_done:  int = 0
        self.highest_done: int = 0
        self.blocks_written: int = 0
        self.errors:        int = 0
        self.running:       bool = False
        self.complete:      bool = False

    # ── Public API ───────────────────────────────────────────────────────────

    async def start(self) -> None:
        """Start the backfill in the background."""
        if self._task and not self._task.done():
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run())
        self._task.add_done_callback(self._on_done)
        logger.info("NedbBackfillTask started — db=%s col=%s batch=%d sleep=%.3fs",
                    self._db, self._collection, self._batch_size, self._sleep_s)

    async def stop(self) -> None:
        """Signal the backfill to stop after the current batch."""
        self._stop_event.set()
        if self._task:
            try:
                await asyncio.wait_for(self._task, timeout=10)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self._task.cancel()

    def status(self) -> dict:
        return {
            "running":       self.running,
            "complete":      self.complete,
            "lowest_done":   self.lowest_done,
            "highest_done":  self.highest_done,
            "blocks_written": self.blocks_written,
            "errors":        self.errors,
        }

    # ── Internal ─────────────────────────────────────────────────────────────

    def _on_done(self, task: asyncio.Task) -> None:
        self.running = False
        if task.cancelled():
            logger.info("NedbBackfillTask cancelled")
        elif task.exception():
            logger.error("NedbBackfillTask crashed: %s", task.exception())
        else:
            logger.info("NedbBackfillTask complete — %d blocks written", self.blocks_written)

    async def _load_cursor(self) -> dict:
        """Load the persisted cursor from nedbd, or return a fresh one."""
        try:
            rows = await self._nd._query(
                f'FROM {self.CURSOR_COLL} WHERE _id = "{self.CURSOR_ID}" LIMIT 1'
            )
            if rows.get("rows"):
                return rows["rows"][0]
        except Exception:
            pass
        return {"lowest_done": -1, "highest_done": -1, "total": 0}

    async def _save_cursor(self) -> None:
        try:
            await self._nd._put(
                self.CURSOR_COLL,
                self.CURSOR_ID,
                {
                    "lowest_done":  self.lowest_done,
                    "highest_done": self.highest_done,
                    "total":        self.blocks_written,
                },
            )
        except Exception:
            pass

    async def _fetch_block(self, height: int) -> Optional[dict]:
        """Try each source in order; return first hit."""
        for source in self._sources:
            try:
                block = await source.get(height)
                if block:
                    return block
            except Exception:
                continue
        return None

    async def _write_block(self, block: dict) -> bool:
        height = block.get("height", 0)
        doc_id = str(height)
        # Causal link: each block's caused_by is its parent
        caused_by = str(height - 1) if height > 0 else None
        try:
            await self._nd._put(
                self._collection,
                doc_id,
                block,
                caused_by=caused_by,
            )
            return True
        except Exception as e:
            logger.debug("backfill write h=%d failed: %s", height, e)
            self.errors += 1
            return False

    async def _get_tip_height(self) -> int:
        """Get current tip from nedbd KV store, fallback to 0."""
        try:
            rows = await self._nd._query(
                'FROM kv WHERE _id = "vision:tip:height" LIMIT 1'
            )
            if rows.get("rows"):
                return int(rows["rows"][0].get("value", 0))
        except Exception:
            pass
        # Try SQLite source directly
        for source in self._sources:
            if isinstance(source, SqliteBlockSource):
                try:
                    from ..sqlite_store import Keys
                    val = await source._sq.get(Keys.TIP_HEIGHT)
                    if val:
                        return int(val)
                except Exception:
                    pass
        return 0

    async def _run(self) -> None:
        self.running = True

        # Load persisted cursor
        cursor = await self._load_cursor()
        self.lowest_done  = cursor.get("lowest_done",  -1)
        self.highest_done = cursor.get("highest_done", -1)
        self.blocks_written = cursor.get("total",       0)

        tip = await self._get_tip_height()
        if tip == 0:
            logger.warning("NedbBackfillTask: could not determine tip height, aborting")
            return

        # If this is a fresh start, begin at tip
        if self.highest_done < 0:
            self.highest_done = tip
            self.lowest_done  = tip

        logger.info(
            "NedbBackfillTask resuming — tip=%d lowest_done=%d blocks_written=%d",
            tip, self.lowest_done, self.blocks_written,
        )

        current = self.lowest_done - 1  # next height to process (going backwards)

        while not self._stop_event.is_set() and current >= 0:
            batch_written = 0

            for h in range(current, max(current - self._batch_size, -1), -1):
                if self._stop_event.is_set():
                    break
                block = await self._fetch_block(h)
                if block:
                    if await self._write_block(block):
                        batch_written += 1
                        self.blocks_written += 1
                        self.lowest_done = h
                else:
                    # Source couldn't supply this block — skip, don't stall
                    self.errors += 1
                    self.lowest_done = h

            current = self.lowest_done - 1

            if batch_written > 0:
                await self._save_cursor()
                logger.debug(
                    "NedbBackfillTask batch: wrote %d blocks, lowest=%d, total=%d",
                    batch_written, self.lowest_done, self.blocks_written,
                )

            # Also sync the forward direction: pick up any new blocks since start
            new_tip = await self._get_tip_height()
            if new_tip > self.highest_done:
                for h in range(self.highest_done + 1, new_tip + 1):
                    if self._stop_event.is_set():
                        break
                    block = await self._fetch_block(h)
                    if block and await self._write_block(block):
                        self.blocks_written += 1
                        self.highest_done = h
                if new_tip > self.highest_done:
                    self.highest_done = new_tip

            # Yield to event loop
            await asyncio.sleep(self._sleep_s)

        self.complete = (current < 0)
        await self._save_cursor()
        logger.info(
            "NedbBackfillTask finished — complete=%s lowest=%d blocks_written=%d errors=%d",
            self.complete, self.lowest_done, self.blocks_written, self.errors,
        )


# ── Singleton management ─────────────────────────────────────────────────────

_backfill_task: Optional[NedbBackfillTask] = None


def get_backfill_task() -> Optional[NedbBackfillTask]:
    return _backfill_task


def set_backfill_task(task: NedbBackfillTask) -> None:
    global _backfill_task
    _backfill_task = task
