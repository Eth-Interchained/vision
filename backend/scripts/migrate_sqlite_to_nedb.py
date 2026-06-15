#!/usr/bin/env python3
"""migrate_sqlite_to_nedb.py — optimized, resumable SQLite → nedbd migrator.

Streams SQLite rows via LIMIT/OFFSET so peak memory is proportional to
--chunk, not total rows. Sends nedbd batches concurrently (asyncio semaphore).
Persists a state file after every chunk so interrupted runs resume within one
chunk of where they stopped. Verifies actual nedbd collection counts at
startup and advances past data already present (survives partial prior runs).

Usage
-----
  # Full migration (auto-resumes if interrupted):
  python migrate_sqlite_to_nedb.py --sqlite ../data/vision.db

  # Skip the ~1.2M block-cache rows — only migrate live state:
  python migrate_sqlite_to_nedb.py --sqlite ../data/vision.db --skip-block-cache

  # Dry run (counts rows, no writes):
  python migrate_sqlite_to_nedb.py --sqlite ../data/vision.db --dry-run

  # Reset saved progress and start fresh:
  python migrate_sqlite_to_nedb.py --sqlite ../data/vision.db --reset

  # Tune for a slow/encrypted nedbd:
  python migrate_sqlite_to_nedb.py --concurrency 2 --batch-size 50

Environment variables (override CLI defaults):
  NEDB_URL, NEDB_DB_NAME, NEDBD_TOKEN
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Callable, Optional

try:
    import aiosqlite
    import httpx
except ImportError:
    print("ERROR: aiosqlite and httpx are required.")
    print("  pip install aiosqlite httpx")
    sys.exit(1)

__version__ = "2.0.0"

# ---------------------------------------------------------------------------
# State file (resume)
# ---------------------------------------------------------------------------

def _load_state(path: Path) -> dict:
    try:
        return json.loads(path.read_text())
    except Exception:
        return {"kv_done": 0, "zsets_done": 0, "sets_done": 0}


def _save_state(path: Path, state: dict) -> None:
    """Atomic write via temp file + rename."""
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2))
    tmp.rename(path)


# ---------------------------------------------------------------------------
# nedbd helpers
# ---------------------------------------------------------------------------

async def _nedb_health(client: httpx.AsyncClient, base: str) -> dict:
    r = await client.get(f"{base}/health")
    r.raise_for_status()
    return r.json()


async def _ensure_db(client: httpx.AsyncClient, base: str, db: str) -> None:
    for attempt in range(1, 4):
        try:
            r = await client.get(f"{base}/v1/databases/{db}")
            if r.status_code == 200:
                return
            if r.status_code == 404:
                cr = await client.post(f"{base}/v1/databases", json={"name": db})
                cr.raise_for_status()
                return
        except (httpx.TimeoutException, httpx.ConnectError) as e:
            if attempt < 3:
                print(f"  ensure_db attempt {attempt}/3 failed ({e}), retrying in 5s…")
                await asyncio.sleep(5)
            else:
                raise


async def _count_collection(
    client: httpx.AsyncClient, base: str, db: str, coll: str
) -> int:
    try:
        r = await client.post(
            f"{base}/v1/databases/{db}/query",
            json={"nql": f"FROM {coll} LIMIT 9999999"},
            timeout=120.0,
        )
        if r.status_code == 200:
            return int(r.json().get("count", 0))
    except Exception:
        pass
    return 0


async def _send_batch(
    client: httpx.AsyncClient,
    base: str,
    db: str,
    ops: list,
    dry_run: bool = False,
) -> int:
    if dry_run:
        return len(ops)

    delay = 0.5
    last_err = ""
    for attempt in range(1, 5):
        try:
            r = await client.post(
                f"{base}/v1/databases/{db}/batch",
                json={"ops": ops},
                timeout=120.0,
            )
            if r.status_code == 200:
                return int(r.json().get("count", 0))
            last_err = f"HTTP {r.status_code}"
        except (httpx.TimeoutException, httpx.ConnectError) as e:
            last_err = str(e)

        if attempt < 4:
            print(f"\n  batch retry {attempt}/4 ({last_err}), waiting {delay:.1f}s…",
                  flush=True)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 8.0)

    raise RuntimeError(f"batch failed after 4 attempts: {last_err}")


# ---------------------------------------------------------------------------
# Progress bar (stdlib only)
# ---------------------------------------------------------------------------

class _Bar:
    def __init__(self, label: str, total: int, start: int = 0) -> None:
        self.label  = label
        self.total  = max(total, 1)
        self.done   = start
        self._t0    = time.time()
        self._drawn = 0

    def update(self, n: int = 0) -> None:
        self.done += n
        elapsed = max(time.time() - self._t0, 0.001)
        rate    = self.done / elapsed
        pct     = self.done / self.total * 100
        remain  = max(self.total - self.done, 0)
        eta     = f"{remain / max(rate, 1):.0f}s" if self.done > 0 else "?"
        bar_w   = 38
        filled  = int(bar_w * self.done / self.total)
        bar     = "█" * filled + "░" * (bar_w - filled)
        line    = (
            f"\r  {self.label:<6} [{bar}] "
            f"{self.done:>9,}/{self.total:>9,}  "
            f"{rate:>8,.0f}/s  eta {eta}   "
        )
        print(line, end="", flush=True)
        self._drawn += 1

    def finish(self) -> None:
        self.update(0)
        print(flush=True)


# ---------------------------------------------------------------------------
# Core: stream one table
# ---------------------------------------------------------------------------

async def stream_table(
    *,
    label:        str,
    db_path:      Path,
    fetch_sql:    str,
    total:        int,
    start_offset: int,
    to_op:        Callable,
    state:        dict,
    state_key:    str,
    state_file:   Path,
    client:       httpx.AsyncClient,
    base:         str,
    db:           str,
    chunk:        int,
    batch_size:   int,
    concurrency:  int,
    dry_run:      bool,
) -> int:
    if start_offset >= total:
        print(f"  {label}: already done ({start_offset:,}/{total:,})")
        return 0

    sem    = asyncio.Semaphore(concurrency)
    bar    = _Bar(label, total, start_offset)
    sent   = 0
    offset = start_offset

    async with aiosqlite.connect(str(db_path)) as conn:
        conn.row_factory = aiosqlite.Row

        while offset < total:
            async with conn.execute(fetch_sql, (chunk, offset)) as cur:
                raw_rows = await cur.fetchall()

            if not raw_rows:
                offset += chunk
                continue

            ops = [op for row in raw_rows if (op := to_op(row)) is not None]
            chunk_len = len(raw_rows)

            if ops:
                tasks = []
                for i in range(0, len(ops), batch_size):
                    batch = ops[i : i + batch_size]

                    async def _send(b: list = batch) -> int:
                        async with sem:
                            return await _send_batch(client, base, db, b, dry_run)

                    tasks.append(asyncio.create_task(_send()))

                results = await asyncio.gather(*tasks, return_exceptions=True)
                for r in results:
                    if isinstance(r, Exception):
                        raise r
                    sent += r
                    bar.update(r)

            offset += chunk_len
            state[state_key] = offset
            if not dry_run:
                _save_state(state_file, state)

    bar.finish()
    return sent


# ---------------------------------------------------------------------------
# Row → nedbd op converters
# ---------------------------------------------------------------------------

def _make_kv_op(skip_block_cache: bool) -> Callable:
    now = time.time()

    def _op(row: aiosqlite.Row) -> Optional[dict]:
        key        = row["key"]
        value      = row["value"]
        expires_at = row["expires_at"]
        if expires_at is not None and expires_at < now:
            return None
        if skip_block_cache and (
            key.startswith("vision:block:height:")
            or key.startswith("vision:block:hash:")
        ):
            return None
        return {
            "op": "put", "coll": "kv", "id": key,
            "doc": {"_id": key, "value": value, "expires_at": expires_at},
        }

    return _op


def _zset_op(row: aiosqlite.Row) -> dict:
    name, member, score = row["name"], row["member"], row["score"]
    doc_id = f"{name}::{member}"
    return {
        "op": "put", "coll": "zset", "id": doc_id,
        "doc": {"_id": doc_id, "_name": name, "_member": member, "score": score},
    }


def _set_op(row: aiosqlite.Row) -> dict:
    name, member = row["name"], row["member"]
    doc_id = f"{name}::{member}"
    return {
        "op": "put", "coll": "set", "id": doc_id,
        "doc": {"_id": doc_id, "_name": name, "_member": member},
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def _run(args: argparse.Namespace) -> int:
    base = args.nedb_url.rstrip("/")
    db   = args.db

    print(f"\nnedb-migrator v{__version__}  —  SQLite → nedbd  (streaming async)\n")
    print(f"  sqlite            {args.sqlite}")
    print(f"  nedbd             {base}")
    print(f"  database          {db}")
    print(f"  chunk             {args.chunk:,}")
    print(f"  concurrency       {args.concurrency}")
    print(f"  batch-size        {args.batch_size}")
    print(f"  skip-block-cache  {args.skip_block_cache}")
    print(f"  dry-run           {args.dry_run}")
    print(f"  state file        {args.state_file}\n")

    # ── State ────────────────────────────────────────────────────────────────
    state_file = Path(args.state_file)
    if args.reset and state_file.exists():
        state_file.unlink()
        print("↺ State reset.\n")

    state = _load_state(state_file)
    if any(state.get(k, 0) for k in ("kv_done", "zsets_done", "sets_done")):
        print(f"→ Resuming — kv={state['kv_done']:,} "
              f"zsets={state['zsets_done']:,} sets={state['sets_done']:,}\n")

    # ── Count rows ───────────────────────────────────────────────────────────
    sqlite_path = Path(args.sqlite)
    if not sqlite_path.exists():
        print(f"ERROR: SQLite not found: {sqlite_path}")
        return 1

    async with aiosqlite.connect(str(sqlite_path)) as conn:
        async with conn.execute("SELECT COUNT(*) FROM kv")    as c: kv_total    = (await c.fetchone())[0]
        async with conn.execute("SELECT COUNT(*) FROM zsets") as c: zsets_total = (await c.fetchone())[0]
        async with conn.execute("SELECT COUNT(*) FROM sets")  as c: sets_total  = (await c.fetchone())[0]

    print(f"◉ Rows in SQLite — kv={kv_total:,}  zsets={zsets_total:,}  sets={sets_total:,}\n")

    # ── nedbd ────────────────────────────────────────────────────────────────
    headers: dict = {}
    if args.token:
        headers["Authorization"] = f"Bearer {args.token}"

    async with httpx.AsyncClient(headers=headers, timeout=120.0) as client:
        if not args.dry_run:
            try:
                h = await _nedb_health(client, base)
                print(f"✓ nedbd OK  version={h.get('version')}  "
                      f"encrypted={h.get('encrypted')}\n")
            except Exception as e:
                print(f"ERROR: Cannot reach nedbd at {base}: {e}")
                return 1

            if not args.no_verify:
                await _ensure_db(client, base, db)
                print("◉ Verifying against nedbd…", end="  ", flush=True)
                kv_n   = await _count_collection(client, base, db, "kv")
                zset_n = await _count_collection(client, base, db, "zset")
                set_n  = await _count_collection(client, base, db, "set")
                print(f"kv={kv_n:,} zset={zset_n:,} set={set_n:,}")

                advanced = False
                for key, nedb_n, total, lbl in [
                    ("kv_done",    kv_n,   kv_total,    "kv"),
                    ("zsets_done", zset_n, zsets_total, "zsets"),
                    ("sets_done",  set_n,  sets_total,  "sets"),
                ]:
                    cur = state.get(key, 0)
                    if nedb_n >= total:
                        print(f"  ✓ {lbl}: all {total:,} rows already in nedbd")
                        state[key] = total
                        advanced = True
                    elif nedb_n > cur:
                        print(f"  ↑ {lbl}: state={cur:,} → nedbd={nedb_n:,} (advancing)")
                        state[key] = nedb_n
                        advanced = True

                if advanced:
                    _save_state(state_file, state)
                    print("  ✓ State synced from nedbd.\n")
                else:
                    print("  ✓ Consistent.\n")
        else:
            print("⚠ Dry-run — skipping nedbd check\n")

        t0   = time.time()
        skip = args.skip_block_cache

        kv_start    = state.get("kv_done",    0)
        zsets_start = state.get("zsets_done", 0)
        sets_start  = state.get("sets_done",  0)

        kv_sent = await stream_table(
            label="kv",
            db_path=sqlite_path,
            fetch_sql="SELECT key, value, expires_at FROM kv ORDER BY rowid LIMIT ? OFFSET ?",
            total=kv_total,
            start_offset=kv_start,
            to_op=_make_kv_op(skip),
            state=state, state_key="kv_done", state_file=state_file,
            client=client, base=base, db=db,
            chunk=args.chunk, batch_size=args.batch_size,
            concurrency=args.concurrency, dry_run=args.dry_run,
        )

        zsets_sent = await stream_table(
            label="zsets",
            db_path=sqlite_path,
            fetch_sql="SELECT name, member, score FROM zsets ORDER BY rowid LIMIT ? OFFSET ?",
            total=zsets_total,
            start_offset=zsets_start,
            to_op=_zset_op,
            state=state, state_key="zsets_done", state_file=state_file,
            client=client, base=base, db=db,
            chunk=args.chunk, batch_size=args.batch_size,
            concurrency=args.concurrency, dry_run=args.dry_run,
        )

        sets_sent = await stream_table(
            label="sets",
            db_path=sqlite_path,
            fetch_sql="SELECT name, member FROM sets ORDER BY rowid LIMIT ? OFFSET ?",
            total=sets_total,
            start_offset=sets_start,
            to_op=_set_op,
            state=state, state_key="sets_done", state_file=state_file,
            client=client, base=base, db=db,
            chunk=args.chunk, batch_size=args.batch_size,
            concurrency=args.concurrency, dry_run=args.dry_run,
        )

    elapsed = time.time() - t0
    total   = kv_sent + zsets_sent + sets_sent
    rps     = total / max(elapsed, 0.001)

    print("\n" + "─" * 52)
    print(" Migration complete " if not args.dry_run else " DRY-RUN summary ")
    print("─" * 52)
    print(f"  kv sent:     {kv_sent:>10,}")
    if kv_start:
        print(f"  kv skipped:  {kv_start:>10,}  (already in nedbd)")
    print(f"  zsets sent:  {zsets_sent:>10,}")
    print(f"  sets sent:   {sets_sent:>10,}")
    print(f"  total:       {total:>10,}")
    print(f"  elapsed:     {elapsed:>9.1f}s  ({rps:,.0f} rows/s)")
    if not args.dry_run and total > 0:
        print(f"\n✓ State → {state_file}")
    if total == 0:
        print("\n✓ Nothing new — already migrated. Use --reset to start over.")
    print()
    return 0


def main() -> None:
    p = argparse.ArgumentParser(
        description="Optimized, resumable SQLite → nedbd migrator (v2)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--sqlite",           default=os.getenv("SQLITE_PATH", "../data/vision.db"))
    p.add_argument("--nedb-url",         default=os.getenv("NEDB_URL", "http://127.0.0.1:7070"))
    p.add_argument("--db",               default=os.getenv("NEDB_DB_NAME", "vision"))
    p.add_argument("--token",            default=os.getenv("NEDBD_TOKEN", ""))
    p.add_argument("--chunk",            type=int, default=2000,
                   help="rows fetched from SQLite per pass (controls peak memory)")
    p.add_argument("--concurrency",      type=int, default=4,
                   help="concurrent nedbd batch requests (lower for encrypted DBs)")
    p.add_argument("--batch-size",       type=int, default=50,
                   help="rows per nedbd batch request")
    p.add_argument("--skip-block-cache", action="store_true",
                   help="skip vision:block:height:* and vision:block:hash:* rows")
    p.add_argument("--state-file",       default=".nedb-migrator-state.json")
    p.add_argument("--reset",            action="store_true",
                   help="delete state file and start from scratch")
    p.add_argument("--no-verify",        action="store_true",
                   help="skip nedbd verification pass at startup")
    p.add_argument("--dry-run",          action="store_true",
                   help="count rows, print plan, no writes")
    args = p.parse_args()
    sys.exit(asyncio.run(_run(args)))


if __name__ == "__main__":
    main()
