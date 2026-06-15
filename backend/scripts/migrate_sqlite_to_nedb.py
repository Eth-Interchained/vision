#!/usr/bin/env python3
"""One-shot migration from Vision's SQLite KV store to a nedbd database.

Reads every row from the ``kv``, ``zsets`` and ``sets`` tables in the source
SQLite file and PUTs them into nedbd via the batch API. PUT is idempotent
(it's an upsert), so this script can be re-run safely; the only side effect
is incrementing the destination database's ``seq``.

Usage
-----
    python migrate_sqlite_to_nedb.py \
        --sqlite ../data/vision.db \
        --nedb-url http://127.0.0.1:7070 \
        --db vision

    # Add --token if your nedbd is auth-gated.
    # Add --dry-run to preview without writing.
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
import time
from typing import Iterable, List, Optional

import httpx


BATCH_SIZE = 100


def _build_client(base_url: str, token: str) -> httpx.Client:
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return httpx.Client(base_url=base_url.rstrip("/"), headers=headers, timeout=60.0)


def _send_batch(client: httpx.Client, db: str, ops: List[dict], dry_run: bool) -> None:
    if not ops:
        return
    if dry_run:
        return
    resp = client.post(f"/v1/databases/{db}/batch", json={"ops": ops})
    resp.raise_for_status()


def _ensure_database(client: httpx.Client, db: str, dry_run: bool) -> None:
    """Best-effort: create the database if it doesn't exist yet.

    nedbd's ``POST /v1/databases`` is idempotent for our purposes — if the
    database already exists we treat the resulting 4xx as a no-op so the
    migration can proceed.
    """
    if dry_run:
        return
    try:
        client.post("/v1/databases", json={"name": db})
    except Exception as e:
        # Not fatal — the put/batch calls will surface a real error if the
        # database really is missing.
        print(f"  ! ensure_database({db}) raised {e!r} — continuing", file=sys.stderr)


def _chunked(iterable: Iterable[dict], n: int) -> Iterable[List[dict]]:
    buf: List[dict] = []
    for item in iterable:
        buf.append(item)
        if len(buf) >= n:
            yield buf
            buf = []
    if buf:
        yield buf


def migrate_kv(
    conn: sqlite3.Connection, client: httpx.Client, db: str, *, dry_run: bool
) -> int:
    """Migrate the ``kv`` table. Returns the number of rows written.

    Expired rows (``expires_at < now``) are skipped — they're effectively
    dead from a reader's perspective and porting them would just consume
    seq numbers and disk space on the destination.
    """
    now = time.time()
    cur = conn.execute(
        "SELECT key, value, expires_at FROM kv WHERE expires_at IS NULL OR expires_at > ?",
        (now,),
    )
    written = 0
    skipped = 0

    def _iter_ops():
        for key, value, expires_at in cur:
            doc = {"_id": key, "value": str(value), "expires_at": expires_at}
            yield {"op": "put", "coll": "kv", "id": key, "doc": doc}

    for batch in _chunked(_iter_ops(), BATCH_SIZE):
        _send_batch(client, db, batch, dry_run)
        written += len(batch)
        if written % (BATCH_SIZE * 10) == 0:
            print(f"  kv: {written} rows…")

    # Count expired rows (informational only).
    cur2 = conn.execute(
        "SELECT COUNT(*) FROM kv WHERE expires_at IS NOT NULL AND expires_at <= ?",
        (now,),
    )
    row = cur2.fetchone()
    if row:
        skipped = int(row[0] or 0)
    if skipped:
        print(f"  kv: skipped {skipped} expired rows")

    return written


def migrate_zsets(
    conn: sqlite3.Connection, client: httpx.Client, db: str, *, dry_run: bool
) -> int:
    """Migrate the ``zsets`` table → nedbd ``zset`` collection."""
    cur = conn.execute("SELECT name, member, score FROM zsets")
    written = 0

    def _iter_ops():
        for name, member, score in cur:
            doc_id = f"{name}::{member}"
            doc = {
                "_id": doc_id,
                "_name": name,
                "_member": str(member),
                "score": float(score),
            }
            yield {"op": "put", "coll": "zset", "id": doc_id, "doc": doc}

    for batch in _chunked(_iter_ops(), BATCH_SIZE):
        _send_batch(client, db, batch, dry_run)
        written += len(batch)
        if written % (BATCH_SIZE * 10) == 0:
            print(f"  zsets: {written} rows…")

    return written


def migrate_sets(
    conn: sqlite3.Connection, client: httpx.Client, db: str, *, dry_run: bool
) -> int:
    """Migrate the ``sets`` table → nedbd ``set`` collection."""
    cur = conn.execute("SELECT name, member FROM sets")
    written = 0

    def _iter_ops():
        for name, member in cur:
            doc_id = f"{name}::{member}"
            doc = {"_id": doc_id, "_name": name, "_member": str(member)}
            yield {"op": "put", "coll": "set", "id": doc_id, "doc": doc}

    for batch in _chunked(_iter_ops(), BATCH_SIZE):
        _send_batch(client, db, batch, dry_run)
        written += len(batch)
        if written % (BATCH_SIZE * 10) == 0:
            print(f"  sets: {written} rows…")

    return written


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Migrate Vision SQLite KV → nedbd")
    p.add_argument("--sqlite", required=True, help="Path to vision.db")
    p.add_argument("--nedb-url", required=True, help="nedbd base URL, e.g. http://127.0.0.1:7070")
    p.add_argument("--db", default="vision", help="Destination database name in nedbd")
    p.add_argument("--token", default="", help="Bearer token for nedbd auth (if set)")
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Read SQLite and report counts without writing to nedbd",
    )
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    print("─" * 60)
    print("Vision SQLite → nedbd migration")
    print("─" * 60)
    print(f"  source : {args.sqlite}")
    print(f"  target : {args.nedb_url}  (database={args.db})")
    print(f"  mode   : {'DRY RUN — no writes' if args.dry_run else 'WRITE'}")
    print()

    conn = sqlite3.connect(args.sqlite)
    try:
        client = _build_client(args.nedb_url, args.token)
        try:
            _ensure_database(client, args.db, args.dry_run)

            t0 = time.time()
            kv_count = migrate_kv(conn, client, args.db, dry_run=args.dry_run)
            zset_count = migrate_zsets(conn, client, args.db, dry_run=args.dry_run)
            set_count = migrate_sets(conn, client, args.db, dry_run=args.dry_run)
            dt = time.time() - t0

            print()
            print("─" * 60)
            print("Migration summary")
            print("─" * 60)
            verb = "would migrate" if args.dry_run else "migrated"
            print(f"  kv    : {verb} {kv_count} rows")
            print(f"  zsets : {verb} {zset_count} rows")
            print(f"  sets  : {verb} {set_count} rows")
            print(f"  total : {kv_count + zset_count + set_count} rows in {dt:.2f}s")
            if args.dry_run:
                print("  (dry run — no data written)")
            return 0
        finally:
            client.close()
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
