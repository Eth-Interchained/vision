#!/usr/bin/env python3
"""itsl_mirror.py — ITSL → NEDB sidecar mirror daemon.

Bridges an ITC Core node (JSON-RPC) to a running nedbd instance, populating
three collections that power the Vision NEDB showcase:

  itsl_ops       – every ITSL token operation, causally linked via TRACE
  reward_splits  – per-block coinbase reward split (miner / governance / operator)
  blocks         – block headers for chain-wide time-travel and fork detection

Cursor state is persisted inside nedbd itself (``_mirror`` collection in the
``itsl_mirror`` database) so restarts resume without re-processing.

Usage
-----
  # Run once (sync then exit):
  python itsl_mirror.py --once

  # Daemon mode (poll every 30s):
  python itsl_mirror.py --interval 30

  # Custom endpoints:
  python itsl_mirror.py \\
      --rpc-url http://127.0.0.1:8332 \\
      --rpc-user alice --rpc-pass s3cr3t \\
      --wallet bulk_payout_wallet \\
      --nedb-url http://127.0.0.1:7070 \\
      --nedb-token mytoken \\
      --db vision

  # Dry-run (print what would be written, no writes):
  python itsl_mirror.py --once --dry-run

Environment variables (override CLI defaults):
  ITC_RPC_URL, ITC_RPC_USER, ITC_RPC_PASS, ITC_WALLET_NAME
  NEDB_URL, NEDBD_TOKEN, NEDB_DB_NAME
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import logging
import os
import signal
import sys
import time
from http.client import HTTPConnection, HTTPSConnection
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

__version__ = "1.0.0"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("itsl_mirror")


# ---------------------------------------------------------------------------
# ITC JSON-RPC client (sync, stdlib only)
# ---------------------------------------------------------------------------

class RPCError(Exception):
    def __init__(self, code: int, message: str):
        super().__init__(f"[{code}] {message}")
        self.code = code


class ITCClient:
    """Minimal sync JSON-RPC client for interchainedd."""

    def __init__(
        self,
        url: str = "http://127.0.0.1:8332",
        user: str = "",
        password: str = "",
        wallet: str = "",
        timeout: int = 30,
    ):
        self.url = url.rstrip("/")
        self.wallet = wallet
        self.timeout = timeout
        self._auth = (
            b"Basic " + base64.b64encode(f"{user}:{password}".encode())
            if user
            else None
        )
        self._req_id = 0

    def _endpoint(self, use_wallet: bool = False) -> str:
        if use_wallet and self.wallet:
            return f"{self.url}/wallet/{self.wallet}"
        return self.url

    def call(self, method: str, params: Optional[list] = None, *, use_wallet: bool = False) -> Any:
        endpoint = self._endpoint(use_wallet)
        parsed = urlparse(endpoint)
        host = parsed.hostname or "127.0.0.1"
        port = parsed.port or (443 if parsed.scheme == "https" else 8332)
        path = parsed.path or "/"

        self._req_id += 1
        payload = json.dumps({
            "jsonrpc": "1.0",
            "id": f"mirror_{self._req_id}",
            "method": method,
            "params": params or [],
        }).encode()

        if parsed.scheme == "https":
            conn = HTTPSConnection(host, port, timeout=self.timeout)
        else:
            conn = HTTPConnection(host, port, timeout=self.timeout)

        headers: Dict[str, str] = {"Content-Type": "application/json"}
        if self._auth:
            headers["Authorization"] = self._auth.decode()

        try:
            conn.request("POST", path, payload, headers)
            resp = conn.getresponse()
            body = resp.read()
        finally:
            conn.close()

        data = json.loads(body)
        if data.get("error"):
            err = data["error"]
            raise RPCError(err.get("code", -1), err.get("message", "unknown"))
        return data.get("result")

    # --- convenience wrappers -----------------------------------------------

    def get_block_count(self) -> int:
        return int(self.call("getblockcount"))

    def get_block_hash(self, height: int) -> str:
        return self.call("getblockhash", [height])

    def get_block(self, bhash: str, verbosity: int = 2) -> dict:
        return self.call("getblock", [bhash, verbosity])

    def all_tokens(self) -> List[dict]:
        """Chain-wide token list — must NOT use wallet endpoint."""
        result = self.call("all_tokens", [], use_wallet=False)
        return result if isinstance(result, list) else []

    def token_history(self, token_id: str) -> List[dict]:
        return self.call("token_history", [token_id], use_wallet=True) or []

    def token_meta(self, token_id: str) -> dict:
        return self.call("token_meta", [token_id], use_wallet=True) or {}


# ---------------------------------------------------------------------------
# nedbd HTTP client (sync, stdlib only)
# ---------------------------------------------------------------------------

class NedbClient:
    """Minimal sync HTTP client for nedbd."""

    def __init__(
        self,
        url: str = "http://127.0.0.1:7070",
        token: str = "",
        timeout: int = 30,
    ):
        self.url = url.rstrip("/")
        self.token = token
        self.timeout = timeout

    def _req(self, method: str, path: str, body: Optional[dict] = None) -> dict:
        parsed = urlparse(self.url)
        host = parsed.hostname or "127.0.0.1"
        port = parsed.port or 7070
        full_path = f"{parsed.path}{path}"

        payload = json.dumps(body).encode() if body is not None else None
        headers: Dict[str, str] = {"Content-Type": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"

        if parsed.scheme == "https":
            conn = HTTPSConnection(host, port, timeout=self.timeout)
        else:
            conn = HTTPConnection(host, port, timeout=self.timeout)

        try:
            conn.request(method, full_path, payload, headers)
            resp = conn.getresponse()
            data = json.loads(resp.read())
        finally:
            conn.close()

        if resp.status >= 400:
            raise RuntimeError(f"nedbd HTTP {resp.status}: {data.get('error', data)}")
        return data

    def health(self) -> dict:
        return self._req("GET", "/health")

    def ensure_db(self, name: str) -> None:
        """Create the database if it doesn't exist."""
        try:
            self._req("GET", f"/v1/databases/{name}")
        except RuntimeError:
            self._req("POST", "/v1/databases", {"name": name})

    def query(self, db: str, nql: str) -> List[dict]:
        data = self._req("POST", f"/v1/databases/{db}/query", {"nql": nql})
        return data.get("rows", [])

    def put(
        self,
        db: str,
        coll: str,
        doc_id: str,
        doc: dict,
        *,
        caused_by: Optional[List[str]] = None,
        valid_from: Optional[str] = None,
        valid_to: Optional[str] = None,
        evidence: Optional[str] = None,
        confidence: Optional[float] = None,
    ) -> dict:
        payload: Dict[str, Any] = {"coll": coll, "id": doc_id, "doc": doc}
        if caused_by is not None:
            payload["caused_by"] = caused_by
        if valid_from is not None:
            payload["valid_from"] = valid_from
        if valid_to is not None:
            payload["valid_to"] = valid_to
        if evidence is not None:
            payload["evidence"] = evidence
        if confidence is not None:
            payload["confidence"] = confidence
        return self._req("POST", f"/v1/databases/{db}/put", payload)

    def batch(self, db: str, ops: List[dict]) -> dict:
        """Batch PUT/DEL operations. ops: [{op, coll, id, doc?}]"""
        return self._req("POST", f"/v1/databases/{db}/batch", {"ops": ops})

    def seq(self, db: str) -> int:
        """Current sequence number of the database."""
        data = self._req("GET", f"/v1/databases/{db}")
        return data.get("seq", 0)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_OP_NAMES = {
    0: "CREATE",
    1: "TRANSFER",
    2: "APPROVE",
    3: "TRANSFERFROM",
    4: "INCREASE_ALLOWANCE",
    5: "DECREASE_ALLOWANCE",
    6: "BURN",
    7: "MINT",
    8: "TRANSFER_OWNERSHIP",
}


def _op_name(op_int: Any) -> str:
    """Coerce op field (int or string) to a readable name."""
    try:
        return _OP_NAMES.get(int(op_int), str(op_int))
    except (TypeError, ValueError):
        return str(op_int)


def _op_hash(op: dict) -> str:
    """Stable deterministic hash for a token operation dict.

    Mirrors the C++ TokenOperationHash: serialise all fields EXCEPT
    signature + signer, then SHA-256 (we use SHA-256 instead of BLAKE2b
    for stdlib availability; the result is stored as the nedb _id prefix
    and is never compared to the node's own hash).
    """
    fields = (
        str(op.get("op", "")),
        str(op.get("from", "")),
        str(op.get("to", "")),
        str(op.get("spender", "")),
        str(op.get("token", "")),
        str(op.get("amount", 0)),
        str(op.get("name", "")),
        str(op.get("symbol", "")),
        str(op.get("decimals", 0)),
        str(op.get("timestamp", 0)),
    )
    raw = "|".join(fields).encode()
    return hashlib.sha256(raw).hexdigest()[:32]


def _ts_to_date(ts: Any) -> str:
    """Unix timestamp → ISO date string for VALID AS OF queries."""
    import datetime
    try:
        dt = datetime.datetime.utcfromtimestamp(int(ts))
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return "1970-01-01"


def _parse_coinbase_outputs(block: dict) -> Tuple[int, int, int, str, str, str]:
    """Extract (miner, governance, operator) rewards + addresses from coinbase tx.

    Returns: (miner_sats, gov_sats, op_sats, miner_addr, gov_addr, op_addr)
    ITC block reward split: output[0]=miner, output[1]=governance, output[2]=operator.
    Falls back gracefully if the coinbase has fewer outputs.
    """
    txs = block.get("tx") or []
    if not txs:
        return 0, 0, 0, "", "", ""

    cb = txs[0] if isinstance(txs[0], dict) else {}
    vouts = cb.get("vout") or []

    def _extract(idx: int) -> Tuple[int, str]:
        if idx >= len(vouts):
            return 0, ""
        vout = vouts[idx]
        # value is in ITC (float) → convert to sats
        sats = int(round(float(vout.get("value", 0)) * 1e8))
        spk = vout.get("scriptPubKey") or {}
        addr = spk.get("address") or (spk.get("addresses") or [""])[0]
        return sats, addr

    miner_sats, miner_addr = _extract(0)
    gov_sats, gov_addr = _extract(1)
    op_sats, op_addr = _extract(2)
    return miner_sats, gov_sats, op_sats, miner_addr, gov_addr, op_addr


# ---------------------------------------------------------------------------
# Cursor state (persisted in nedbd _mirror db)
# ---------------------------------------------------------------------------

MIRROR_DB = "itsl_mirror"
CURSOR_COLL = "cursors"
CURSOR_ID = "state"


def _load_cursor(nedb: NedbClient) -> dict:
    try:
        rows = nedb.query(MIRROR_DB, f'FROM {CURSOR_COLL} WHERE _id = "{CURSOR_ID}" LIMIT 1')
        if rows:
            return rows[0]
    except Exception as e:
        log.debug("cursor load failed (first run?): %s", e)
    return {
        "_id": CURSOR_ID,
        "last_block": -1,       # last block height fully indexed for reward_splits + blocks
        "token_ops": {},        # {token_id: count_of_ops_already_mirrored}
        "version": 1,
    }


def _save_cursor(nedb: NedbClient, cursor: dict, dry_run: bool = False) -> None:
    if dry_run:
        return
    try:
        nedb.ensure_db(MIRROR_DB)
        nedb.put(MIRROR_DB, CURSOR_COLL, CURSOR_ID, cursor)
    except Exception as e:
        log.warning("cursor save failed: %s", e)


# ---------------------------------------------------------------------------
# Token operations mirror
# ---------------------------------------------------------------------------

def mirror_tokens(
    rpc: ITCClient,
    nedb: NedbClient,
    db: str,
    cursor: dict,
    dry_run: bool = False,
) -> int:
    """Sync all ITSL token operations into the itsl_ops collection.

    Returns the total number of new ops written.
    """
    total_written = 0
    token_ops_cursor: Dict[str, int] = cursor.get("token_ops") or {}

    try:
        tokens = rpc.all_tokens()
    except RPCError as e:
        log.error("all_tokens RPC failed: %s", e)
        return 0

    log.info("Found %d tokens to mirror", len(tokens))

    for token_entry in tokens:
        token_id = (
            token_entry.get("address")
            or token_entry.get("id")
            or token_entry.get("token_id")
            or ""
        )
        if not token_id:
            continue

        try:
            history = rpc.token_history(token_id)
        except RPCError as e:
            log.warning("token_history(%s) failed: %s", token_id[:16], e)
            continue

        if not isinstance(history, list):
            continue

        already_indexed = token_ops_cursor.get(token_id, 0)
        new_ops = history[already_indexed:]

        if not new_ops:
            log.debug("  %s … up to date (%d ops)", token_id[:20], already_indexed)
            continue

        log.info("  %s: %d new ops (total %d)", token_id[:20], len(new_ops), len(history))

        # Build causal chain: each op's caused_by = [prev_op_nedb_id]
        # We need the nedb _id of the previous op to build the link.
        # The nedb _id for op index i is: f"{token_id}::{op_hash(history[i])}"
        # For the first op ever: caused_by = []
        # For op index i > 0: caused_by = [id of op i-1]

        prev_id: Optional[str] = None

        # If this isn't the very first batch for this token, look up the last op's id.
        if already_indexed > 0 and len(history) > already_indexed:
            prev_op = history[already_indexed - 1]
            prev_id = f"{token_id}::{_op_hash(prev_op)}"

        written_this_token = 0
        for i, op in enumerate(new_ops):
            global_idx = already_indexed + i
            h = _op_hash(op)
            doc_id = f"{token_id}::{h}"

            op_int = op.get("op", 0)
            ts = op.get("timestamp", 0)

            doc = {
                "token": token_id,
                "op": _op_name(op_int),
                "op_code": int(op_int) if str(op_int).isdigit() else 0,
                "from": op.get("from", ""),
                "to": op.get("to", ""),
                "spender": op.get("spender", ""),
                "amount": int(op.get("amount", 0)),
                "name": op.get("name", ""),
                "symbol": op.get("symbol", ""),
                "decimals": int(op.get("decimals", 0)),
                "timestamp": int(ts),
                "signer": op.get("signer", ""),
                "memo": op.get("memo", ""),
                "op_hash": h,
                "seq_in_token": global_idx,
            }

            caused_by: List[str] = [prev_id] if prev_id else []
            valid_from = _ts_to_date(ts)

            if dry_run:
                log.info(
                    "    [DRY-RUN] PUT itsl_ops/%s  op=%s  caused_by=%s",
                    doc_id[:40], doc["op"], caused_by,
                )
            else:
                try:
                    nedb.put(
                        db,
                        "itsl_ops",
                        doc_id,
                        doc,
                        caused_by=caused_by,
                        valid_from=valid_from,
                        evidence=f"itsl_mirror@{__version__}",
                        confidence=1.0,
                    )
                    written_this_token += 1
                    total_written += 1
                except Exception as e:
                    log.warning("    PUT failed for %s: %s", doc_id[:40], e)
                    break  # stop this token on first failure; retry next run

            prev_id = doc_id

        if not dry_run and written_this_token > 0:
            token_ops_cursor[token_id] = already_indexed + written_this_token
            cursor["token_ops"] = token_ops_cursor

    return total_written


# ---------------------------------------------------------------------------
# Block headers + reward splits mirror
# ---------------------------------------------------------------------------

def mirror_blocks(
    rpc: ITCClient,
    nedb: NedbClient,
    db: str,
    cursor: dict,
    *,
    batch_size: int = 50,
    dry_run: bool = False,
) -> int:
    """Sync block headers and reward splits from the last indexed height to tip.

    Returns the number of blocks written.
    """
    try:
        tip = rpc.get_block_count()
    except RPCError as e:
        log.error("getblockcount failed: %s", e)
        return 0

    last_block: int = cursor.get("last_block", -1)
    start = last_block + 1

    if start > tip:
        log.debug("Blocks up to date at height %d", tip)
        return 0

    total = tip - start + 1
    log.info("Indexing blocks %d → %d (%d total)", start, tip, total)

    written = 0
    h = start
    while h <= tip:
        end = min(h + batch_size - 1, tip)
        batch_ops: List[dict] = []

        for height in range(h, end + 1):
            try:
                bhash = rpc.get_block_hash(height)
                # verbosity=2 gives full tx objects (needed for coinbase parse)
                block = rpc.get_block(bhash, verbosity=2)
            except RPCError as e:
                log.warning("  getblock(%d) failed: %s — stopping batch", height, e)
                h = height  # retry from here next run
                break

            ts = block.get("time", 0)
            prev_hash = block.get("previousblockhash", "")

            # ── Block header ────────────────────────────────────────────────
            block_doc_id = str(height)
            block_doc = {
                "height": height,
                "hash": bhash,
                "prev_hash": prev_hash,
                "merkle_root": block.get("merkleroot", ""),
                "timestamp": ts,
                "difficulty": float(block.get("difficulty", 0.0)),
                "n_tx": int(block.get("nTx", 0)),
                "size": int(block.get("size", 0)),
                "weight": int(block.get("weight", 0)),
                "version": int(block.get("version", 0)),
                "bits": block.get("bits", ""),
                "nonce": int(block.get("nonce", 0)),
                "chainwork": block.get("chainwork", ""),
            }

            # ── Block reward split ───────────────────────────────────────────
            miner_s, gov_s, op_s, miner_addr, gov_addr, op_addr = _parse_coinbase_outputs(block)
            total_s = miner_s + gov_s + op_s

            reward_doc_id = str(height)
            reward_doc = {
                "height": height,
                "block_hash": bhash,
                "timestamp": ts,
                "total_reward": total_s,
                "miner_reward": miner_s,
                "governance_reward": gov_s,
                "operator_reward": op_s,
                "miner_address": miner_addr,
                "governance_address": gov_addr,
                "operator_address": op_addr,
                # convenience: float percentages for GROUP BY analytics
                "miner_pct": round(miner_s / total_s * 100, 4) if total_s else 0.0,
                "governance_pct": round(gov_s / total_s * 100, 4) if total_s else 0.0,
                "operator_pct": round(op_s / total_s * 100, 4) if total_s else 0.0,
            }

            valid_from_date = _ts_to_date(ts)

            if dry_run:
                log.info(
                    "  [DRY-RUN] PUT blocks/%d  hash=%s…  reward=%d sats",
                    height, bhash[:12], total_s,
                )
            else:
                # blocks collection: cause chain via prev_hash
                # We store the prev block's nedb doc_id as caused_by so
                # TRACE queries can walk the full block ancestry.
                caused_by: List[str] = [str(height - 1)] if height > 0 else []
                batch_ops.append({
                    "op": "put",
                    "coll": "blocks",
                    "id": block_doc_id,
                    "doc": block_doc,
                    # batch API doesn't support caused_by yet — put separately below
                })
                batch_ops.append({
                    "op": "put",
                    "coll": "reward_splits",
                    "id": reward_doc_id,
                    "doc": reward_doc,
                })

        if batch_ops and not dry_run:
            try:
                result = nedb.batch(db, batch_ops)
                count = result.get("count", 0)
                written += count // 2  # 2 ops per block (blocks + reward_splits)
                new_last = min(end, tip)
                cursor["last_block"] = new_last
                log.info(
                    "  Wrote heights %d–%d (%d ops) — seq %d",
                    h, new_last, count, result.get("seq", 0),
                )
            except Exception as e:
                log.error("  Batch write failed for heights %d–%d: %s", h, end, e)
                break
        elif dry_run:
            written += end - h + 1

        h = end + 1

    return written


# ---------------------------------------------------------------------------
# Main daemon loop
# ---------------------------------------------------------------------------

def run_once(
    rpc: ITCClient,
    nedb: NedbClient,
    db: str,
    *,
    dry_run: bool = False,
    skip_tokens: bool = False,
    skip_blocks: bool = False,
) -> None:
    """Run one full sync pass."""
    t0 = time.time()

    cursor = _load_cursor(nedb)
    log.info(
        "Cursor loaded — last_block=%d  tokens_tracked=%d",
        cursor.get("last_block", -1),
        len(cursor.get("token_ops") or {}),
    )

    if not skip_tokens:
        n_ops = mirror_tokens(rpc, nedb, db, cursor, dry_run=dry_run)
        log.info("Token ops: %d new", n_ops)
    else:
        log.info("Token ops: skipped")

    if not skip_blocks:
        n_blocks = mirror_blocks(rpc, nedb, db, cursor, dry_run=dry_run)
        log.info("Blocks:    %d new", n_blocks)
    else:
        log.info("Blocks:    skipped")

    _save_cursor(nedb, cursor, dry_run=dry_run)
    elapsed = time.time() - t0
    log.info("Pass complete in %.2fs", elapsed)


_shutdown = False


def _handle_signal(signum, frame):
    global _shutdown
    log.info("Signal %d received — shutting down after current pass", signum)
    _shutdown = True


def main() -> int:
    parser = argparse.ArgumentParser(
        description="ITSL → NEDB mirror daemon",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # ITC RPC
    parser.add_argument("--rpc-url", default=os.getenv("ITC_RPC_URL", "http://127.0.0.1:8332"))
    parser.add_argument("--rpc-user", default=os.getenv("ITC_RPC_USER", ""))
    parser.add_argument("--rpc-pass", default=os.getenv("ITC_RPC_PASS", ""))
    parser.add_argument("--wallet", default=os.getenv("ITC_WALLET_NAME", "bulk_payout_wallet"))

    # nedbd
    parser.add_argument("--nedb-url", default=os.getenv("NEDB_URL", "http://127.0.0.1:7070"))
    parser.add_argument("--nedb-token", default=os.getenv("NEDBD_TOKEN", ""))
    parser.add_argument("--db", default=os.getenv("NEDB_DB_NAME", "vision"),
                        help="nedbd database name to write into")

    # Behaviour
    parser.add_argument("--once", action="store_true",
                        help="Run one pass then exit (default: daemon)")
    parser.add_argument("--once-blocks", action="store_true",
                        help="Run one pass for blocks only then exit")
    parser.add_argument("--interval", type=int, default=30,
                        help="Poll interval in seconds (daemon mode)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print what would be written without writing")
    parser.add_argument("--skip-tokens", action="store_true",
                        help="Skip token operation mirroring")
    parser.add_argument("--skip-blocks", action="store_true",
                        help="Skip block/reward mirroring")
    parser.add_argument("--reset-cursor", action="store_true",
                        help="Delete the cursor and start from scratch")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Enable DEBUG logging")

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    log.info("itsl_mirror v%s starting", __version__)
    log.info("  ITC RPC:   %s  wallet=%s", args.rpc_url, args.wallet)
    log.info("  nedbd:     %s  db=%s", args.nedb_url, args.db)

    rpc = ITCClient(
        url=args.rpc_url,
        user=args.rpc_user,
        password=args.rpc_pass,
        wallet=args.wallet,
    )
    nedb = NedbClient(url=args.nedb_url, token=args.nedb_token)

    # Verify connectivity
    try:
        health = nedb.health()
        log.info("nedbd  OK — version=%s  encrypted=%s", health.get("version"), health.get("encrypted"))
    except Exception as e:
        log.error("Cannot reach nedbd at %s: %s", args.nedb_url, e)
        return 1

    try:
        tip = rpc.get_block_count()
        log.info("ITC node OK — tip height=%d", tip)
    except Exception as e:
        log.error("Cannot reach ITC node at %s: %s", args.rpc_url, e)
        return 1

    # Ensure the target database exists
    try:
        nedb.ensure_db(args.db)
        nedb.ensure_db(MIRROR_DB)
    except Exception as e:
        log.error("Failed to ensure databases: %s", e)
        return 1

    # Reset cursor if requested
    if args.reset_cursor and not args.dry_run:
        try:
            nedb.put(MIRROR_DB, CURSOR_COLL, CURSOR_ID, {
                "_id": CURSOR_ID,
                "last_block": -1,
                "token_ops": {},
                "version": 1,
            })
            log.info("Cursor reset to genesis")
        except Exception as e:
            log.warning("Cursor reset failed: %s", e)

    # Register signal handlers
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    # --once-blocks shorthand
    if args.once_blocks:
        run_once(rpc, nedb, args.db, dry_run=args.dry_run,
                 skip_tokens=True, skip_blocks=False)
        return 0

    if args.once:
        run_once(rpc, nedb, args.db, dry_run=args.dry_run,
                 skip_tokens=args.skip_tokens, skip_blocks=args.skip_blocks)
        return 0

    # Daemon mode
    log.info("Daemon mode — polling every %ds  (SIGTERM or Ctrl-C to stop)", args.interval)
    pass_num = 0
    while not _shutdown:
        pass_num += 1
        log.info("── Pass %d ──────────────────────────────", pass_num)
        try:
            run_once(rpc, nedb, args.db, dry_run=args.dry_run,
                     skip_tokens=args.skip_tokens, skip_blocks=args.skip_blocks)
        except Exception as e:
            log.exception("Unexpected error in pass %d: %s", pass_num, e)

        # Sleep in 1s increments so SIGTERM is responsive
        for _ in range(args.interval):
            if _shutdown:
                break
            time.sleep(1)

    log.info("itsl_mirror stopped after %d passes", pass_num)
    return 0


if __name__ == "__main__":
    sys.exit(main())
