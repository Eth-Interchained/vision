# Deploying Interchained Vision

This document is the short, opinionated path to a working production
instance. For full architectural details see `README.md`.

---

## 0. What you need

- An **ITC Core node** with `txindex=1` and JSON-RPC enabled.
- A Linux server with **Python 3.11+** and **Node 20+** (or just Docker).
- ~5 GB of free disk for the local address index DB (grows with chain).

ElectrumX is **optional** — Vision builds and serves its own UTXO and
per-address tx index from the node's RPC, so the explorer is fully
functional without one.

---

## 1. Configure

```bash
cp .env.example .env
$EDITOR .env   # fill in ITC_RPC_HOST / ITC_RPC_USER / ITC_RPC_PASS
```

If your node is behind a reverse-proxied URL, just put the full URL
in `ITC_RPC_HOST` and leave `ITC_RPC_PORT` blank.

---

## 2. Run — pick one

### Option A — Docker (recommended)

```bash
docker compose up -d --build
```

This builds and starts:

- `backend` on port **8080** (FastAPI + indexers)
- `web` on port **5000** (Next.js)

State lives in the named volume `vision-data` (persists across restarts).

### Option B — Bare metal

Two terminals:

```bash
# Terminal 1 — backend
cd backend
pip install -r requirements.txt
bash start.sh
```

```bash
# Terminal 2 — frontend
cd web
npm install
npm run build
npm start
```

---

## 3. First run — what to expect

On first start the backend opens a fresh SQLite database and begins
**two indexers in parallel**:

1. **Block indexer** — caches block headers and recent tx records. Fast.
2. **Address indexer** — walks every block at verbosity=2 and builds a
   local UTXO + per-address-tx history table. **One-time backfill** of
   ~85 minutes on a healthy LAN node, ~85 blocks/sec sustained.

Watch progress:

```bash
curl http://localhost:8080/api/address-index/status
# {"phase":"backfilling","last_height":12345,"tip":510123}
```

Once `phase` reads `"live"` you're caught up. **Restarts always resume
from the last persisted height** — the address index is never rebuilt
unless you explicitly delete `data/vision.db`.

---

## 4. Reverse proxy

Vision listens on `:8080` (API) and `:5000` (web). A typical Nginx
front-door:

```nginx
server {
  listen 443 ssl http2;
  server_name vision.example.com;

  location /api/ { proxy_pass http://127.0.0.1:8080; }
  location /sse/ {
    proxy_pass http://127.0.0.1:8080;
    proxy_http_version 1.1;
    proxy_set_header Connection "";
    proxy_buffering off;
  }
  location /ws/ {
    proxy_pass http://127.0.0.1:8080;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
  }
  location / { proxy_pass http://127.0.0.1:5000; }
}
```

If you put the API on a different origin from the web, set the three
`NEXT_PUBLIC_*_BASE` values in `.env` to the public API origin.

---

## 5. Operational notes

- **Backups**: snapshot `data/vision.db` (and the WAL/SHM siblings) for
  a point-in-time recovery. SQLite WAL mode allows hot copies.
- **Restarts are cheap.** The address indexer resumes from the last
  persisted height; no re-walking, no reindex.
- **Reorgs are automatic** within `REORG_DEPTH` (default 6 blocks). The
  indexer self-detects drift via stored per-height hashes and atomically
  rolls back the canonical UTXO set using its undo log.
- **ElectrumX is optional.** If you point `ELECTRUMX_*` at a server, it
  is used only to top up unconfirmed (mempool) balance. Failures are
  cached for 30 s so a slow upstream cannot block address pages.
- **Schema migrations.** New tables use `CREATE TABLE IF NOT EXISTS` —
  upgrades are non-destructive. Any future incompatible schema change
  will be flagged in the release notes.

---

## 6. Updating

```bash
git pull
docker compose up -d --build   # or repeat the bare-metal step 2
```

Database is preserved. The address index resumes where it left off.

---

## 7. NEDB integration (optional — enables time-travel + NQL showcase)

Vision can use [nedb-engine](https://github.com/Eth-Interchained/nedb) as its
primary KV store and as a rich query layer over token operations, block headers,
and reward splits.

### 7.1 Start nedbd

```bash
pip install nedb-engine
nedbd                              # HTTP on :7070, data in ./nedb-data
# With encryption:
NEDB_TMK=<32-byte-hex> nedbd
```

### 7.2 Enable in Vision

Add to your `.env`:

```env
NEDB_URL=http://127.0.0.1:7070
NEDB_DB_NAME=vision
NEDBD_TOKEN=                       # leave blank unless you set --token on nedbd
```

Restart Vision. The `/api/nedb/*` routes and the `/nedb` showcase page are now live.

### 7.3 Migrate existing SQLite state (one-time)

```bash
cd backend
python scripts/migrate_sqlite_to_nedb.py \
    --sqlite ../data/vision.db \
    --nedb-url http://127.0.0.1:7070 \
    --db vision
```

### 7.4 Start the ITSL mirror daemon

The mirror daemon populates three collections that power the `/nedb` page:
`itsl_ops` (token operations with causal links), `blocks` (block headers),
and `reward_splits` (per-block coinbase splits).

```bash
cp itsl_mirror.env.example itsl_mirror.env
$EDITOR itsl_mirror.env     # fill in RPC + nedbd credentials

# One-off sync (test first):
python backend/scripts/itsl_mirror.py --once

# Daemon mode (keep running in the background):
python backend/scripts/itsl_mirror.py --interval 30

# systemd (production):
sudo cp itsl_mirror.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now itsl_mirror
sudo journalctl -u itsl_mirror -f
```

### 7.5 NQL queries unlocked after mirror runs

```nql
-- Full token operation history with causal chain
FROM itsl_ops WHERE token = "0x...tok" TRACE caused_by

-- Time-travel: what were token balances at block 50000?
FROM itsl_ops WHERE token = "0x...tok" AS OF 50000

-- Bi-temporal: operations valid on a specific date
FROM itsl_ops VALID AS OF "2026-01-01" WHERE token = "0x...tok"

-- Governance treasury inflow this month
FROM reward_splits WHERE timestamp >= 1748736000 GROUP BY governance_address SUM governance_reward

-- Chain tip at any point in history
FROM blocks ORDER BY height DESC LIMIT 1 AS OF <seq>
```

Visit `/nedb` in the Vision UI to run these interactively.
