//! nedb-migrator — fast, resumable, low-memory SQLite → nedbd migration tool
//!
//! Streams SQLite rows in chunks so memory usage stays constant regardless of
//! table size. 1.2M rows uses the same RAM as 1k rows.
//!
//! # Usage
//!
//! ```bash
//! nedb-migrator --sqlite ../data/vision.db
//! nedb-migrator --sqlite ../data/vision.db --skip-block-cache
//! nedb-migrator --sqlite ../data/vision.db --reset
//! nedb-migrator --sqlite ../data/vision.db --dry-run
//! nedb-migrator --sqlite ../data/vision.db --chunk 5000 --concurrency 32
//! ```

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use clap::Parser;
use colored::*;
use indicatif::{ProgressBar, ProgressStyle};
use reqwest::Client;
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::Semaphore;

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

#[derive(Parser, Debug)]
#[command(name = "nedb-migrator", version = "1.0.0",
          about = "Fast, resumable, streaming SQLite → nedbd migration")]
struct Cli {
    #[arg(long, default_value = "../data/vision.db")]
    sqlite: PathBuf,

    #[arg(long, default_value = "http://127.0.0.1:7070")]
    nedb_url: String,

    #[arg(long, default_value = "vision")]
    db: String,

    #[arg(long, default_value = "")]
    token: String,

    /// Rows fetched from SQLite per streaming chunk (controls peak memory)
    #[arg(long, default_value_t = 2000)]
    chunk: usize,

    /// Concurrent nedbd batch requests per chunk.
    /// Reduce to 2-4 for encrypted databases — nedbd's Sequencer serialises
    /// writes and high concurrency causes queue buildup + timeouts.
    #[arg(long, default_value_t = 4)]
    concurrency: usize,

    /// Rows per nedbd batch request
    #[arg(long, default_value_t = 50)]
    batch_size: usize,

    /// Skip vision:block:height:* and vision:block:hash:* kv rows
    #[arg(long)]
    skip_block_cache: bool,

    #[arg(long, default_value = ".nedb-migrator-state.json")]
    state_file: PathBuf,

    #[arg(long)]
    reset: bool,

    #[arg(long)]
    no_verify: bool,

    #[arg(long)]
    dry_run: bool,

    #[arg(long, short)]
    verbose: bool,
}

// ---------------------------------------------------------------------------
// Resume state
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct State {
    kv_done:    usize,
    zsets_done: usize,
    sets_done:  usize,
}

fn load_state(path: &Path) -> State {
    fs::read_to_string(path)
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default()
}

fn save_state(path: &Path, state: &State) -> Result<()> {
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, serde_json::to_string_pretty(state)?)?;
    fs::rename(&tmp, path).context("atomic rename failed")?;
    Ok(())
}

// ---------------------------------------------------------------------------
// SQLite helpers — streaming via LIMIT/OFFSET, never loads full table
// ---------------------------------------------------------------------------

fn count_table(conn: &Connection, table: &str, extra_where: &str) -> Result<usize> {
    let sql = if extra_where.is_empty() {
        format!("SELECT COUNT(*) FROM {table}")
    } else {
        format!("SELECT COUNT(*) FROM {table} WHERE {extra_where}")
    };
    Ok(conn.query_row(&sql, [], |r| r.get::<_, i64>(0))? as usize)
}

/// Fetch one chunk of kv rows starting at `offset`, up to `limit` rows.
fn fetch_kv_chunk(
    conn: &Connection,
    offset: usize,
    limit: usize,
    skip_block_cache: bool,
) -> Result<Vec<Value>> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64();

    let sql = "SELECT key, value, expires_at FROM kv ORDER BY rowid LIMIT ?1 OFFSET ?2";
    let mut stmt = conn.prepare(sql)?;
    let rows: Vec<Value> = stmt
        .query_map([limit as i64, offset as i64], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, Option<f64>>(2)?,
            ))
        })?
        .filter_map(|r| r.ok())
        .filter(|(key, _, expires_at)| {
            if let Some(exp) = expires_at {
                if *exp < now { return false; }
            }
            if skip_block_cache
                && (key.starts_with("vision:block:height:")
                    || key.starts_with("vision:block:hash:"))
            {
                return false;
            }
            true
        })
        .map(|(key, value, expires_at)| json!({
            "op": "put", "coll": "kv", "id": &key,
            "doc": { "_id": &key, "value": value, "expires_at": expires_at }
        }))
        .collect();
    Ok(rows)
}

fn fetch_zset_chunk(conn: &Connection, offset: usize, limit: usize) -> Result<Vec<Value>> {
    let sql = "SELECT name, member, score FROM zsets ORDER BY rowid LIMIT ?1 OFFSET ?2";
    let mut stmt = conn.prepare(sql)?;
    let rows: Vec<Value> = stmt
        .query_map([limit as i64, offset as i64], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, f64>(2)?))
        })?
        .filter_map(|r| r.ok())
        .map(|(name, member, score)| {
            let id = format!("{name}::{member}");
            json!({
                "op": "put", "coll": "zset", "id": &id,
                "doc": { "_id": &id, "_name": name, "_member": member, "score": score }
            })
        })
        .collect();
    Ok(rows)
}

fn fetch_set_chunk(conn: &Connection, offset: usize, limit: usize) -> Result<Vec<Value>> {
    let sql = "SELECT name, member FROM sets ORDER BY rowid LIMIT ?1 OFFSET ?2";
    let mut stmt = conn.prepare(sql)?;
    let rows: Vec<Value> = stmt
        .query_map([limit as i64, offset as i64], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
        })?
        .filter_map(|r| r.ok())
        .map(|(name, member)| {
            let id = format!("{name}::{member}");
            json!({
                "op": "put", "coll": "set", "id": &id,
                "doc": { "_id": &id, "_name": name, "_member": member }
            })
        })
        .collect();
    Ok(rows)
}

// ---------------------------------------------------------------------------
// nedbd HTTP
// ---------------------------------------------------------------------------

trait MaybeBearer { fn maybe_bearer(self, token: &str) -> Self; }
impl MaybeBearer for reqwest::RequestBuilder {
    fn maybe_bearer(self, t: &str) -> Self {
        if t.is_empty() { self } else { self.bearer_auth(t) }
    }
}

async fn nedb_health(client: &Client, base: &str, token: &str) -> Result<Value> {
    Ok(client.get(format!("{base}/health"))
        .maybe_bearer(token).send().await?.json().await?)
}

async fn ensure_db(client: &Client, base: &str, db: &str, token: &str) -> Result<()> {
    let mut last_err = String::new();
    for attempt in 1u8..=3 {
        match client.get(format!("{base}/v1/databases/{db}"))
            .maybe_bearer(token).send().await
        {
            Ok(r) if r.status().is_success() => return Ok(()),
            Ok(r) if r.status().as_u16() == 404 => {
                client.post(format!("{base}/v1/databases"))
                    .maybe_bearer(token)
                    .json(&json!({"name": db}))
                    .send().await?.error_for_status()?;
                return Ok(());
            }
            Ok(r)  => last_err = format!("HTTP {}", r.status()),
            Err(e) => {
                last_err = e.to_string();
                if attempt < 3 {
                    eprintln!("  ensure_db attempt {attempt}/3 failed, retrying in 5s…");
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                }
            }
        }
    }
    anyhow::bail!("ensure_db failed after 3 attempts: {last_err}")
}

async fn send_batch(client: &Client, base: &str, db: &str, token: &str, ops: Vec<Value>) -> Result<usize> {
    // Retry up to 4 times with exponential backoff.
    // Encrypted nedbd under write pressure can timeout transiently — this
    // ensures a single slow Sequencer flush doesn't abort the migration.
    let mut delay_ms = 500u64;
    let mut last_err = String::new();
    for attempt in 1u8..=4 {
        match client.post(format!("{base}/v1/databases/{db}/batch"))
            .maybe_bearer(token)
            .json(&json!({"ops": &ops}))
            .send().await
        {
            Ok(r) if r.status().is_success() => {
                let body: Value = r.json().await?;
                return Ok(body["count"].as_u64().unwrap_or(0) as usize);
            }
            Ok(r) => last_err = format!("HTTP {}", r.status()),
            Err(e) => last_err = e.to_string(),
        }
        if attempt < 4 {
            tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
            delay_ms = (delay_ms * 2).min(8_000);
        }
    }
    anyhow::bail!("batch failed after 4 attempts: {last_err}")
}

// ---------------------------------------------------------------------------
// Streaming table sender — processes one chunk at a time, constant memory
// ---------------------------------------------------------------------------

async fn stream_table(
    label:        &str,
    total:        usize,
    start_offset: usize,
    cli:          &Cli,
    state_field:  &mut usize,
    state:        &mut State,
    fetch_chunk:  impl Fn(usize, usize) -> Result<Vec<Value>>,
    client:       Arc<Client>,
    pb:           &ProgressBar,
) -> Result<usize> {
    let mut offset  = start_offset;
    let mut sent    = 0usize;

    while offset < total {
        // 1. Fetch one chunk from SQLite (low memory — only `chunk` rows at a time)
        let ops = fetch_chunk(offset, cli.chunk)?;
        if ops.is_empty() {
            break; // expired/filtered rows mean chunk may be empty — advance
        }

        let chunk_len = ops.len();

        // 2. Split chunk into batches and send concurrently
        let sem = Arc::new(Semaphore::new(cli.concurrency));
        let mut handles = Vec::new();

        for batch in ops.chunks(cli.batch_size) {
            let batch_ops  = batch.to_vec();
            let batch_len  = batch_ops.len();
            let c2         = Arc::clone(&client);
            let sem2       = Arc::clone(&sem);
            let base       = cli.nedb_url.clone();
            let db         = cli.db.clone();
            let tok        = cli.token.clone();
            let dry        = cli.dry_run;

            let h: tokio::task::JoinHandle<Result<usize>> = tokio::spawn(async move {
                let _p = sem2.acquire_owned().await.unwrap();
                if dry { return Ok(batch_len); }
                send_batch(&c2, &base, &db, &tok, batch_ops).await
            });
            handles.push((h, batch_len));
        }

        // 3. Collect results in order
        let mut chunk_sent = 0usize;
        for (h, batch_len) in handles {
            chunk_sent += h.await.context("task panicked")??;
            pb.inc(batch_len as u64);
        }

        sent    += chunk_sent;
        offset  += chunk_len;       // advance by raw chunk size (pre-filter)

        // 4. Persist cursor after every chunk — losing a chunk is the worst case
        *state_field = offset;
        if !cli.dry_run {
            save_state(&cli.state_file, state)?;
        }

        if cli.verbose {
            eprintln!("  {label}: offset={offset}/{total} sent_this_chunk={chunk_sent}");
        }
    }

    pb.finish_with_message(format!("{} rows", start_offset + sent));
    Ok(sent)
}

// ---------------------------------------------------------------------------
// nedbd-side verification
// ---------------------------------------------------------------------------

async fn count_collection(client: &Client, base: &str, db: &str, token: &str, coll: &str) -> usize {
    let res = client.post(format!("{base}/v1/databases/{db}/query"))
        .maybe_bearer(token)
        .json(&json!({"nql": format!("FROM {coll} LIMIT 9999999")}))
        .send().await;
    match res {
        Ok(r) if r.status().is_success() => {
            r.json::<Value>().await.ok()
                .and_then(|v| v["count"].as_u64())
                .unwrap_or(0) as usize
        }
        _ => 0,
    }
}

async fn verify_state(
    client: &Client, base: &str, db: &str, token: &str,
    state: &mut State, state_file: &Path,
    kv_total: usize, zsets_total: usize, sets_total: usize,
) -> Result<()> {
    print!("{} Checking nedbd collections…  ", "◉".blue());
    std::io::Write::flush(&mut std::io::stdout()).ok();

    let kv_n   = count_collection(client, base, db, token, "kv").await;
    let zset_n = count_collection(client, base, db, token, "zset").await;
    let set_n  = count_collection(client, base, db, token, "set").await;
    println!("kv={kv_n} zset={zset_n} set={set_n}");

    let mut advanced = false;
    macro_rules! sync_f {
        ($f:expr, $n:expr, $total:expr, $lbl:expr) => {
            if $n > $f && $n <= $total {
                println!("  {} {}: {} → {} (advancing)", "↑".yellow(), $lbl, $f, $n);
                $f = $n; advanced = true;
            } else if $n >= $total {
                println!("  {} {}: all {} rows already in nedbd", "✓".green(), $lbl, $total);
                $f = $total; advanced = true;
            }
        };
    }
    sync_f!(state.kv_done,    kv_n,   kv_total,    "kv");
    sync_f!(state.zsets_done, zset_n, zsets_total, "zsets");
    sync_f!(state.sets_done,  set_n,  sets_total,  "sets");

    if advanced { save_state(state_file, state)?; println!("  {} State synced.\n", "✓".green()); }
    else        { println!("  {} Consistent.\n", "✓".green()); }
    Ok(())
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    println!("\n{} {}  —  SQLite → nedbd  (streaming)\n",
        "nedb-migrator".bold().cyan(), "v1.0.0".dimmed());
    println!("  sqlite            {}", cli.sqlite.display());
    println!("  nedbd             {}", cli.nedb_url);
    println!("  database          {}", cli.db);
    println!("  chunk size        {}  rows (peak memory: ~{} MB)",
        cli.chunk,
        cli.chunk * 300 / 1_000_000 + 1);  // rough estimate
    println!("  concurrency       {}", cli.concurrency);
    println!("  batch size        {}", cli.batch_size);
    println!("  skip-block-cache  {}", cli.skip_block_cache);
    println!("  dry-run           {}", cli.dry_run);
    println!("  state file        {}", cli.state_file.display());
    println!();

    // ── Resume state ────────────────────────────────────────────────────────
    if cli.reset && cli.state_file.exists() {
        fs::remove_file(&cli.state_file).ok();
        println!("{} State reset.\n", "↺".yellow());
    }
    let mut state = if cli.reset { State::default() } else { load_state(&cli.state_file) };

    if state.kv_done + state.zsets_done + state.sets_done > 0 {
        println!("{} Resuming — kv={} zsets={} sets={}\n",
            "→".green(), state.kv_done, state.zsets_done, state.sets_done);
    }

    // ── Open SQLite (read-only) ──────────────────────────────────────────────
    let canon = cli.sqlite.canonicalize()
        .with_context(|| format!("SQLite not found: {}", cli.sqlite.display()))?;
    let conn = Connection::open_with_flags(&canon,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX)?;

    // Count rows — cheap, doesn't load data
    print!("{} Counting rows…  ", "◉".blue());
    std::io::Write::flush(&mut std::io::stdout()).ok();
    let kv_total    = count_table(&conn, "kv", "")?;
    let zsets_total = count_table(&conn, "zsets", "")?;
    let sets_total  = count_table(&conn, "sets", "")?;
    println!("kv={} zsets={} sets={}\n",
        kv_total.to_string().yellow(),
        zsets_total.to_string().yellow(),
        sets_total.to_string().yellow());

    // ── Connectivity ────────────────────────────────────────────────────────
    let client = Arc::new(Client::builder()
        .timeout(std::time::Duration::from_secs(120))
        .build()?);

    if !cli.dry_run {
        let h = nedb_health(&client, &cli.nedb_url, &cli.token)
            .await.context("Cannot reach nedbd")?;
        println!("{} nedbd {}  version={}  encrypted={}\n",
            "✓".green(), "OK".green(),
            h["version"].as_str().unwrap_or("?"),
            h["encrypted"].as_bool().unwrap_or(false));

        if !cli.no_verify {
            ensure_db(&client, &cli.nedb_url, &cli.db, &cli.token).await?;
            verify_state(&client, &cli.nedb_url, &cli.db, &cli.token,
                &mut state, &cli.state_file,
                kv_total, zsets_total, sets_total).await?;
        } else {
            println!("{} Skipping nedbd check (--no-verify)\n", "⚠".yellow());
        }
    } else {
        println!("{} Dry-run\n", "⚠".yellow());
    }

    // ── Progress bars ────────────────────────────────────────────────────────
    let style = ProgressStyle::with_template(
        "{prefix:.bold}  [{bar:42.cyan/blue}] {pos:>9}/{len:>9}  {per_sec:>12}  eta {eta}"
    ).unwrap().progress_chars("█▉▊▋▌▍▎▏ ");

    let pb_kv = ProgressBar::new(kv_total as u64);
    pb_kv.set_style(style.clone()); pb_kv.set_prefix("kv   ");
    pb_kv.set_position(state.kv_done as u64);

    let pb_zset = ProgressBar::new(zsets_total as u64);
    pb_zset.set_style(style.clone()); pb_zset.set_prefix("zset ");
    pb_zset.set_position(state.zsets_done as u64);

    let pb_set = ProgressBar::new(sets_total as u64);
    pb_set.set_style(style.clone()); pb_set.set_prefix("set  ");
    pb_set.set_position(state.sets_done as u64);

    let t0 = Instant::now();

    // ── kv ───────────────────────────────────────────────────────────────────
    let kv_start = state.kv_done;
    {
        let skip = cli.skip_block_cache;
        let c    = Arc::clone(&client);
        let kv_sent = stream_table(
            "kv", kv_total, kv_start, &cli,
            &mut state.kv_done, &mut state,
            |off, lim| fetch_kv_chunk(&conn, off, lim, skip),
            c, &pb_kv,
        ).await?;
        let _ = kv_sent; // progress bar handles display
    }

    // ── zsets ────────────────────────────────────────────────────────────────
    let zsets_start = state.zsets_done;
    {
        let c = Arc::clone(&client);
        stream_table(
            "zset", zsets_total, zsets_start, &cli,
            &mut state.zsets_done, &mut state,
            |off, lim| fetch_zset_chunk(&conn, off, lim),
            c, &pb_zset,
        ).await?;
    }

    // ── sets ─────────────────────────────────────────────────────────────────
    let sets_start = state.sets_done;
    {
        let c = Arc::clone(&client);
        stream_table(
            "set", sets_total, sets_start, &cli,
            &mut state.sets_done, &mut state,
            |off, lim| fetch_set_chunk(&conn, off, lim),
            c, &pb_set,
        ).await?;
    }

    // ── Summary ───────────────────────────────────────────────────────────────
    let elapsed = t0.elapsed().as_secs_f64();
    let total   = (state.kv_done    - kv_start)
                + (state.zsets_done - zsets_start)
                + (state.sets_done  - sets_start);
    let rps = if elapsed > 0.0 { total as f64 / elapsed } else { 0.0 };

    println!("\n{}", "─".repeat(52));
    println!("{}", if cli.dry_run { " DRY-RUN summary " } else { " Migration complete " }.bold());
    println!("{}", "─".repeat(52));
    println!("  kv sent:     {}", (state.kv_done - kv_start).to_string().green());
    println!("  zsets sent:  {}", (state.zsets_done - zsets_start).to_string().green());
    println!("  sets sent:   {}", (state.sets_done - sets_start).to_string().green());
    println!("  total:       {}", total.to_string().bold().green());
    println!("  elapsed:     {:.1}s  ({:.0} rows/s)", elapsed, rps);

    if !cli.dry_run && total > 0 {
        println!("\n{} State → {}", "✓".green(), cli.state_file.display());
    }
    if total == 0 {
        println!("\n{} Nothing new — already migrated. Use --reset to start over.", "✓".green());
    }
    println!();
    Ok(())
}
