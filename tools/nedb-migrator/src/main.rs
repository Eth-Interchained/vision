//! nedb-migrator — fast, resumable SQLite → nedbd migration tool
//!
//! Reads all kv / zsets / sets rows from a Vision SQLite database and writes
//! them to a running nedbd instance using concurrent batch HTTP requests.
//!
//! # Usage
//!
//! ```bash
//! # Dry run — see what would be migrated
//! nedb-migrator --sqlite ../data/vision.db --dry-run
//!
//! # Full migration (resumes automatically if interrupted)
//! nedb-migrator --sqlite ../data/vision.db
//!
//! # Skip the ~90k block cache rows, only migrate live state (~20 rows)
//! nedb-migrator --sqlite ../data/vision.db --skip-block-cache
//!
//! # Reset progress and start from scratch
//! nedb-migrator --sqlite ../data/vision.db --reset
//!
//! # Tune concurrency and batch size for faster hardware
//! nedb-migrator --sqlite ../data/vision.db --concurrency 32 --batch-size 200
//! ```

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use clap::Parser;
use colored::*;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use reqwest::Client;
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::Semaphore;

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

#[derive(Parser, Debug)]
#[command(
    name        = "nedb-migrator",
    version     = "1.0.0",
    about       = "Fast, resumable SQLite → nedbd migration for Interchained Vision",
    long_about  = None,
)]
struct Cli {
    /// Path to the Vision SQLite database
    #[arg(long, default_value = "../data/vision.db")]
    sqlite: PathBuf,

    /// nedbd base URL
    #[arg(long, env = "NEDB_URL", default_value = "http://127.0.0.1:7070")]
    nedb_url: String,

    /// nedbd database name
    #[arg(long, env = "NEDB_DB_NAME", default_value = "vision")]
    db: String,

    /// nedbd bearer token (leave blank if not set)
    #[arg(long, env = "NEDBD_TOKEN", default_value = "")]
    token: String,

    /// Number of ops per batch request sent to nedbd
    #[arg(long, default_value_t = 100)]
    batch_size: usize,

    /// Maximum concurrent batch requests in flight
    #[arg(long, default_value_t = 16)]
    concurrency: usize,

    /// Skip vision:block:height:* and vision:block:hash:* rows (~90k rows)
    #[arg(long)]
    skip_block_cache: bool,

    /// Path to the resume state file
    #[arg(long, default_value = ".nedb-migrator-state.json")]
    state_file: PathBuf,

    /// Delete saved progress and start from scratch
    #[arg(long)]
    reset: bool,

    /// Print what would be migrated without writing to nedbd
    #[arg(long)]
    dry_run: bool,

    /// Verbose: print each batch
    #[arg(long, short)]
    verbose: bool,
}

// ---------------------------------------------------------------------------
// Resume state
// ---------------------------------------------------------------------------

/// Tracks how many rows of each table have already been successfully sent.
/// Written atomically after every batch via temp-file-then-rename.
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
// SQLite row types
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct KvRow {
    key:        String,
    value:      String,
    expires_at: Option<f64>,
}

#[derive(Clone)]
struct ZsetRow {
    name:   String,
    member: String,
    score:  f64,
}

#[derive(Clone)]
struct SetRow {
    name:   String,
    member: String,
}

// ---------------------------------------------------------------------------
// SQLite readers (all read-only, one pass each)
// ---------------------------------------------------------------------------

fn read_kv(conn: &Connection, skip_block_cache: bool) -> Result<Vec<KvRow>> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64();

    let mut stmt = conn.prepare("SELECT key, value, expires_at FROM kv ORDER BY rowid")?;
    let rows: Vec<KvRow> = stmt
        .query_map([], |r| {
            Ok(KvRow {
                key:        r.get(0)?,
                value:      r.get(1)?,
                expires_at: r.get(2)?,
            })
        })?
        .filter_map(|r| r.ok())
        .filter(|r| {
            // Drop expired rows
            if let Some(exp) = r.expires_at {
                if exp < now {
                    return false;
                }
            }
            // Optionally skip the per-block cache (~90k rows)
            if skip_block_cache
                && (r.key.starts_with("vision:block:height:")
                    || r.key.starts_with("vision:block:hash:"))
            {
                return false;
            }
            true
        })
        .collect();
    Ok(rows)
}

fn read_zsets(conn: &Connection) -> Result<Vec<ZsetRow>> {
    let mut stmt =
        conn.prepare("SELECT name, member, score FROM zsets ORDER BY rowid")?;
    Ok(stmt
        .query_map([], |r| {
            Ok(ZsetRow {
                name:   r.get(0)?,
                member: r.get(1)?,
                score:  r.get(2)?,
            })
        })?
        .filter_map(|r| r.ok())
        .collect())
}

fn read_sets(conn: &Connection) -> Result<Vec<SetRow>> {
    let mut stmt = conn.prepare("SELECT name, member FROM sets ORDER BY rowid")?;
    Ok(stmt
        .query_map([], |r| {
            Ok(SetRow {
                name:   r.get(0)?,
                member: r.get(1)?,
            })
        })?
        .filter_map(|r| r.ok())
        .collect())
}

// ---------------------------------------------------------------------------
// nedbd batch op builders
// ---------------------------------------------------------------------------

fn kv_op(r: &KvRow) -> Value {
    json!({
        "op": "put", "coll": "kv", "id": r.key,
        "doc": { "_id": r.key, "value": r.value, "expires_at": r.expires_at }
    })
}

fn zset_op(r: &ZsetRow) -> Value {
    let id = format!("{}::{}", r.name, r.member);
    json!({
        "op": "put", "coll": "zset", "id": &id,
        "doc": { "_id": &id, "_name": r.name, "_member": r.member, "score": r.score }
    })
}

fn set_op(r: &SetRow) -> Value {
    let id = format!("{}::{}", r.name, r.member);
    json!({
        "op": "put", "coll": "set", "id": &id,
        "doc": { "_id": &id, "_name": r.name, "_member": r.member }
    })
}

// ---------------------------------------------------------------------------
// nedbd HTTP helpers
// ---------------------------------------------------------------------------

async fn nedb_health(client: &Client, base: &str, token: &str) -> Result<Value> {
    Ok(client
        .get(format!("{}/health", base))
        .maybe_bearer(token)
        .send()
        .await?
        .json::<Value>()
        .await?)
}

async fn ensure_db(client: &Client, base: &str, db: &str, token: &str) -> Result<()> {
    let check = client
        .get(format!("{}/v1/databases/{}", base, db))
        .maybe_bearer(token)
        .send()
        .await?;
    if check.status().is_success() {
        return Ok(());
    }
    client
        .post(format!("{}/v1/databases", base))
        .maybe_bearer(token)
        .json(&json!({"name": db}))
        .send()
        .await?
        .error_for_status()
        .context("Failed to create nedbd database")?;
    Ok(())
}

async fn send_batch_http(
    client: &Client,
    base:   &str,
    db:     &str,
    token:  &str,
    ops:    Vec<Value>,
) -> Result<usize> {
    let resp = client
        .post(format!("{}/v1/databases/{}/batch", base, db))
        .maybe_bearer(token)
        .json(&json!({"ops": ops}))
        .send()
        .await?
        .error_for_status()
        .context("nedbd batch failed")?;
    let body: Value = resp.json().await?;
    Ok(body["count"].as_u64().unwrap_or(0) as usize)
}

/// Trait for optional bearer auth — keeps call sites clean.
trait MaybeBearer {
    fn maybe_bearer(self, token: &str) -> Self;
}
impl MaybeBearer for reqwest::RequestBuilder {
    fn maybe_bearer(self, token: &str) -> Self {
        if token.is_empty() { self } else { self.bearer_auth(token) }
    }
}

// ---------------------------------------------------------------------------
// Core: send one table's ops with resume + concurrent batches
// ---------------------------------------------------------------------------

/// Send all ops for a single table, resuming from `already_done`.
///
/// Spawns up to `concurrency` tokio tasks simultaneously (semaphore-limited).
/// After each batch completes **in order**, the cursor is advanced and the
/// state file is saved atomically — so a kill at any point loses at most
/// one batch worth of work.
async fn send_table_ops(
    ops:         Vec<Value>,
    already_done: usize,
    label:       &str,
    cli:         &Cli,
    state:       &mut State,
    state_field: fn(&mut State) -> &mut usize,
    pb:          &ProgressBar,
) -> Result<usize> {
    let remaining = if already_done < ops.len() {
        &ops[already_done..]
    } else {
        pb.finish_with_message("already done");
        return Ok(0);
    };

    let client  = Arc::new(Client::builder()
        .timeout(std::time::Duration::from_secs(60))
        .build()?);
    let sem     = Arc::new(Semaphore::new(cli.concurrency));

    // Pre-chunk so we can spawn all tasks up front, then await in order.
    let chunks: Vec<Vec<Value>> = remaining
        .chunks(cli.batch_size)
        .map(|c| c.to_vec())
        .collect();

    let mut handles = Vec::with_capacity(chunks.len());
    for chunk in chunks {
        let chunk_len = chunk.len();
        let client2   = Arc::clone(&client);
        let sem2      = Arc::clone(&sem);
        let base      = cli.nedb_url.clone();
        let db        = cli.db.clone();
        let token     = cli.token.clone();
        let dry       = cli.dry_run;

        let h: tokio::task::JoinHandle<Result<usize>> = tokio::spawn(async move {
            let _permit = sem2.acquire_owned().await.unwrap();
            if dry { return Ok(chunk_len); }
            send_batch_http(&client2, &base, &db, &token, chunk).await
        });
        handles.push((h, chunk_len));
    }

    let mut total_sent = 0usize;
    for (handle, chunk_len) in handles {
        let written = handle.await.context("task panicked")??;
        total_sent += written;
        *state_field(state) += chunk_len;
        if !cli.dry_run {
            save_state(&cli.state_file, state)?;
        }
        pb.inc(chunk_len as u64);
        if cli.verbose {
            eprintln!("  {} +{} (total {})", label, chunk_len, *state_field(state));
        }
    }

    pb.finish_with_message(format!("{} rows", already_done + total_sent));
    Ok(total_sent)
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    println!(
        "\n{} {}  —  SQLite → nedbd\n",
        "nedb-migrator".bold().cyan(),
        "v1.0.0".dimmed()
    );
    println!("  sqlite          {}", cli.sqlite.display());
    println!("  nedbd           {}", cli.nedb_url);
    println!("  database        {}", cli.db);
    println!("  batch-size      {}", cli.batch_size);
    println!("  concurrency     {}", cli.concurrency);
    println!("  skip-block-cache  {}", cli.skip_block_cache);
    println!("  dry-run         {}", cli.dry_run);
    println!("  state file      {}", cli.state_file.display());
    println!();

    // ── Resume state ─────────────────────────────────────────────────────
    if cli.reset && cli.state_file.exists() {
        fs::remove_file(&cli.state_file).ok();
        println!("{} State reset.\n", "↺".yellow());
    }
    let mut state = if cli.reset { State::default() } else { load_state(&cli.state_file) };

    if state.kv_done + state.zsets_done + state.sets_done > 0 {
        println!(
            "{} Resuming — kv={} zsets={} sets={}\n",
            "→".green(), state.kv_done, state.zsets_done, state.sets_done
        );
    }

    // ── Open SQLite ───────────────────────────────────────────────────────
    let canon = cli.sqlite.canonicalize()
        .with_context(|| format!("SQLite not found: {}", cli.sqlite.display()))?;
    let conn = Connection::open_with_flags(
        &canon,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .context("Failed to open SQLite")?;

    print!("{} Reading SQLite…  ", "◉".blue());
    let t0 = Instant::now();
    let kv_rows   = read_kv(&conn, cli.skip_block_cache)?;
    let zset_rows = read_zsets(&conn)?;
    let set_rows  = read_sets(&conn)?;
    drop(conn); // release the file handle
    println!(
        "kv={} zsets={} sets={}  ({} ms)\n",
        kv_rows.len().to_string().yellow(),
        zset_rows.len().to_string().yellow(),
        set_rows.len().to_string().yellow(),
        t0.elapsed().as_millis()
    );

    // ── Connectivity check ────────────────────────────────────────────────
    if !cli.dry_run {
        let probe = Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()?;
        let h = nedb_health(&probe, &cli.nedb_url, &cli.token)
            .await
            .context("Cannot reach nedbd — is it running?")?;
        println!(
            "{} nedbd {}  version={}  encrypted={}\n",
            "✓".green(), "OK".green(),
            h["version"].as_str().unwrap_or("?"),
            h["encrypted"].as_bool().unwrap_or(false)
        );
        ensure_db(&probe, &cli.nedb_url, &cli.db, &cli.token).await?;
    } else {
        println!("{} Dry-run — skipping nedbd check\n", "⚠".yellow());
    }

    // ── Progress bars ─────────────────────────────────────────────────────
    let style = ProgressStyle::with_template(
        "{prefix:.bold}  [{bar:42.cyan/blue}] {pos:>7}/{len:>7}  {per_sec:>10}  eta {eta}",
    )
    .unwrap()
    .progress_chars("█▉▊▋▌▍▎▏ ");

    let mp = MultiProgress::new();

    let pb_kv = mp.add(ProgressBar::new(kv_rows.len() as u64));
    pb_kv.set_style(style.clone());
    pb_kv.set_prefix("kv   ");
    pb_kv.set_position(state.kv_done as u64);

    let pb_zset = mp.add(ProgressBar::new(zset_rows.len() as u64));
    pb_zset.set_style(style.clone());
    pb_zset.set_prefix("zset ");
    pb_zset.set_position(state.zsets_done as u64);

    let pb_set = mp.add(ProgressBar::new(set_rows.len() as u64));
    pb_set.set_style(style.clone());
    pb_set.set_prefix("set  ");
    pb_set.set_position(state.sets_done as u64);

    // ── Build op vecs ─────────────────────────────────────────────────────
    let kv_ops:   Vec<Value> = kv_rows.iter().map(kv_op).collect();
    let zset_ops: Vec<Value> = zset_rows.iter().map(zset_op).collect();
    let set_ops:  Vec<Value> = set_rows.iter().map(set_op).collect();

    let t_migrate = Instant::now();

    // ── Send kv ───────────────────────────────────────────────────────────
    let kv_skip = state.kv_done;
    let kv_sent = send_table_ops(
        kv_ops, kv_skip, "kv", &cli, &mut state,
        |s| &mut s.kv_done, &pb_kv,
    ).await?;

    // ── Send zsets ────────────────────────────────────────────────────────
    let zsets_skip = state.zsets_done;
    let zsets_sent = send_table_ops(
        zset_ops, zsets_skip, "zset", &cli, &mut state,
        |s| &mut s.zsets_done, &pb_zset,
    ).await?;

    // ── Send sets ─────────────────────────────────────────────────────────
    let sets_skip = state.sets_done;
    let sets_sent = send_table_ops(
        set_ops, sets_skip, "set", &cli, &mut state,
        |s| &mut s.sets_done, &pb_set,
    ).await?;

    // ── Summary ───────────────────────────────────────────────────────────
    let elapsed = t_migrate.elapsed().as_secs_f64();
    let total   = kv_sent + zsets_sent + sets_sent;
    let rps     = if elapsed > 0.0 { total as f64 / elapsed } else { 0.0 };

    println!();
    println!("{}", "─".repeat(52));
    println!("{}", if cli.dry_run { " DRY-RUN summary " } else { " Migration complete " }.bold());
    println!("{}", "─".repeat(52));

    let tag = if cli.dry_run { "[DRY] ".yellow().to_string() } else { String::new() };
    println!("  {}kv sent:       {}", tag, kv_sent.to_string().green());
    if kv_skip > 0 {
        println!("  kv skipped:    {} (already done)", kv_skip.to_string().dimmed());
    }
    println!("  {}zsets sent:    {}", tag, zsets_sent.to_string().green());
    println!("  {}sets sent:     {}", tag, sets_sent.to_string().green());
    println!("  {}total:         {}", tag, total.to_string().bold().green());
    println!("  elapsed:        {:.2}s  ({:.0} rows/s)", elapsed, rps);

    if !cli.dry_run && total > 0 {
        println!();
        println!("{} State → {}", "✓".green(), cli.state_file.display());
    }

    if total == 0 && (kv_skip + zsets_skip + sets_skip) > 0 {
        println!();
        println!("{} All rows already migrated. Run with {} to start over.",
            "✓".green(), "--reset".bold());
    }

    println!();
    Ok(())
}
