'use client';

import { useEffect, useState, useRef, useCallback } from 'react';
import Image from 'next/image';
import { api } from '@/lib/api';
import { formatNumber } from '@/lib/utils';

/* ── Types ─────────────────────────────────────────────────────────── */
interface SyncInfo {
  phase: string;
  last_height?: number;
  tip?: number;
}

interface HealthInfo {
  status: string;
  rpc: boolean;
  rpc_height: number | null;
  electrumx: boolean;
  db: boolean;
  indexer_height: number | null;
}

/* ── Full-screen Lighthouse Portal ────────────────────────────────── */
export function NodeSyncingBanner() {
  const [syncInfo, setSyncInfo] = useState<SyncInfo | null>(null);
  const [healthInfo, setHealthInfo] = useState<HealthInfo | null>(null);
  const [liveHeight, setLiveHeight] = useState<number>(0);
  const [visible, setVisible] = useState(true);
  const [exiting, setExiting] = useState(false);
  const [bps, setBps] = useState<number>(0);
  const blockCounter = useRef<number>(0);
  const bpsInterval = useRef<ReturnType<typeof setInterval>>();
  const sseRef = useRef<EventSource | null>(null);

  // Poll indexer status for sync progress only — never touches the service
  // health chips so the real /health endpoint values don't get overwritten.
  const fetchStatus = useCallback(async () => {
    try {
      const idx = await api.indexerStatus();
      setSyncInfo(idx);
      if (idx.last_height) setLiveHeight(prev => Math.max(prev, idx.last_height!));
      // Update rpc_height and indexer_height from the indexer response, but
      // leave rpc / electrumx / db untouched — those come from /health below.
      setHealthInfo(prev => ({
        status: prev?.status ?? 'ok',
        rpc: prev?.rpc ?? false,
        rpc_height: idx.tip ?? prev?.rpc_height ?? null,
        electrumx: prev?.electrumx ?? false,
        db: prev?.db ?? true,
        indexer_height: idx.last_height ?? prev?.indexer_height ?? null,
      }));
    } catch { /* ignore */ }
  }, []);

  // Fetch real health every 10 s — this is the sole source of truth for
  // the RPC / ElectrumX / DB chips. Runs immediately on mount too.
  useEffect(() => {
    const checkHealth = async () => {
      try {
        const h = await api.health();
        setHealthInfo(prev => ({
          status: h.status,
          rpc: h.rpc,
          rpc_height: h.rpc_height ?? prev?.rpc_height ?? null,
          electrumx: h.electrumx,
          db: h.db,
          indexer_height: h.indexer_height ?? prev?.indexer_height ?? null,
        }));
      } catch { /* ignore */ }
    };
    checkHealth();
    const t = setInterval(checkHealth, 10_000);
    return () => clearInterval(t);
  }, []);

  // SSE connection for real-time block events
  // Always use a relative URL — this component is 'use client' so it only
  // runs in the browser, and relative URLs stay on the same HTTPS origin.
  useEffect(() => {
    const sseUrl = '/api/sse';

    function connect() {
      if (sseRef.current) {
        sseRef.current.close();
      }

      const es = new EventSource(sseUrl);
      sseRef.current = es;

      es.addEventListener('snapshot', (e) => {
        try {
          const data = JSON.parse(e.data);
          if (data.tip?.height) {
            setLiveHeight(prev => Math.max(prev, data.tip.height));
          }
        } catch {}
      });

      es.addEventListener('block', (e) => {
        try {
          const data = JSON.parse(e.data);
          const height = data.height;
          if (height) {
            setLiveHeight(prev => Math.max(prev, height));
            blockCounter.current += 1;
          }
        } catch {}
      });

      // Live sync progress from the indexer (fires every chunk)
      es.addEventListener('sync', (e) => {
        try {
          const data = JSON.parse(e.data);
          if (data.last_height) {
            setLiveHeight(data.last_height);
            setSyncInfo({
              phase: data.phase,
              last_height: data.last_height,
              tip: data.tip,
            });
            blockCounter.current += 8;
          }
        } catch {}
      });

      es.onerror = () => {
        es.close();
        setTimeout(connect, 3000);
      };
    }

    connect();

    return () => {
      sseRef.current?.close();
    };
  }, []);

  // Calculate blocks-per-second every second
  useEffect(() => {
    bpsInterval.current = setInterval(() => {
      setBps(blockCounter.current);
      blockCounter.current = 0;
    }, 1000);
    return () => clearInterval(bpsInterval.current);
  }, []);

  // Poll status every 2 seconds for phase changes & service health
  useEffect(() => {
    fetchStatus();
    const id = setInterval(fetchStatus, 2000);
    return () => clearInterval(id);
  }, [fetchStatus]);

  // Calculate sync progress (needed before the visibility check)
  const indexedHeight = liveHeight || syncInfo?.last_height || healthInfo?.indexer_height || 0;
  const tipHeight = syncInfo?.tip ?? healthInfo?.rpc_height ?? 0;
  const progress = tipHeight > 0 ? Math.min(100, (indexedHeight / tipHeight) * 100) : 0;
  const blocksRemaining = Math.max(0, tipHeight - indexedHeight);

  const backendHealthy = healthInfo?.status === 'ok' || healthInfo?.rpc === true || healthInfo?.db === true;
  const caughtUp = tipHeight > 0 && blocksRemaining < 100;

  // Determine if we should show the portal
  const phase = syncInfo?.phase ?? 'starting';
  // Consider "live" if the backend says so, OR if we're nearly caught up and
  // the backend health check is already good. This prevents the overlay from
  // lingering when ElectrumX is still false but the node is otherwise usable.
  const isLive = phase === 'live' || (backendHealthy && caughtUp);

  useEffect(() => {
    if (isLive && visible && !exiting) {
      setExiting(true);
      // Wait for exit animation, then hide
      setTimeout(() => setVisible(false), 1200);
    }
  }, [isLive, visible, exiting]);

  if (!visible) return null;

  // ETA calculation
  const etaMinutes = bps > 0 ? Math.ceil(blocksRemaining / bps / 60) : null;
  const etaStr = etaMinutes !== null
    ? etaMinutes > 60
      ? `~${Math.floor(etaMinutes / 60)}h ${etaMinutes % 60}m`
      : `~${etaMinutes}m`
    : null;

  const phaseLabel: Record<string, string> = {
    starting: 'Initializing Node',
    syncing: 'Syncing Blockchain',
    live: 'Fully Synced',
    rpc_offline: 'Node Offline',
    db_locked: 'Database Busy',
  };

  const label = phaseLabel[phase] ?? phase;
  const isSyncing = phase === 'syncing' || phase === 'starting';

  return (
    <div
      className={`portal-overlay ${exiting ? 'portal-exit' : 'portal-enter'}`}
      style={{
        position: 'fixed',
        inset: 0,
        zIndex: 9999,
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        justifyContent: 'center',
        background: 'radial-gradient(ellipse at center, #0a1426 0%, #050a14 50%, #020510 100%)',
        overflow: 'hidden',
      }}
    >
      {/* Star field particles */}
      <div className="portal-stars" />

      {/* Lighthouse beam sweeper */}
      <div className="portal-beam" />

      {/* Outer orbit ring 1 */}
      <div className="portal-ring portal-ring-1" />
      {/* Outer orbit ring 2 */}
      <div className="portal-ring portal-ring-2" />
      {/* Outer orbit ring 3 */}
      <div className="portal-ring portal-ring-3" />

      {/* Central glow */}
      <div className="portal-glow" />

      {/* Logo */}
      <div className="portal-logo sync-float">
        <Image
          src="/logo.png"
          alt="Interchained"
          width={120}
          height={120}
          className="rounded-full"
          style={{ width: 120, height: 120 }}
          priority
          unoptimized
        />
      </div>

      {/* Phase label */}
      <div className="portal-label" style={{ marginTop: 48 }}>
        <span className="portal-phase-text">{label}</span>
        {isSyncing && <BounceDots />}
      </div>

      {/* Progress section */}
      {tipHeight > 0 && (
        <div className="portal-stats">
          {/* Progress bar */}
          <div className="portal-progress-track">
            <div
              className="portal-progress-fill"
              style={{ width: `${progress}%` }}
            />
            <div
              className="portal-progress-glow"
              style={{ left: `${progress}%` }}
            />
          </div>

          {/* Stats row */}
          <div className="portal-stats-row">
            <div className="portal-stat">
              <span className="portal-stat-label">Block</span>
              <span className="portal-stat-value mono">
                {formatNumber(indexedHeight)} / {formatNumber(tipHeight)}
              </span>
            </div>
            <div className="portal-stat">
              <span className="portal-stat-label">Progress</span>
              <span className="portal-stat-value mono">{progress.toFixed(1)}%</span>
            </div>
            {bps > 0 && (
              <div className="portal-stat">
                <span className="portal-stat-label">Speed</span>
                <span className="portal-stat-value mono">{bps} blk/s</span>
              </div>
            )}
            {etaStr && (
              <div className="portal-stat">
                <span className="portal-stat-label">ETA</span>
                <span className="portal-stat-value mono">{etaStr}</span>
              </div>
            )}
          </div>

          {/* Remaining blocks */}
          <div className="portal-remaining mono">
            {formatNumber(blocksRemaining)} blocks remaining
          </div>
        </div>
      )}

      {/* Service status chips */}
      <div className="portal-services">
        <ServiceChip label="RPC" ok={healthInfo?.rpc ?? false} />
        <ServiceChip label="ElectrumX" ok={healthInfo?.electrumx ?? false} />
        <ServiceChip label="DB" ok={healthInfo?.db ?? true} />
      </div>

      {/* Bottom brand */}
      <div className="portal-brand">
        <span style={{ fontSize: 11, letterSpacing: '0.25em', textTransform: 'uppercase', color: 'rgba(138,152,179,0.5)' }}>
          Interchained Vision
        </span>
      </div>
    </div>
  );
}

/* ── Helper components ─────────────────────────────────────────────── */
function BounceDots() {
  return (
    <span className="flex items-center gap-1 ml-2">
      {[1, 2, 3].map((i) => (
        <span
          key={i}
          className={`sync-dot-${i} inline-block w-1.5 h-1.5 rounded-full`}
          style={{ background: 'var(--color-accent)' }}
        />
      ))}
    </span>
  );
}

function ServiceChip({ label, ok }: { label: string; ok: boolean }) {
  return (
    <div
      className="portal-service-chip"
      style={{
        borderColor: ok ? 'rgba(46,204,113,0.4)' : 'rgba(255,107,107,0.3)',
      }}
    >
      <span
        className="portal-service-dot"
        style={{
          background: ok ? 'var(--color-success)' : 'var(--color-danger)',
          boxShadow: ok ? '0 0 6px var(--color-success)' : '0 0 6px var(--color-danger)',
        }}
      />
      <span style={{ color: ok ? 'var(--color-success)' : 'var(--color-danger)' }}>
        {label}
      </span>
    </div>
  );
}

/* ── Compatibility exports (used by other pages) ───────────────────── */
export function NodeSyncingPage() {
  return (
    <div className="flex flex-col items-center justify-center py-20 gap-4">
      <div className="w-16 h-16 rounded-full border-2 border-[var(--color-accent)] border-t-transparent animate-spin" />
      <p className="text-sm text-[var(--color-text-dim)]">Loading data…</p>
    </div>
  );
}

export function NodeSyncingInline() {
  return (
    <div className="flex items-center justify-center py-8 gap-3">
      <div className="w-5 h-5 rounded-full border-2 border-[var(--color-accent)] border-t-transparent animate-spin" />
      <span className="text-sm text-[var(--color-text-dim)]">Syncing…</span>
    </div>
  );
}

