'use client';

/**
 * NEDB showcase page — three panels demonstrating the unique capabilities of
 * the NEDB engine powering Vision: NQL queries, time-travel by sequence
 * number, and BLAKE2b hash-chain integrity verification.
 *
 * Visual register: dark terminal aesthetic (#0d1117 / #00ff88), monospace
 * throughout. Intentionally distinct from the rest of the Vision UI to
 * communicate "you are below the explorer, looking at the engine".
 */

import { useState, useEffect, useMemo, Suspense } from 'react';
import { useSearchParams } from 'next/navigation';
import { NqlConsole } from '@/components/nql-console';

// ── Visual primitives ──────────────────────────────────────────────────────

const COLORS = {
  bg: '#0d1117',
  surface: '#161b22',
  border: '#30363d',
  borderSoft: '#21262d',
  text: '#c9d1d9',
  dim: '#8b949e',
  accent: '#00ff88',
  danger: '#ff4444',
  ok: '#00ff88',
} as const;

const MONO = 'ui-monospace, SFMono-Regular, Menlo, Consolas, monospace';

function Panel({ title, subtitle, children }: { title: string; subtitle: string; children: React.ReactNode }) {
  return (
    <section
      style={{
        background: COLORS.bg,
        border: `1px solid ${COLORS.border}`,
        borderRadius: 10,
        padding: 24,
        marginBottom: 24,
        fontFamily: MONO,
        color: COLORS.text,
      }}
    >
      <h2
        style={{
          margin: 0,
          color: COLORS.accent,
          fontSize: 20,
          letterSpacing: 0.5,
        }}
      >
        {title}
      </h2>
      <p style={{ margin: '6px 0 18px 0', color: COLORS.dim, fontSize: 13 }}>
        {subtitle}
      </p>
      {children}
    </section>
  );
}

// ── Panel 2 — Time Travel ─────────────────────────────────────────────────

type HistoryRow = Record<string, unknown>;

function TimeTravelPanel() {
  const [tokenId, setTokenId] = useState<string>('');
  const [asOf, setAsOf] = useState<string>('');
  const [rows, setRows] = useState<HistoryRow[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState<boolean>(false);
  const [head, setHead] = useState<{ seq?: number; head?: string }>({});

  async function rewind(): Promise<void> {
    if (!tokenId.trim()) {
      setError('Token id is required.');
      return;
    }
    setLoading(true);
    setError(null);
    try {
      const params = new URLSearchParams({ limit: '50' });
      if (asOf.trim()) params.set('as_of', asOf.trim());
      const res = await fetch(
        `/api/nedb/token-history/${encodeURIComponent(tokenId.trim())}?${params.toString()}`,
      );
      if (!res.ok) {
        const text = await res.text();
        throw new Error(`HTTP ${res.status}: ${text || res.statusText}`);
      }
      const data = await res.json();
      setRows((data.rows ?? []) as HistoryRow[]);
      setHead({ seq: data.seq, head: data.head });
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      setRows([]);
    } finally {
      setLoading(false);
    }
  }

  const columns = useMemo(() => {
    if (rows.length === 0) return [] as string[];
    // Prefer timestamp + op + key fields if present, then the rest.
    const all = new Set<string>();
    for (const r of rows) for (const k of Object.keys(r)) all.add(k);
    const preferred = ['timestamp', 'time', 'ts', 'op', 'action', 'amount', 'from', 'to', 'seq'];
    const ordered = preferred.filter((p) => all.has(p));
    for (const k of Array.from(all)) {
      if (!ordered.includes(k)) ordered.push(k);
    }
    return ordered;
  }, [rows]);

  return (
    <Panel
      title="Time-Travel — AS OF Sequence"
      subtitle="Rewind Vision to any point in the chain's history."
    >
      <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', marginBottom: 12 }}>
        <input
          value={tokenId}
          onChange={(e) => setTokenId(e.target.value)}
          placeholder="Token id"
          style={{
            flex: '2 1 240px',
            background: '#000',
            color: COLORS.accent,
            border: `1px solid ${COLORS.border}`,
            borderRadius: 4,
            padding: '8px 10px',
            fontFamily: MONO,
            fontSize: 13,
            outline: 'none',
          }}
        />
        <input
          value={asOf}
          onChange={(e) => setAsOf(e.target.value)}
          placeholder="Sequence number (AS OF)"
          type="number"
          style={{
            flex: '1 1 200px',
            background: '#000',
            color: COLORS.accent,
            border: `1px solid ${COLORS.border}`,
            borderRadius: 4,
            padding: '8px 10px',
            fontFamily: MONO,
            fontSize: 13,
            outline: 'none',
          }}
        />
        <button
          onClick={rewind}
          disabled={loading}
          style={{
            background: COLORS.accent,
            color: '#000',
            border: 'none',
            padding: '8px 18px',
            borderRadius: 4,
            fontFamily: MONO,
            fontWeight: 700,
            cursor: loading ? 'wait' : 'pointer',
            opacity: loading ? 0.6 : 1,
          }}
        >
          {loading ? 'Rewinding…' : 'Rewind'}
        </button>
      </div>

      {(head.seq !== undefined || head.head) && (
        <div style={{ fontSize: 12, color: COLORS.dim, marginBottom: 8 }}>
          {head.seq !== undefined && <>seq={head.seq} </>}
          {head.head && <>· head={head.head.slice(0, 12)}…</>}
        </div>
      )}

      {error && (
        <div
          style={{
            background: '#2d0d0d',
            border: `1px solid ${COLORS.danger}`,
            color: '#ff8888',
            padding: 10,
            borderRadius: 4,
            fontSize: 12,
          }}
        >
          {error}
        </div>
      )}

      {rows.length > 0 && (
        <div
          style={{
            border: `1px solid ${COLORS.border}`,
            borderRadius: 4,
            overflow: 'auto',
            maxHeight: 440,
          }}
        >
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
            <thead>
              <tr style={{ background: COLORS.surface }}>
                {columns.map((c) => (
                  <th
                    key={c}
                    style={{
                      textAlign: 'left',
                      padding: '8px 10px',
                      color: COLORS.accent,
                      borderBottom: `1px solid ${COLORS.border}`,
                      position: 'sticky',
                      top: 0,
                      background: COLORS.surface,
                    }}
                  >
                    {c}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map((r, i) => (
                <tr key={i} style={{ borderBottom: `1px solid ${COLORS.borderSoft}` }}>
                  {columns.map((c) => {
                    const v = r[c];
                    const text =
                      v === null || v === undefined
                        ? ''
                        : typeof v === 'object'
                        ? JSON.stringify(v)
                        : String(v);
                    return (
                      <td
                        key={c}
                        style={{
                          padding: '6px 10px',
                          color: COLORS.text,
                          whiteSpace: 'pre-wrap',
                          wordBreak: 'break-all',
                          verticalAlign: 'top',
                        }}
                      >
                        {text}
                      </td>
                    );
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {rows.length === 0 && !error && !loading && (
        <div style={{ color: COLORS.dim, fontSize: 12, fontStyle: 'italic' }}>
          Enter a token id and (optionally) a sequence number, then click Rewind.
        </div>
      )}
    </Panel>
  );
}

// ── Panel 3 — Tamper Verify ───────────────────────────────────────────────

type VerifyResult = {
  ok: boolean;
  seq?: number;
  head?: string;
  tamper_evident?: boolean;
};

function TamperVerifyPanel() {
  const [result, setResult] = useState<VerifyResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState<boolean>(false);

  async function verify(): Promise<void> {
    setLoading(true);
    setError(null);
    try {
      const res = await fetch('/api/nedb/verify');
      if (!res.ok) {
        const text = await res.text();
        throw new Error(`HTTP ${res.status}: ${text || res.statusText}`);
      }
      const data = (await res.json()) as VerifyResult;
      setResult(data);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      setResult(null);
    } finally {
      setLoading(false);
    }
  }

  return (
    <Panel
      title="Tamper-Evident Chain — BLAKE2b Merkle Roots"
      subtitle="Every write is hash-chained. This panel verifies the chain is intact."
    >
      <button
        onClick={verify}
        disabled={loading}
        style={{
          background: COLORS.accent,
          color: '#000',
          border: 'none',
          padding: '10px 22px',
          borderRadius: 4,
          fontFamily: MONO,
          fontWeight: 700,
          cursor: loading ? 'wait' : 'pointer',
          opacity: loading ? 0.6 : 1,
        }}
      >
        {loading ? 'Verifying…' : 'Verify Now'}
      </button>

      {error && (
        <div
          style={{
            marginTop: 14,
            background: '#2d0d0d',
            border: `1px solid ${COLORS.danger}`,
            color: '#ff8888',
            padding: 10,
            borderRadius: 4,
            fontSize: 12,
          }}
        >
          {error}
        </div>
      )}

      {result && (
        <div
          style={{
            marginTop: 16,
            padding: 16,
            border: `1px solid ${result.ok ? COLORS.ok : COLORS.danger}`,
            borderRadius: 6,
            background: result.ok ? '#082b16' : '#2d0d0d',
          }}
        >
          <div
            style={{
              fontSize: 18,
              color: result.ok ? COLORS.ok : COLORS.danger,
              fontWeight: 700,
              marginBottom: 8,
            }}
          >
            {result.ok ? 'Chain Intact' : 'Tampering Detected'}
          </div>
          <div style={{ fontSize: 13, color: COLORS.text }}>
            <div>seq: <span style={{ color: COLORS.accent }}>{result.seq ?? '—'}</span></div>
            <div>
              head:{' '}
              <span style={{ color: COLORS.accent, wordBreak: 'break-all' }}>
                {result.head ?? '—'}
              </span>
            </div>
            {result.tamper_evident && (
              <div style={{ color: COLORS.dim, marginTop: 6 }}>
                Hash-chain: BLAKE2b · every put/del extends the Merkle root.
              </div>
            )}
          </div>
        </div>
      )}
    </Panel>
  );
}

// ── Panel 1 — NQL Console ─────────────────────────────────────────────────

function NqlConsolePanel() {
  const search = useSearchParams();
  const queryParam = search?.get('query') ?? undefined;
  return (
    <Panel
      title="NQL Console — Query the Chain"
      subtitle="Run NQL queries directly against the NEDB engine powering Vision."
    >
      <NqlConsole defaultQuery={queryParam} />
    </Panel>
  );
}

// ── Page root ─────────────────────────────────────────────────────────────

export default function NedbPage() {
  // Lock the body background while this page is mounted so the terminal
  // aesthetic doesn't fight the rest of the app's theme.
  useEffect(() => {
    const prev = document.body.style.background;
    document.body.style.background = '#0a0d12';
    return () => {
      document.body.style.background = prev;
    };
  }, []);

  return (
    <main
      style={{
        maxWidth: 1100,
        margin: '0 auto',
        padding: '32px 24px',
        fontFamily: MONO,
        color: COLORS.text,
      }}
    >
      <header style={{ marginBottom: 28 }}>
        <h1
          style={{
            margin: 0,
            color: COLORS.accent,
            fontSize: 30,
            letterSpacing: 1,
          }}
        >
          NEDB · Vision Engine
        </h1>
        <p style={{ color: COLORS.dim, marginTop: 8, fontSize: 14 }}>
          Direct access to the database powering Interchained Vision. Query, rewind, and verify.
        </p>
      </header>

      <Suspense
        fallback={
          <Panel
            title="NQL Console — Query the Chain"
            subtitle="Run NQL queries directly against the NEDB engine powering Vision."
          >
            <div style={{ color: COLORS.dim }}>Loading…</div>
          </Panel>
        }
      >
        <NqlConsolePanel />
      </Suspense>
      <TimeTravelPanel />
      <TamperVerifyPanel />
    </main>
  );
}
