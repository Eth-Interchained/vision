'use client';

/**
 * NQL Console — terminal-styled NQL query runner for the NEDB layer.
 *
 * Hits `POST /api/nedb/query` with `{nql}` and renders the result table
 * with auto-detected columns. Pure client component; no server-side data
 * dependency.
 */

import { useMemo, useState } from 'react';

type QueryResult = {
  rows: Array<Record<string, unknown>>;
  count: number;
  seq?: number;
  head?: string;
};

type Example = { label: string; query: string };

const DEFAULT_EXAMPLES: Example[] = [
  {
    label: 'Tip state',
    query: 'FROM kv WHERE _id = "vision:tip:height"',
  },
  {
    label: 'Recent blocks (top 20)',
    query: 'FROM zset WHERE _name = "vision:recent:blocks" ORDER BY score DESC LIMIT 20',
  },
  {
    label: 'Block by height',
    query: 'FROM blocks WHERE height = 100000 LIMIT 1',
  },
  {
    label: 'Token history',
    query: 'FROM itsl_ops WHERE token = "0x...tok" ORDER BY seq DESC LIMIT 50',
  },
  {
    label: 'Trace caused_by',
    query: 'FROM itsl_ops WHERE token = "0x...tok" TRACE caused_by',
  },
];

export interface NqlConsoleProps {
  defaultQuery?: string;
  dbName?: string;
  examples?: Example[];
}

function shortHash(h: string | undefined, head = 8, tail = 6): string {
  if (!h) return '';
  if (h.length <= head + tail + 1) return h;
  return `${h.slice(0, head)}…${h.slice(-tail)}`;
}

function detectColumns(rows: Array<Record<string, unknown>>): string[] {
  if (rows.length === 0) return [];
  const cols = new Set<string>();
  // Inspect up to first 20 rows to cover heterogeneous shapes.
  for (const r of rows.slice(0, 20)) {
    for (const k of Object.keys(r)) cols.add(k);
  }
  return Array.from(cols);
}

function renderCell(v: unknown): string {
  if (v === null || v === undefined) return '';
  if (typeof v === 'object') return JSON.stringify(v);
  return String(v);
}

export function NqlConsole({
  defaultQuery,
  dbName,
  examples = DEFAULT_EXAMPLES,
}: NqlConsoleProps) {
  const [query, setQuery] = useState<string>(defaultQuery ?? examples[0]?.query ?? '');
  const [result, setResult] = useState<QueryResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState<boolean>(false);

  const columns = useMemo(
    () => (result ? detectColumns(result.rows) : []),
    [result],
  );

  async function runQuery(): Promise<void> {
    setLoading(true);
    setError(null);
    try {
      const body: Record<string, unknown> = { nql: query };
      if (dbName) body.db = dbName;
      const res = await fetch('/api/nedb/query', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      if (!res.ok) {
        const text = await res.text();
        throw new Error(`HTTP ${res.status}: ${text || res.statusText}`);
      }
      const data = (await res.json()) as QueryResult;
      setResult(data);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      setResult(null);
    } finally {
      setLoading(false);
    }
  }

  function onExampleChange(e: React.ChangeEvent<HTMLSelectElement>): void {
    const idx = Number(e.target.value);
    if (Number.isFinite(idx) && examples[idx]) {
      setQuery(examples[idx].query);
    }
  }

  return (
    <div
      style={{
        background: '#0d1117',
        border: '1px solid #1f2933',
        borderRadius: 8,
        padding: 16,
        fontFamily: 'ui-monospace, SFMono-Regular, Menlo, Consolas, monospace',
        color: '#c9d1d9',
      }}
    >
      <div style={{ display: 'flex', gap: 8, marginBottom: 12, alignItems: 'center' }}>
        <select
          onChange={onExampleChange}
          defaultValue=""
          style={{
            background: '#161b22',
            color: '#00ff88',
            border: '1px solid #30363d',
            padding: '6px 10px',
            borderRadius: 4,
            fontFamily: 'inherit',
            fontSize: 12,
          }}
        >
          <option value="" disabled>
            Examples…
          </option>
          {examples.map((ex, i) => (
            <option key={ex.label} value={i}>
              {ex.label}
            </option>
          ))}
        </select>
        <span style={{ fontSize: 11, color: '#8b949e' }}>
          db={dbName ?? 'vision'}
        </span>
      </div>

      <textarea
        value={query}
        onChange={(e) => setQuery(e.target.value)}
        spellCheck={false}
        rows={4}
        style={{
          width: '100%',
          background: '#000',
          color: '#00ff88',
          border: '1px solid #30363d',
          borderRadius: 4,
          padding: 12,
          fontFamily: 'inherit',
          fontSize: 13,
          resize: 'vertical',
          outline: 'none',
        }}
        placeholder="FROM coll WHERE field = value ..."
      />

      <div style={{ display: 'flex', gap: 8, marginTop: 8, alignItems: 'center' }}>
        <button
          onClick={runQuery}
          disabled={loading || !query.trim()}
          style={{
            background: '#00ff88',
            color: '#000',
            border: 'none',
            padding: '8px 18px',
            borderRadius: 4,
            fontFamily: 'inherit',
            fontWeight: 700,
            cursor: loading ? 'wait' : 'pointer',
            opacity: loading || !query.trim() ? 0.6 : 1,
          }}
        >
          {loading ? 'Running…' : 'Run'}
        </button>
        {result && (
          <span style={{ fontSize: 12, color: '#8b949e' }}>
            rows={result.count}
            {result.seq !== undefined && ` · seq=${result.seq}`}
            {result.head !== undefined && ` · head=${shortHash(result.head)}`}
          </span>
        )}
      </div>

      {error && (
        <div
          style={{
            marginTop: 12,
            background: '#2d0d0d',
            border: '1px solid #ff4444',
            color: '#ff8888',
            padding: 10,
            borderRadius: 4,
            fontSize: 12,
            whiteSpace: 'pre-wrap',
            wordBreak: 'break-all',
          }}
        >
          {error}
        </div>
      )}

      {result && result.rows.length > 0 && (
        <div
          style={{
            marginTop: 12,
            border: '1px solid #30363d',
            borderRadius: 4,
            overflow: 'auto',
            maxHeight: 480,
          }}
        >
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
            <thead>
              <tr style={{ background: '#161b22' }}>
                {columns.map((c) => (
                  <th
                    key={c}
                    style={{
                      textAlign: 'left',
                      padding: '8px 10px',
                      color: '#00ff88',
                      borderBottom: '1px solid #30363d',
                      position: 'sticky',
                      top: 0,
                      background: '#161b22',
                    }}
                  >
                    {c}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {result.rows.map((row, i) => (
                <tr key={i} style={{ borderBottom: '1px solid #21262d' }}>
                  {columns.map((c) => (
                    <td
                      key={c}
                      style={{
                        padding: '6px 10px',
                        color: '#c9d1d9',
                        verticalAlign: 'top',
                        whiteSpace: 'pre-wrap',
                        wordBreak: 'break-all',
                      }}
                    >
                      {renderCell(row[c])}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {result && result.rows.length === 0 && (
        <div
          style={{
            marginTop: 12,
            padding: 12,
            color: '#8b949e',
            fontSize: 12,
            fontStyle: 'italic',
          }}
        >
          0 rows returned.
        </div>
      )}
    </div>
  );
}

export default NqlConsole;
