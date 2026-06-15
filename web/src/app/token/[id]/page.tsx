'use client';

import Link from 'next/link';
import { use, useEffect, useState } from 'react';
import { api, friendlyError } from '@/lib/api';
import { CopyButton } from '@/components/copy-button';
import { KeyValue } from '@/components/key-value';
import { Empty } from '@/components/empty';
import { formatNumber, shortHash, timeAgo } from '@/lib/utils';
import { NodeSyncingPage, NodeSyncingInline } from '@/components/node-syncing';
import { useSyncState } from '@/hooks/use-sync';

export default function TokenPage({ params }: { params: Promise<{ id: string }> }) {
  const { id } = use(params);
  const [meta, setMeta] = useState<any>(null);
  const [history, setHistory] = useState<any[]>([]);
  const [error, setError] = useState<string | null>(null);
  const syncing = useSyncState();

  useEffect(() => {
    api.token(id).then(setMeta).catch((e) => setError(friendlyError(e)));
    api.tokenHistory(id, { limit: 100 }).then((r) => setHistory(r.items)).catch(() => {});
  }, [id]);

  if (error) return syncing ? <NodeSyncingPage /> : (
    <div className="card p-8 space-y-3">
      <p className="text-[var(--color-danger)] font-semibold">Unable to load token</p>
      <p className="text-sm text-[var(--color-text-dim)]">{error}</p>
      <button onClick={() => { setError(null); api.token(id).then(setMeta).catch((e) => setError(friendlyError(e))); }} className="text-xs text-[var(--color-accent)] hover:underline">Try again →</button>
    </div>
  );
  if (!meta) return syncing ? <NodeSyncingPage /> : <div className="card p-8 text-[var(--color-text-dim)]">Loading…</div>;

  return (
    <div className="space-y-6">
      <div>
        <div className="flex items-center gap-3 mb-2">
          <span className="chip">ITSL TOKEN</span>
          {meta.verified && <span className="chip chip-gold">VERIFIED</span>}
        </div>
        <div className="flex items-baseline gap-3">
          <h1 className="text-3xl lg:text-4xl font-bold text-[var(--color-gold)]">{meta.symbol}</h1>
          <span className="text-lg text-[var(--color-text-dim)]">{meta.name}</span>
        </div>
        <div className="flex items-center gap-2 mt-2 text-xs text-[var(--color-text-dim)] mono break-all">
          {meta.id}
          <CopyButton value={meta.id} />
        </div>
      </div>

      <div className="grid lg:grid-cols-2 gap-6">
        <div className="card p-5">
          <h2 className="text-sm font-semibold mb-3 text-white">Token Info</h2>
          <KeyValue label="Symbol" value={meta.symbol} />
          <KeyValue label="Name" value={meta.name} />
          <KeyValue label="Decimals" value={String(meta.decimals)} />
          <KeyValue label="Total Supply" value={formatNumber(meta.total_supply, 0)} />
          {meta.creator && (
            <KeyValue
              label="Creator"
              value={<Link href={`/address/${meta.creator}`} className="text-[var(--color-accent)]">{shortHash(meta.creator, 12, 8)}</Link>}
              copy={meta.creator}
            />
          )}
          {meta.created_height && (
            <KeyValue
              label="Created"
              value={<Link href={`/block/${meta.created_height}`} className="text-[var(--color-accent)]">Block #{formatNumber(meta.created_height)}</Link>}
            />
          )}
          {meta.create_txid && (
            <KeyValue
              label="Genesis Tx"
              value={<Link href={`/tx/${meta.create_txid}`} className="text-[var(--color-accent)]">{shortHash(meta.create_txid)}</Link>}
              copy={meta.create_txid}
            />
          )}
        </div>

        <div className="card overflow-hidden">
          <div className="px-5 py-3 border-b border-[var(--color-border)] flex items-center justify-between">
            <h2 className="text-sm font-semibold">Recent Activity</h2>
            <Link
              href={`/nedb?query=${encodeURIComponent(`FROM itsl_ops WHERE token = "${id}" TRACE caused_by`)}`}
              className="text-xs text-[var(--color-accent)] hover:underline mono"
              title="Open this token's causal history in the NEDB Console"
            >
              View in NEDB ↗
            </Link>
          </div>
          {history.length === 0 ? (
            <div className="px-5 py-12">
              {syncing ? <NodeSyncingInline /> : <Empty title="No transfer history yet" />}
            </div>
          ) : (
            <div className="divide-y divide-[var(--color-border-soft)] max-h-[500px] overflow-y-auto">
              {history.map((h, idx) => (
                <div key={`${h.txid || idx}`} className="px-5 py-3 text-xs">
                  <div className="flex items-center justify-between">
                    <span className="chip">{h.op || h.action || 'TRANSFER'}</span>
                    {h.amount && <span className="mono text-[var(--color-gold)]">{h.amount}</span>}
                  </div>
                  {h.txid && (
                    <Link href={`/tx/${h.txid}`} className="block mt-1 mono text-[var(--color-text-dim)] truncate hover:text-[var(--color-accent)]">
                      {shortHash(h.txid)}
                    </Link>
                  )}
                  {h.from && h.to && (
                    <div className="mt-1 text-[var(--color-text-faint)] truncate">
                      {shortHash(h.from, 8, 6)} → {shortHash(h.to, 8, 6)}
                    </div>
                  )}
                </div>
              ))}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
