// Display formatters shared across tables and panels. Accept `unknown`
// because most callers pass ag-grid `valueFormatter` params, which are
// typed loosely.

function toFiniteNumber(value: unknown): number | null {
  if (value === null || value === undefined) return null;
  if (typeof value === 'number') return Number.isFinite(value) ? value : null;
  if (typeof value === 'string') {
    const trimmed = value.trim();
    if (trimmed === '') return null;
    const parsed = Number(trimmed);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

export function formatBytes(value: unknown): string {
  const bytes = toFiniteNumber(value);
  if (bytes === null) return '-';
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.min(Math.floor(Math.log(bytes) / Math.log(k)), sizes.length - 1);
  return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
}

export function formatNumber(value: unknown): string {
  const num = toFiniteNumber(value);
  if (num === null) return '-';
  if (num >= 1_000_000) return (num / 1_000_000).toFixed(1) + 'M';
  if (num >= 1_000) return (num / 1_000).toFixed(1) + 'K';
  return num.toLocaleString();
}

export function formatDuration(value: unknown): string {
  const ms = toFiniteNumber(value);
  if (ms === null) return '-';
  if (ms >= 1000) return (ms / 1000).toFixed(2) + 's';
  return ms.toFixed(0) + 'ms';
}

// ClickHouse DateTime values arrive as naive 'YYYY-MM-DD HH:MM:SS' strings with
// no zone attached. The server pins its session timezone to UTC so they always
// denote UTC, but `new Date(...)` reads that shape as *browser-local* time,
// shifting every timestamp by the viewer's UTC offset. Parse them explicitly so
// a viewer in any timezone sees the same instant, rendered in their own zone.
// Values that already carry a zone (ISO strings ending in Z or +hh:mm) and epoch
// numbers are passed through untouched.
const NAIVE_TIMESTAMP = /^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?$/;

export function parseServerTime(value: unknown): Date {
  if (value instanceof Date) return value;
  if (typeof value === 'number') return new Date(value);
  if (typeof value !== 'string' || value === '') return new Date(NaN);
  const s = value.trim();
  return NAIVE_TIMESTAMP.test(s) ? new Date(`${s.replace(' ', 'T')}Z`) : new Date(s);
}

// datetime-local inputs are read back by the browser as local wall-clock time,
// so they must be *written* as local wall-clock too. toISOString() would write
// UTC into a local-time field, shifting the range on every round trip.
export function toDateTimeLocalValue(date: Date): string {
  const pad = (n: number) => String(n).padStart(2, '0');
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}` +
    `T${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`;
}

export function formatDateTime(value: string | null | undefined): string {
  if (!value) return '-';
  const d = parseServerTime(value);
  if (Number.isNaN(d.getTime())) return '-';
  return d.toLocaleString('en-US', {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  });
}
