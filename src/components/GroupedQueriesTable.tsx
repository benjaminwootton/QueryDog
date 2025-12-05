import { useEffect, useState } from 'react';
import { ChevronUp, ChevronDown, Loader2, Copy, Check } from 'lucide-react';
import { useQueryStore } from '../stores/queryStore';
import { fetchGroupedQueryLog, type GroupedQueryEntry } from '../services/api';

function formatDuration(ms: number): string {
  if (ms < 1000) return `${Math.round(ms)}ms`;
  if (ms < 60000) return `${(ms / 1000).toFixed(2)}s`;
  return `${(ms / 60000).toFixed(2)}m`;
}

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
}

function formatNumber(num: number): string {
  if (num >= 1000000) return (num / 1000000).toFixed(1) + 'M';
  if (num >= 1000) return (num / 1000).toFixed(1) + 'K';
  return num.toLocaleString();
}

function formatDateTime(dateStr: string): string {
  const date = new Date(dateStr);
  return date.toLocaleString('en-US', {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });
}

interface ColumnDef {
  field: keyof GroupedQueryEntry;
  label: string;
  width: string;
  sortable: boolean;
  format?: (value: unknown, row: GroupedQueryEntry) => React.ReactNode;
}

const columns: ColumnDef[] = [
  {
    field: 'example_query',
    label: 'Query',
    width: 'min-w-[400px]',
    sortable: false,
    format: (value) => {
      const query = String(value || '');
      return (
        <div className="font-mono text-[10px] text-gray-300 truncate max-w-[600px]" title={query}>
          {query.slice(0, 200)}{query.length > 200 ? '...' : ''}
        </div>
      );
    },
  },
  {
    field: 'count',
    label: 'Count',
    width: 'w-20',
    sortable: true,
    format: (value) => (
      <span className="text-blue-400 font-medium">{formatNumber(Number(value))}</span>
    ),
  },
  {
    field: 'avg_duration',
    label: 'Avg Duration',
    width: 'w-24',
    sortable: true,
    format: (value) => formatDuration(Number(value)),
  },
  {
    field: 'max_duration',
    label: 'Max Duration',
    width: 'w-24',
    sortable: true,
    format: (value) => (
      <span className="text-orange-400">{formatDuration(Number(value))}</span>
    ),
  },
  {
    field: 'total_duration',
    label: 'Total Duration',
    width: 'w-24',
    sortable: true,
    format: (value) => formatDuration(Number(value)),
  },
  {
    field: 'avg_memory',
    label: 'Avg Memory',
    width: 'w-24',
    sortable: true,
    format: (value) => formatBytes(Number(value)),
  },
  {
    field: 'max_memory',
    label: 'Max Memory',
    width: 'w-24',
    sortable: true,
    format: (value) => (
      <span className="text-purple-400">{formatBytes(Number(value))}</span>
    ),
  },
  {
    field: 'avg_read_rows',
    label: 'Avg Rows Read',
    width: 'w-24',
    sortable: true,
    format: (value) => formatNumber(Number(value)),
  },
  {
    field: 'total_read_bytes',
    label: 'Total Read',
    width: 'w-24',
    sortable: true,
    format: (value) => formatBytes(Number(value)),
  },
  {
    field: 'user',
    label: 'User',
    width: 'w-24',
    sortable: false,
  },
  {
    field: 'first_seen',
    label: 'First Seen',
    width: 'w-32',
    sortable: true,
    format: (value) => formatDateTime(String(value)),
  },
  {
    field: 'last_seen',
    label: 'Last Seen',
    width: 'w-32',
    sortable: true,
    format: (value) => formatDateTime(String(value)),
  },
];

function CopyButton({ text }: { text: string }) {
  const [copied, setCopied] = useState(false);

  const handleCopy = async () => {
    await navigator.clipboard.writeText(text);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <button
      onClick={handleCopy}
      className="p-1 hover:bg-gray-600 rounded text-gray-400 hover:text-white"
      title="Copy query"
    >
      {copied ? <Check className="w-3 h-3 text-green-400" /> : <Copy className="w-3 h-3" />}
    </button>
  );
}

export function GroupedQueriesTable() {
  const {
    timeRange,
    search,
    fieldFilters,
    rangeFilters,
    groupedEntries,
    groupedLoading,
    groupedSortField,
    groupedSortOrder,
    setGroupedEntries,
    setGroupedLoading,
    setGroupedSortField,
    setGroupedSortOrder,
  } = useQueryStore();

  const [expandedRow, setExpandedRow] = useState<string | null>(null);

  // Fetch grouped data
  useEffect(() => {
    setGroupedLoading(true);
    fetchGroupedQueryLog(
      timeRange,
      search,
      groupedSortField,
      groupedSortOrder,
      fieldFilters,
      rangeFilters,
      1000
    )
      .then(setGroupedEntries)
      .catch(console.error)
      .finally(() => setGroupedLoading(false));
  }, [timeRange, search, fieldFilters, rangeFilters, groupedSortField, groupedSortOrder, setGroupedEntries, setGroupedLoading]);

  const handleSort = (field: string) => {
    if (field === groupedSortField) {
      setGroupedSortOrder(groupedSortOrder === 'DESC' ? 'ASC' : 'DESC');
    } else {
      setGroupedSortField(field);
      setGroupedSortOrder('DESC');
    }
  };

  if (groupedLoading) {
    return (
      <div className="h-full flex items-center justify-center">
        <Loader2 className="w-8 h-8 text-blue-500 animate-spin" />
      </div>
    );
  }

  return (
    <div className="h-full flex flex-col">
      <div className="text-xs text-gray-400 mb-2">
        {groupedEntries.length.toLocaleString()} unique queries
      </div>
      <div className="flex-1 overflow-auto border border-gray-700 rounded">
        <table className="w-full text-xs">
          <thead className="bg-gray-800 sticky top-0 z-10">
            <tr>
              {columns.map((col) => (
                <th
                  key={col.field}
                  className={`${col.width} px-2 py-2 text-left text-gray-400 font-medium border-b border-gray-700 ${
                    col.sortable ? 'cursor-pointer hover:bg-gray-700' : ''
                  }`}
                  onClick={() => col.sortable && handleSort(col.field)}
                >
                  <div className="flex items-center gap-1">
                    {col.label}
                    {col.sortable && groupedSortField === col.field && (
                      groupedSortOrder === 'DESC' ? (
                        <ChevronDown className="w-3 h-3" />
                      ) : (
                        <ChevronUp className="w-3 h-3" />
                      )
                    )}
                  </div>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {groupedEntries.map((entry, idx) => (
              <>
                <tr
                  key={entry.normalized_query_hash}
                  className={`border-b border-gray-800 hover:bg-gray-800/50 cursor-pointer ${
                    expandedRow === entry.normalized_query_hash ? 'bg-gray-800' : ''
                  } ${idx % 2 === 0 ? 'bg-gray-900/30' : ''}`}
                  onClick={() =>
                    setExpandedRow(
                      expandedRow === entry.normalized_query_hash
                        ? null
                        : entry.normalized_query_hash
                    )
                  }
                >
                  {columns.map((col) => (
                    <td key={col.field} className={`${col.width} px-2 py-1.5 text-gray-300`}>
                      {col.format
                        ? col.format(entry[col.field], entry)
                        : String(entry[col.field] ?? '')}
                    </td>
                  ))}
                </tr>
                {expandedRow === entry.normalized_query_hash && (
                  <tr key={`${entry.normalized_query_hash}-expanded`}>
                    <td colSpan={columns.length} className="bg-gray-800/70 p-4">
                      <div className="space-y-3">
                        <div className="flex items-start justify-between gap-4">
                          <div className="flex-1">
                            <div className="text-[10px] text-gray-500 uppercase mb-1">
                              Example Query
                            </div>
                            <pre className="font-mono text-[10px] text-gray-300 whitespace-pre-wrap bg-gray-900 p-3 rounded border border-gray-700 max-h-48 overflow-auto">
                              {entry.example_query}
                            </pre>
                          </div>
                          <CopyButton text={entry.example_query} />
                        </div>
                        <div className="grid grid-cols-6 gap-4 text-[10px]">
                          <div>
                            <div className="text-gray-500 uppercase">Database</div>
                            <div className="text-gray-300">{entry.current_database || 'N/A'}</div>
                          </div>
                          <div>
                            <div className="text-gray-500 uppercase">Min Duration</div>
                            <div className="text-green-400">{formatDuration(entry.min_duration)}</div>
                          </div>
                          <div>
                            <div className="text-gray-500 uppercase">Total Memory</div>
                            <div className="text-gray-300">{formatBytes(entry.total_memory)}</div>
                          </div>
                          <div>
                            <div className="text-gray-500 uppercase">Total Read Rows</div>
                            <div className="text-gray-300">{formatNumber(entry.total_read_rows)}</div>
                          </div>
                          <div>
                            <div className="text-gray-500 uppercase">Avg Result Rows</div>
                            <div className="text-gray-300">{formatNumber(entry.avg_result_rows)}</div>
                          </div>
                          <div>
                            <div className="text-gray-500 uppercase">Total Result Rows</div>
                            <div className="text-gray-300">{formatNumber(entry.total_result_rows)}</div>
                          </div>
                        </div>
                      </div>
                    </td>
                  </tr>
                )}
              </>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
