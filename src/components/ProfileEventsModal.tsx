import { useState } from 'react';
import { X, Copy, Check, ExternalLink } from 'lucide-react';
import { useQueryStore } from '../stores/queryStore';

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

function formatNumber(num: number): string {
  return num.toLocaleString();
}

interface ArrayDisplayProps {
  items: string[];
  label: string;
}

function ArrayDisplay({ items, label }: ArrayDisplayProps) {
  if (!items || items.length === 0) return null;
  return (
    <div className="mb-3">
      <h4 className="text-xs font-semibold text-gray-400 mb-1">{label}</h4>
      <div className="flex flex-wrap gap-1">
        {items.map((item, idx) => (
          <span key={idx} className="px-1.5 py-0.5 bg-gray-700 rounded text-xs text-gray-300">
            {item}
          </span>
        ))}
      </div>
    </div>
  );
}

export function ProfileEventsModal() {
  const { selectedEntry, setSelectedEntry } = useQueryStore();
  const [copied, setCopied] = useState(false);
  const [queryCopied, setQueryCopied] = useState(false);

  if (!selectedEntry) return null;

  const profileEvents = (selectedEntry.ProfileEvents || {}) as Record<string, number>;
  const settings = (selectedEntry.Settings || {}) as Record<string, string>;

  const sortedEvents = Object.entries(profileEvents)
    .filter(([, value]) => value > 0)
    .sort((a, b) => b[1] - a[1]);

  const sortedSettings = Object.entries(settings).sort((a, b) => a[0].localeCompare(b[0]));

  const handleAnalyseQuery = () => {
    const query = selectedEntry.query as string;
    // Use the global function exposed by App.tsx to open the query editor
    const openQueryEditor = (window as unknown as { openQueryEditor?: (query: string) => void }).openQueryEditor;
    if (openQueryEditor) {
      openQueryEditor(query);
      handleClose();
    }
  };

  const handleClose = () => {
    setSelectedEntry(null);
    setCopied(false);
    setQueryCopied(false);
  };

  const handleCopyQuery = async () => {
    await navigator.clipboard.writeText(String(selectedEntry.query || ''));
    setQueryCopied(true);
    setTimeout(() => setQueryCopied(false), 2000);
  };

  const handleCopyProfileEvents = async () => {
    const text = sortedEvents
      .map(([key, value]) => `${key}: ${value}`)
      .join('\n');
    await navigator.clipboard.writeText(text);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <div className="fixed inset-0 bg-black/70 flex items-center justify-center z-50" onClick={handleClose}>
      <div
        className="bg-gray-900 border border-gray-700 rounded-lg w-[1100px] max-h-[85vh] overflow-hidden flex flex-col"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex items-center justify-between p-3 border-b border-gray-700">
          <div>
            <h2 className="text-sm font-semibold text-white">Query Details</h2>
            <p className="text-xs text-gray-400 font-mono">{String(selectedEntry.query_id || '')}</p>
          </div>
          <button onClick={handleClose} className="text-gray-400 hover:text-white">
            <X className="w-4 h-4" />
          </button>
        </div>

        <div className="flex-1 overflow-auto p-3">
          {/* Query */}
          <div className="mb-4">
            <div className="flex items-center justify-between mb-1">
              <div className="flex items-center gap-2">
                <h3 className="text-xs font-semibold text-gray-400">Query</h3>
                <button
                  onClick={handleCopyQuery}
                  className="flex items-center gap-1 px-1.5 py-0.5 text-xs text-gray-400 hover:text-white hover:bg-gray-700 rounded transition-colors"
                  title="Copy query"
                >
                  {queryCopied ? <Check className="w-3 h-3 text-green-400" /> : <Copy className="w-3 h-3" />}
                  {queryCopied ? 'Copied' : 'Copy'}
                </button>
              </div>
              <button
                onClick={handleAnalyseQuery}
                className="flex items-center gap-1 px-2 py-1 text-xs bg-blue-600 hover:bg-blue-500 rounded text-white"
              >
                <ExternalLink className="w-3 h-3" />
                Analyse Query
              </button>
            </div>
            <pre className="bg-gray-800 p-3 rounded text-xs text-gray-300 max-h-48 overflow-y-auto whitespace-pre-wrap break-words font-mono">
              {String(selectedEntry.query || '')}
            </pre>
          </div>

          {/* Summary Stats */}
          <div className="grid grid-cols-6 gap-2 mb-4">
            <div className="bg-gray-800 p-2 rounded">
              <div className="text-xs text-gray-400">Duration</div>
              <div className="text-sm font-semibold text-white">{formatNumber(Number(selectedEntry.query_duration_ms) || 0)} ms</div>
            </div>
            <div className="bg-gray-800 p-2 rounded">
              <div className="text-xs text-gray-400">Memory</div>
              <div className="text-sm font-semibold text-white">{formatBytes(Number(selectedEntry.memory_usage) || 0)}</div>
            </div>
            <div className="bg-gray-800 p-2 rounded">
              <div className="text-xs text-gray-400">Read Rows</div>
              <div className="text-sm font-semibold text-white">{formatNumber(Number(selectedEntry.read_rows) || 0)}</div>
            </div>
            <div className="bg-gray-800 p-2 rounded">
              <div className="text-xs text-gray-400">Read Bytes</div>
              <div className="text-sm font-semibold text-white">{formatBytes(Number(selectedEntry.read_bytes) || 0)}</div>
            </div>
            <div className="bg-gray-800 p-2 rounded">
              <div className="text-xs text-gray-400">Result Rows</div>
              <div className="text-sm font-semibold text-white">{formatNumber(Number(selectedEntry.result_rows) || 0)}</div>
            </div>
            <div className="bg-gray-800 p-2 rounded">
              <div className="text-xs text-gray-400">Written Rows</div>
              <div className="text-sm font-semibold text-white">{formatNumber(Number(selectedEntry.written_rows) || 0)}</div>
            </div>
          </div>

          {/* Exception if any */}
          {Boolean(selectedEntry.exception) && (
            <div className="mb-4">
              <h3 className="text-xs font-semibold text-red-400 mb-1">Exception (Code: {String(selectedEntry.exception_code || '')})</h3>
              <pre className="bg-red-900/30 border border-red-800 p-2 rounded text-xs text-red-300 overflow-x-auto">
                {String(selectedEntry.exception)}
              </pre>
            </div>
          )}

          {/* Two column layout for ProfileEvents and Settings */}
          <div className="grid grid-cols-2 gap-4 mb-4">
            {/* Profile Events */}
            <div>
              <div className="flex items-center justify-between mb-2">
                <h3 className="text-xs font-semibold text-gray-400">
                  ProfileEvents ({sortedEvents.length} non-zero)
                </h3>
                <button
                  onClick={handleCopyProfileEvents}
                  className="flex items-center gap-1 px-1.5 py-0.5 text-xs text-gray-400 hover:text-white hover:bg-gray-700 rounded transition-colors"
                  title="Copy to clipboard"
                >
                  {copied ? <Check className="w-3 h-3 text-green-400" /> : <Copy className="w-3 h-3" />}
                  {copied ? 'Copied' : 'Copy'}
                </button>
              </div>
              <div className="bg-gray-800 rounded max-h-64 overflow-y-auto">
                <table className="w-full text-xs">
                  <thead className="sticky top-0 bg-gray-800">
                    <tr className="border-b border-gray-700">
                      <th className="text-left p-1.5 text-gray-400">Event</th>
                      <th className="text-right p-1.5 text-gray-400">Value</th>
                    </tr>
                  </thead>
                  <tbody>
                    {sortedEvents.map(([key, value]) => (
                      <tr key={key} className="border-b border-gray-700/50 hover:bg-gray-700/30">
                        <td className="p-1.5 text-gray-300 font-mono">{key}</td>
                        <td className="p-1.5 text-right text-white">
                          {key.toLowerCase().includes('bytes')
                            ? formatBytes(value)
                            : formatNumber(value)}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>

            {/* Settings */}
            <div>
              <h3 className="text-xs font-semibold text-gray-400 mb-2">
                Settings ({sortedSettings.length})
              </h3>
              <div className="bg-gray-800 rounded max-h-64 overflow-y-auto">
                <table className="w-full text-xs">
                  <thead className="sticky top-0 bg-gray-800">
                    <tr className="border-b border-gray-700">
                      <th className="text-left p-1.5 text-gray-400">Setting</th>
                      <th className="text-right p-1.5 text-gray-400">Value</th>
                    </tr>
                  </thead>
                  <tbody>
                    {sortedSettings.map(([key, value]) => (
                      <tr key={key} className="border-b border-gray-700/50 hover:bg-gray-700/30">
                        <td className="p-1.5 text-gray-300 font-mono">{key}</td>
                        <td className="p-1.5 text-right text-white font-mono truncate max-w-[200px]" title={value}>
                          {value}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </div>

          {/* Arrays Section - Used Functions, etc. */}
          <div>
            <ArrayDisplay items={(selectedEntry.used_functions || []) as string[]} label="Used Functions" />
            <ArrayDisplay items={(selectedEntry.used_aggregate_functions || []) as string[]} label="Aggregate Functions" />
            <ArrayDisplay items={(selectedEntry.used_table_functions || []) as string[]} label="Table Functions" />
            <ArrayDisplay items={(selectedEntry.used_data_type_families || []) as string[]} label="Data Types" />
          </div>
        </div>
      </div>
    </div>
  );
}
