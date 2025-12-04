import { useState } from 'react';
import { X, Play, ChevronDown, ChevronRight, Copy, Check } from 'lucide-react';
import { useQueryStore } from '../stores/queryStore';
import { fetchExplainPlan } from '../services/api';

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
  const [explainPlan, setExplainPlan] = useState<string[] | null>(null);
  const [explainLoading, setExplainLoading] = useState(false);
  const [explainError, setExplainError] = useState<string | null>(null);
  const [explainExpanded, setExplainExpanded] = useState(true);
  const [copied, setCopied] = useState(false);

  if (!selectedEntry) return null;

  const profileEvents = selectedEntry.ProfileEvents || {};
  const settings = selectedEntry.Settings || {};

  const sortedEvents = Object.entries(profileEvents)
    .filter(([, value]) => value > 0)
    .sort((a, b) => b[1] - a[1]);

  const sortedSettings = Object.entries(settings).sort((a, b) => a[0].localeCompare(b[0]));

  const handleExplainPlan = async () => {
    setExplainLoading(true);
    setExplainError(null);
    try {
      const result = await fetchExplainPlan(selectedEntry.query as string);
      // Extract the explain output - typically in 'explain' field
      const lines = result.map(row => row.explain || row.plan || JSON.stringify(row));
      setExplainPlan(lines as string[]);
    } catch (err) {
      setExplainError(err instanceof Error ? err.message : 'Failed to run explain');
    } finally {
      setExplainLoading(false);
    }
  };

  const handleClose = () => {
    setSelectedEntry(null);
    setExplainPlan(null);
    setExplainError(null);
    setCopied(false);
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
            <p className="text-xs text-gray-400 font-mono">{selectedEntry.query_id}</p>
          </div>
          <button onClick={handleClose} className="text-gray-400 hover:text-white">
            <X className="w-4 h-4" />
          </button>
        </div>

        <div className="flex-1 overflow-auto p-3">
          {/* Query */}
          <div className="mb-4">
            <div className="flex items-center justify-between mb-1">
              <h3 className="text-xs font-semibold text-gray-400">Query</h3>
              <button
                onClick={handleExplainPlan}
                disabled={explainLoading}
                className="flex items-center gap-1 px-2 py-1 text-xs bg-blue-600 hover:bg-blue-500 disabled:bg-blue-800 rounded text-white"
              >
                <Play className="w-3 h-3" />
                {explainLoading ? 'Running...' : 'Explain Plan'}
              </button>
            </div>
            <pre className="bg-gray-800 p-3 rounded text-xs text-gray-300 max-h-48 overflow-y-auto whitespace-pre-wrap break-words font-mono">
              {selectedEntry.query}
            </pre>
          </div>

          {/* Explain Plan Result */}
          {(explainPlan || explainError) && (
            <div className="mb-4">
              <button
                onClick={() => setExplainExpanded(!explainExpanded)}
                className="flex items-center gap-1 text-xs font-semibold text-gray-400 mb-1 hover:text-gray-300"
              >
                {explainExpanded ? <ChevronDown className="w-3 h-3" /> : <ChevronRight className="w-3 h-3" />}
                Explain Plan
              </button>
              {explainExpanded && (
                <>
                  {explainError ? (
                    <div className="bg-red-900/30 border border-red-800 p-2 rounded text-xs text-red-300">
                      {explainError}
                    </div>
                  ) : (
                    <pre className="bg-gray-800 p-3 rounded text-xs text-green-300 max-h-64 overflow-auto whitespace-pre-wrap font-mono">
                      {explainPlan?.join('\n')}
                    </pre>
                  )}
                </>
              )}
            </div>
          )}

          {/* Summary Stats */}
          <div className="grid grid-cols-6 gap-2 mb-4">
            <div className="bg-gray-800 p-2 rounded">
              <div className="text-xs text-gray-400">Duration</div>
              <div className="text-sm font-semibold text-white">{formatNumber(selectedEntry.query_duration_ms)} ms</div>
            </div>
            <div className="bg-gray-800 p-2 rounded">
              <div className="text-xs text-gray-400">Memory</div>
              <div className="text-sm font-semibold text-white">{formatBytes(selectedEntry.memory_usage)}</div>
            </div>
            <div className="bg-gray-800 p-2 rounded">
              <div className="text-xs text-gray-400">Read Rows</div>
              <div className="text-sm font-semibold text-white">{formatNumber(selectedEntry.read_rows)}</div>
            </div>
            <div className="bg-gray-800 p-2 rounded">
              <div className="text-xs text-gray-400">Read Bytes</div>
              <div className="text-sm font-semibold text-white">{formatBytes(selectedEntry.read_bytes)}</div>
            </div>
            <div className="bg-gray-800 p-2 rounded">
              <div className="text-xs text-gray-400">Result Rows</div>
              <div className="text-sm font-semibold text-white">{formatNumber(selectedEntry.result_rows)}</div>
            </div>
            <div className="bg-gray-800 p-2 rounded">
              <div className="text-xs text-gray-400">Written Rows</div>
              <div className="text-sm font-semibold text-white">{formatNumber(selectedEntry.written_rows)}</div>
            </div>
          </div>

          {/* Exception if any */}
          {selectedEntry.exception && (
            <div className="mb-4">
              <h3 className="text-xs font-semibold text-red-400 mb-1">Exception (Code: {selectedEntry.exception_code})</h3>
              <pre className="bg-red-900/30 border border-red-800 p-2 rounded text-xs text-red-300 overflow-x-auto">
                {selectedEntry.exception}
              </pre>
            </div>
          )}

          {/* Arrays Section */}
          <div className="mb-4">
            <ArrayDisplay items={selectedEntry.used_functions} label="Used Functions" />
            <ArrayDisplay items={selectedEntry.used_aggregate_functions} label="Aggregate Functions" />
            <ArrayDisplay items={selectedEntry.used_table_functions} label="Table Functions" />
            <ArrayDisplay items={selectedEntry.used_data_type_families} label="Data Types" />
            <ArrayDisplay items={selectedEntry.databases} label="Databases" />
            <ArrayDisplay items={selectedEntry.tables} label="Tables" />
            <ArrayDisplay items={selectedEntry.columns} label="Columns" />
          </div>

          {/* Two column layout for ProfileEvents and Settings */}
          <div className="grid grid-cols-2 gap-4">
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
        </div>
      </div>
    </div>
  );
}
