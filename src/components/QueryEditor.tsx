import { useState, useEffect } from 'react';
import { Play, X, Loader2, Copy, Check, ChevronDown, ChevronRight, Clock, Table as TableIcon, AlertCircle } from 'lucide-react';
import { executeQuery, fetchExplainByType, type ExplainType, type QueryResult } from '../services/api';

interface QueryEditorProps {
  initialQuery?: string;
  onClose: () => void;
}

type ExplainTab = 'plan' | 'indexes' | 'pipeline' | 'ast' | 'syntax' | 'estimate';

const EXPLAIN_TABS: { id: ExplainTab; label: string; description: string }[] = [
  { id: 'plan', label: 'Plan', description: 'Basic execution plan' },
  { id: 'indexes', label: 'Indexes', description: 'Plan with index usage info' },
  { id: 'pipeline', label: 'Pipeline', description: 'Query execution pipeline' },
  { id: 'ast', label: 'AST', description: 'Abstract syntax tree' },
  { id: 'syntax', label: 'Syntax', description: 'Optimized query syntax' },
  { id: 'estimate', label: 'Estimate', description: 'Estimated rows/bytes' },
];

function formatDuration(ms: number): string {
  if (ms < 1000) return `${Math.round(ms)}ms`;
  return `${(ms / 1000).toFixed(2)}s`;
}

function formatValue(value: unknown): string {
  if (value === null || value === undefined) return 'NULL';
  if (typeof value === 'object') return JSON.stringify(value);
  return String(value);
}

export function QueryEditor({ initialQuery = '', onClose }: QueryEditorProps) {
  const [query, setQuery] = useState(initialQuery);
  const [results, setResults] = useState<QueryResult | null>(null);
  const [executing, setExecuting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [copied, setCopied] = useState(false);

  // Explain state
  const [activeExplainTab, setActiveExplainTab] = useState<ExplainTab>('plan');
  const [explainResults, setExplainResults] = useState<Record<ExplainTab, string[] | null>>({
    plan: null,
    indexes: null,
    pipeline: null,
    ast: null,
    syntax: null,
    estimate: null,
  });
  const [explainLoading, setExplainLoading] = useState<Record<ExplainTab, boolean>>({
    plan: false,
    indexes: false,
    pipeline: false,
    ast: false,
    syntax: false,
    estimate: false,
  });
  const [explainErrors, setExplainErrors] = useState<Record<ExplainTab, string | null>>({
    plan: null,
    indexes: null,
    pipeline: null,
    ast: null,
    syntax: null,
    estimate: null,
  });

  const [showResults, setShowResults] = useState(true);
  const [showExplain, setShowExplain] = useState(true);

  // Auto-run explain when query changes and is valid
  useEffect(() => {
    if (initialQuery) {
      // Auto-load the plan tab
      loadExplain('plan');
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [initialQuery]);

  const handleExecute = async () => {
    if (!query.trim()) return;

    setExecuting(true);
    setError(null);
    setResults(null);

    // Reset explain results so they reload with the new query
    setExplainResults({
      plan: null,
      indexes: null,
      pipeline: null,
      ast: null,
      syntax: null,
      estimate: null,
    });
    setExplainErrors({
      plan: null,
      indexes: null,
      pipeline: null,
      ast: null,
      syntax: null,
      estimate: null,
    });

    try {
      const result = await executeQuery(query);
      setResults(result);
      // Auto-load the plan tab after query execution
      loadExplain('plan');
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to execute query');
    } finally {
      setExecuting(false);
    }
  };

  const loadExplain = async (type: ExplainTab, forceReload = false) => {
    if (!query.trim()) return;
    if (!forceReload && (explainResults[type] !== null || explainLoading[type])) return; // Already loaded or loading

    setExplainLoading(prev => ({ ...prev, [type]: true }));
    setExplainErrors(prev => ({ ...prev, [type]: null }));

    try {
      const result = await fetchExplainByType(query, type as ExplainType);
      const lines = result.map(row => row.explain || row.plan || JSON.stringify(row));
      setExplainResults(prev => ({ ...prev, [type]: lines as string[] }));
    } catch (err) {
      setExplainErrors(prev => ({ ...prev, [type]: err instanceof Error ? err.message : 'Failed to run explain' }));
    } finally {
      setExplainLoading(prev => ({ ...prev, [type]: false }));
    }
  };

  const handleExplainTabChange = (tab: ExplainTab) => {
    setActiveExplainTab(tab);
    loadExplain(tab);
  };

  const handleCopyQuery = async () => {
    await navigator.clipboard.writeText(query);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
      handleExecute();
    }
  };

  // Get column names from results
  const columns = results?.data?.[0] ? Object.keys(results.data[0]) : [];

  return (
    <div className="fixed inset-0 bg-black/70 flex items-center justify-center z-50" onClick={onClose}>
      <div
        className="bg-gray-900 border border-gray-700 rounded-lg w-[95vw] h-[90vh] overflow-hidden flex flex-col"
        onClick={(e) => e.stopPropagation()}
      >
        {/* Header */}
        <div className="flex items-center justify-between p-3 border-b border-gray-700 shrink-0">
          <div className="flex items-center gap-3">
            <TableIcon className="w-5 h-5 text-blue-400" />
            <h2 className="text-sm font-semibold text-white">Query Editor</h2>
          </div>
          <button onClick={onClose} className="text-gray-400 hover:text-white">
            <X className="w-4 h-4" />
          </button>
        </div>

        {/* Query Input */}
        <div className="p-4 border-b border-gray-700 shrink-0">
          <div className="flex items-start gap-3">
            <div className="flex-1">
              <textarea
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                onKeyDown={handleKeyDown}
                placeholder="Enter your SQL query here..."
                className="w-full h-32 bg-gray-800 border border-gray-600 rounded p-3 text-sm font-mono text-gray-200 focus:outline-none focus:border-blue-500 resize-none"
                spellCheck={false}
              />
              <div className="flex items-center justify-between mt-2">
                <span className="text-[10px] text-gray-500">Press Ctrl+Enter to execute</span>
                <button
                  onClick={handleCopyQuery}
                  className="flex items-center gap-1 px-2 py-1 text-xs text-gray-400 hover:text-white hover:bg-gray-700 rounded"
                >
                  {copied ? <Check className="w-3 h-3 text-green-400" /> : <Copy className="w-3 h-3" />}
                  {copied ? 'Copied' : 'Copy'}
                </button>
              </div>
            </div>
            <button
              onClick={handleExecute}
              disabled={executing || !query.trim()}
              className="flex items-center gap-2 px-4 py-2 bg-green-600 hover:bg-green-500 disabled:bg-gray-700 disabled:text-gray-500 rounded text-white text-sm font-medium"
            >
              {executing ? <Loader2 className="w-4 h-4 animate-spin" /> : <Play className="w-4 h-4" />}
              Run
            </button>
          </div>
        </div>

        {/* Results and Explain Panels */}
        <div className="flex-1 overflow-hidden flex">
          {/* Results Panel */}
          <div className="flex-1 flex flex-col border-r border-gray-700 overflow-hidden">
            <button
              onClick={() => setShowResults(!showResults)}
              className="flex items-center gap-2 px-4 py-2 bg-gray-800 text-gray-300 text-xs font-medium hover:bg-gray-750 shrink-0"
            >
              {showResults ? <ChevronDown className="w-3 h-3" /> : <ChevronRight className="w-3 h-3" />}
              Results
              {results && (
                <span className="text-gray-500 ml-2">
                  {results.rowCount} rows in {formatDuration(results.duration)}
                </span>
              )}
            </button>

            {showResults && (
              <div className="flex-1 overflow-hidden p-2">
                {error && (
                  <div className="flex items-start gap-2 p-3 bg-red-900/30 border border-red-800 rounded text-sm text-red-300">
                    <AlertCircle className="w-4 h-4 shrink-0 mt-0.5" />
                    <span>{error}</span>
                  </div>
                )}

                {executing && (
                  <div className="flex items-center justify-center h-full">
                    <Loader2 className="w-8 h-8 text-blue-500 animate-spin" />
                  </div>
                )}

                {!executing && !error && !results && (
                  <div className="flex items-center justify-center h-full text-gray-500 text-sm">
                    Run a query to see results
                  </div>
                )}

                {results && results.data.length > 0 && (
                  <div className="h-full overflow-auto border border-gray-700 rounded">
                    <table className="w-full text-xs">
                      <thead className="bg-gray-800 sticky top-0 z-10">
                        <tr>
                          {columns.map((col) => (
                            <th key={col} className="px-2 py-1.5 text-left text-gray-400 font-medium border-b border-gray-700 whitespace-nowrap">
                              {col}
                            </th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {results.data.map((row, idx) => (
                          <tr key={idx} className={`border-b border-gray-800 hover:bg-gray-800/50 ${idx % 2 === 0 ? 'bg-gray-900/30' : ''}`}>
                            {columns.map((col) => (
                              <td key={col} className="px-2 py-1 text-gray-300 font-mono whitespace-nowrap max-w-[300px] truncate" title={formatValue(row[col])}>
                                {formatValue(row[col])}
                              </td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}

                {results && results.data.length === 0 && (
                  <div className="flex items-center justify-center h-full text-gray-500 text-sm">
                    Query returned no results
                  </div>
                )}
              </div>
            )}
          </div>

          {/* Explain Panel */}
          <div className="w-[500px] flex flex-col overflow-hidden">
            <button
              onClick={() => setShowExplain(!showExplain)}
              className="flex items-center gap-2 px-4 py-2 bg-gray-800 text-gray-300 text-xs font-medium hover:bg-gray-750 shrink-0"
            >
              {showExplain ? <ChevronDown className="w-3 h-3" /> : <ChevronRight className="w-3 h-3" />}
              Query Analysis
            </button>

            {showExplain && (
              <div className="flex-1 overflow-hidden flex flex-col">
                {/* Explain Tabs */}
                <div className="flex border-b border-gray-700 px-2 shrink-0">
                  {EXPLAIN_TABS.map((tab) => (
                    <button
                      key={tab.id}
                      onClick={() => handleExplainTabChange(tab.id)}
                      className={`px-3 py-2 text-xs font-medium border-b-2 -mb-px transition-colors ${
                        activeExplainTab === tab.id
                          ? 'border-blue-500 text-blue-400'
                          : 'border-transparent text-gray-400 hover:text-gray-300'
                      }`}
                      title={tab.description}
                    >
                      {tab.label}
                      {explainLoading[tab.id] && <Loader2 className="w-3 h-3 ml-1 inline animate-spin" />}
                    </button>
                  ))}
                </div>

                {/* Explain Content */}
                <div className="flex-1 overflow-auto p-2">
                  {!query.trim() && (
                    <div className="flex items-center justify-center h-full text-gray-500 text-sm">
                      Enter a query to see analysis
                    </div>
                  )}

                  {query.trim() && explainLoading[activeExplainTab] && (
                    <div className="flex items-center justify-center h-full">
                      <Loader2 className="w-6 h-6 text-blue-500 animate-spin" />
                    </div>
                  )}

                  {query.trim() && explainErrors[activeExplainTab] && (
                    <div className="p-3 bg-red-900/30 border border-red-800 rounded text-xs text-red-300">
                      {explainErrors[activeExplainTab]}
                    </div>
                  )}

                  {query.trim() && explainResults[activeExplainTab] && (
                    <pre className="bg-gray-800 p-3 rounded text-xs text-green-300 overflow-auto whitespace-pre-wrap font-mono h-full">
                      {explainResults[activeExplainTab]?.join('\n')}
                    </pre>
                  )}

                  {query.trim() && !explainLoading[activeExplainTab] && !explainErrors[activeExplainTab] && !explainResults[activeExplainTab] && (
                    <div className="flex items-center justify-center h-full text-gray-500 text-sm">
                      Click a tab to load analysis
                    </div>
                  )}
                </div>
              </div>
            )}
          </div>
        </div>

        {/* Footer */}
        <div className="px-4 py-2 border-t border-gray-700 flex items-center gap-4 text-xs text-gray-500 shrink-0">
          <div className="flex items-center gap-1">
            <Clock className="w-3 h-3" />
            <span>Results limited to 1000 rows</span>
          </div>
          <span>|</span>
          <span>Dangerous operations (DROP, DELETE, etc.) are blocked</span>
        </div>
      </div>
    </div>
  );
}
