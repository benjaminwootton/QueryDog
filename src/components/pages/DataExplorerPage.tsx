import { useState, useEffect } from 'react';
import { Database, Table, Loader2, AlertCircle, Play } from 'lucide-react';
import { fetchBrowserDatabases, fetchBrowserTables, fetchBrowserColumns, type BrowserDatabase, type BrowserTable, type BrowserColumn } from '../../services/api';

interface ExploreResult {
  columns: string[];
  data: Record<string, unknown>[];
  rowCount: number;
  duration: number;
  query: string;
}

async function exploreTableData(
  database: string,
  table: string,
  selectedColumns: string[],
  groupByColumns: string[],
  limit: number
): Promise<ExploreResult> {
  const response = await fetch('/api/explore', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ database, table, selectedColumns, groupByColumns, limit }),
  });

  if (!response.ok) {
    const error = await response.json().catch(() => ({ error: response.statusText }));
    throw new Error(error.error || 'Failed to explore table data');
  }

  return response.json();
}

function formatValue(value: unknown): string {
  if (value === null || value === undefined) return 'NULL';
  if (typeof value === 'object') return JSON.stringify(value);
  if (typeof value === 'number') {
    // Format large numbers with commas
    return value.toLocaleString();
  }
  return String(value);
}

function formatDuration(ms: number): string {
  if (ms < 1000) return `${Math.round(ms)}ms`;
  return `${(ms / 1000).toFixed(2)}s`;
}

export function DataExplorerPage() {
  const [databases, setDatabases] = useState<BrowserDatabase[]>([]);
  const [tables, setTables] = useState<BrowserTable[]>([]);
  const [columns, setColumns] = useState<BrowserColumn[]>([]);

  const [selectedDatabase, setSelectedDatabase] = useState<string>('');
  const [selectedTable, setSelectedTable] = useState<string>('');
  const [selectedColumns, setSelectedColumns] = useState<string[]>([]);
  const [groupByColumns, setGroupByColumns] = useState<string[]>([]);
  const [limit, setLimit] = useState<number>(1000);

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<ExploreResult | null>(null);

  // Add CSS for styling select dropdowns
  useEffect(() => {
    const styleId = 'data-explorer-select-styles';
    if (!document.getElementById(styleId)) {
      const style = document.createElement('style');
      style.id = styleId;
      style.textContent = `
        select.data-explorer-select option {
          background-color: #111827;
          color: #d1d5db;
          padding: 8px 12px;
        }
        select.data-explorer-select option:checked {
          background: linear-gradient(0deg, #3b82f6 0%, #3b82f6 100%);
          color: white;
          font-weight: 500;
        }
      `;
      document.head.appendChild(style);
    }
    return () => {
      const existingStyle = document.getElementById(styleId);
      if (existingStyle) {
        existingStyle.remove();
      }
    };
  }, []);

  // Load databases on mount
  useEffect(() => {
    fetchBrowserDatabases()
      .then(setDatabases)
      .catch(err => setError(err.message));
  }, []);

  // Load tables when database changes
  useEffect(() => {
    if (!selectedDatabase) {
      setTables([]);
      setSelectedTable('');
      return;
    }

    fetchBrowserTables(selectedDatabase)
      .then(setTables)
      .catch(err => setError(err.message));
  }, [selectedDatabase]);

  // Load columns when table changes
  useEffect(() => {
    if (!selectedDatabase || !selectedTable) {
      setColumns([]);
      setSelectedColumns([]);
      setGroupByColumns([]);
      return;
    }

    // Clear previous state when switching tables
    setColumns([]);
    setSelectedColumns([]);
    setGroupByColumns([]);
    setError(null);
    setResult(null);

    fetchBrowserColumns(selectedDatabase, selectedTable)
      .then(cols => {
        // Sort columns: PK first, then sort key, then others
        const sortedCols = [...cols].sort((a, b) => {
          const aIsPK = a.is_in_primary_key === 1;
          const bIsPK = b.is_in_primary_key === 1;
          const aIsSort = a.is_in_sorting_key === 1;
          const bIsSort = b.is_in_sorting_key === 1;

          if (aIsPK && !bIsPK) return -1;
          if (!aIsPK && bIsPK) return 1;
          if (aIsSort && !bIsSort) return -1;
          if (!aIsSort && bIsSort) return 1;
          return 0;
        });

        setColumns(sortedCols);
        // Auto-select all columns by default
        setSelectedColumns(sortedCols.map(c => c.name));
      })
      .catch(err => setError(err.message));
  }, [selectedDatabase, selectedTable]);

  const handleExplore = async () => {
    if (!selectedDatabase || !selectedTable) return;

    setLoading(true);
    setError(null);
    setResult(null);

    try {
      const res = await exploreTableData(
        selectedDatabase,
        selectedTable,
        selectedColumns.length > 0 ? selectedColumns : columns.map(c => c.name),
        groupByColumns,
        limit
      );
      setResult(res);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to explore data');
    } finally {
      setLoading(false);
    }
  };

  // Note: Column selection UI was simplified - these functions are kept for potential future use
  // const toggleColumn = (columnName: string) => {
  //   setSelectedColumns(prev =>
  //     prev.includes(columnName)
  //       ? prev.filter(c => c !== columnName)
  //       : [...prev, columnName]
  //   );
  // };

  // const toggleGroupBy = (columnName: string) => {
  //   setGroupByColumns(prev =>
  //     prev.includes(columnName)
  //       ? prev.filter(c => c !== columnName)
  //       : [...prev, columnName]
  //   );
  // };

  // const selectAllColumns = () => {
  //   setSelectedColumns(columns.map(c => c.name));
  // };

  // const clearAllColumns = () => {
  //   setSelectedColumns([]);
  // };

  return (
    <div className="h-full flex flex-col bg-gray-950">
      {/* Header */}
      <div className="border-b border-gray-700 bg-gray-900 p-4">
        <div className="flex items-center gap-3 mb-4">
          <Table className="w-5 h-5 text-blue-400" />
          <h1 className="text-lg font-semibold text-white">Data Explorer</h1>
        </div>

        {/* Selection Controls */}
        <div className="grid grid-cols-4 gap-3">
          {/* Database Selector */}
          <div>
            <label className="block text-xs font-medium text-gray-300 mb-1.5">Database</label>
            <div className="relative">
              <Database className="w-4 h-4 absolute left-3 top-1/2 -translate-y-1/2 text-blue-400 pointer-events-none z-10" />
              <select
                value={selectedDatabase}
                onChange={(e) => setSelectedDatabase(e.target.value)}
                className="data-explorer-select w-full pl-10 pr-8 py-2 bg-gray-900 border border-gray-600 rounded-lg text-sm text-gray-300 font-medium shadow-sm appearance-none cursor-pointer
                  hover:bg-gray-800 hover:border-blue-500 hover:text-white
                  focus:outline-none focus:ring-2 focus:ring-blue-500/30 focus:border-blue-500 focus:text-white
                  transition-all duration-150"
                style={{
                  backgroundImage: `url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='12' height='12' viewBox='0 0 24 24' fill='none' stroke='%239ca3af' stroke-width='2' stroke-linecap='round' stroke-linejoin='round'%3E%3Cpolyline points='6 9 12 15 18 9'%3E%3C/polyline%3E%3C/svg%3E")`,
                  backgroundRepeat: 'no-repeat',
                  backgroundPosition: 'right 0.5rem center',
                  backgroundSize: '1.25rem'
                }}
              >
                <option value="">Select database...</option>
                {databases.map(db => (
                  <option key={db.name} value={db.name}>{db.name}</option>
                ))}
              </select>
            </div>
          </div>

          {/* Table Selector */}
          <div>
            <label className="block text-xs font-medium text-gray-300 mb-1.5">Table</label>
            <div className="relative">
              <Table className="w-4 h-4 absolute left-3 top-1/2 -translate-y-1/2 text-blue-400 pointer-events-none z-10" />
              <select
                value={selectedTable}
                onChange={(e) => setSelectedTable(e.target.value)}
                disabled={!selectedDatabase}
                className="data-explorer-select w-full pl-10 pr-8 py-2 bg-gray-900 border border-gray-600 rounded-lg text-sm text-gray-300 font-medium shadow-sm appearance-none
                  enabled:cursor-pointer enabled:hover:bg-gray-800 enabled:hover:border-blue-500 enabled:hover:text-white
                  focus:outline-none focus:ring-2 focus:ring-blue-500/30 focus:border-blue-500 focus:text-white
                  disabled:opacity-50 disabled:cursor-not-allowed disabled:bg-gray-900/50
                  transition-all duration-150"
                style={{
                  backgroundImage: selectedDatabase ? `url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='12' height='12' viewBox='0 0 24 24' fill='none' stroke='%239ca3af' stroke-width='2' stroke-linecap='round' stroke-linejoin='round'%3E%3Cpolyline points='6 9 12 15 18 9'%3E%3C/polyline%3E%3C/svg%3E")` : 'none',
                  backgroundRepeat: 'no-repeat',
                  backgroundPosition: 'right 0.5rem center',
                  backgroundSize: '1.25rem'
                }}
              >
                <option value="">Select table...</option>
                {tables.map(tbl => (
                  <option key={tbl.name} value={tbl.name}>{tbl.name}</option>
                ))}
              </select>
            </div>
          </div>

          {/* Limit */}
          <div>
            <label className="block text-xs text-gray-400 mb-1">Limit</label>
            <input
              type="number"
              value={limit}
              onChange={(e) => setLimit(Math.min(10000, Math.max(1, parseInt(e.target.value) || 1000)))}
              min="1"
              max="10000"
              className="w-full px-3 py-1.5 bg-gray-800 border border-gray-600 rounded text-sm text-white focus:outline-none focus:border-blue-500"
            />
          </div>

          {/* Execute Button */}
          <div>
            <label className="block text-xs text-gray-400 mb-1">&nbsp;</label>
            <button
              onClick={handleExplore}
              disabled={!selectedDatabase || !selectedTable || loading}
              className="w-full flex items-center justify-center gap-2 px-4 py-1.5 bg-green-600 hover:bg-green-500 disabled:bg-gray-700 disabled:text-gray-500 rounded text-white text-sm font-medium"
            >
              {loading ? <Loader2 className="w-4 h-4 animate-spin" /> : <Play className="w-4 h-4" />}
              Explore
            </button>
          </div>
        </div>
      </div>

      {/* Main Content */}
      <div className="flex-1 flex overflow-hidden">
        {/* Results Panel */}
        <div className="flex-1 flex flex-col overflow-hidden">
          {/* Query Display */}
          {result && (
            <div className="p-2 border-b border-gray-700 bg-gray-900">
              <div className="flex items-start justify-between gap-2">
                <div className="flex-1 min-w-0">
                  <div className="text-[10px] text-gray-500 mb-1">Generated Query:</div>
                  <pre className="text-xs font-mono text-green-300 overflow-x-auto whitespace-pre-wrap">
                    {result.query}
                  </pre>
                </div>
                <div className="text-xs text-gray-400 shrink-0">
                  {result.rowCount.toLocaleString()} rows in {formatDuration(result.duration)}
                </div>
              </div>
            </div>
          )}

          {/* Error Display */}
          {error && (
            <div className="m-3 p-3 bg-red-900/30 border border-red-800 rounded flex items-start gap-2">
              <AlertCircle className="w-4 h-4 text-red-400 shrink-0 mt-0.5" />
              <div className="text-sm text-red-300">{error}</div>
            </div>
          )}

          {/* Loading State */}
          {loading && (
            <div className="flex-1 flex items-center justify-center">
              <Loader2 className="w-8 h-8 text-blue-500 animate-spin" />
            </div>
          )}

          {/* Empty State */}
          {!loading && !error && !result && (
            <div className="flex-1 flex flex-col items-center justify-center text-gray-500">
              <Table className="w-12 h-12 mb-3 opacity-50" />
              <p className="text-sm">Select a database and table, then click Explore</p>
              <p className="text-xs mt-1">You can select specific columns and apply GROUP BY</p>
            </div>
          )}

          {/* Results Table */}
          {!loading && !error && result && result.data.length > 0 && (
            <div className="flex-1 overflow-auto p-3">
              <div className="border border-gray-700 rounded overflow-hidden">
                <table className="w-full text-xs">
                  <thead className="bg-gray-800 sticky top-0">
                    <tr>
                      {result.columns.map(col => (
                        <th key={col} className="px-3 py-2 text-left text-gray-400 font-medium border-b border-gray-700 whitespace-nowrap">
                          {col}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {result.data.map((row, idx) => (
                      <tr
                        key={idx}
                        className={`border-b border-gray-800 hover:bg-gray-800/50 ${
                          idx % 2 === 0 ? 'bg-gray-900/30' : ''
                        }`}
                      >
                        {result.columns.map(col => (
                          <td
                            key={col}
                            className="px-3 py-2 text-gray-300 font-mono whitespace-nowrap max-w-[300px] truncate"
                            title={formatValue(row[col])}
                          >
                            {formatValue(row[col])}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {/* No Results */}
          {!loading && !error && result && result.data.length === 0 && (
            <div className="flex-1 flex items-center justify-center text-gray-500">
              <p className="text-sm">Query returned no results</p>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
