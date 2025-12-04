import { useState, useMemo, useEffect, useCallback } from 'react';
import { AgGridReact } from 'ag-grid-react';
import { AllCommunityModule, ModuleRegistry, themeAlpine } from 'ag-grid-community';
import type { ColDef } from 'ag-grid-community';
import { BarChart2, Clock, RefreshCw, Search } from 'lucide-react';
import { fetchMetrics, fetchAsyncMetrics } from '../../services/api';

ModuleRegistry.registerModules([AllCommunityModule]);

const darkTheme = themeAlpine.withParams({
  backgroundColor: '#111827',
  headerBackgroundColor: '#1f2937',
  oddRowBackgroundColor: '#111827',
  rowHoverColor: '#1f2937',
  borderColor: '#374151',
  foregroundColor: '#f3f4f6',
  headerTextColor: '#f3f4f6',
  fontFamily: 'ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace',
  fontSize: 10,
  cellTextColor: '#f3f4f6',
  rowHeight: 28,
  headerHeight: 30,
});

type MetricsTab = 'metrics' | 'async';

interface MetricRow {
  metric: string;
  value: number | string;
  description: string;
}

function formatValue(value: number | string): string {
  if (typeof value === 'string') return value;
  if (Math.abs(value) >= 1000000000) return (value / 1000000000).toFixed(2) + 'B';
  if (Math.abs(value) >= 1000000) return (value / 1000000).toFixed(2) + 'M';
  if (Math.abs(value) >= 1000) return (value / 1000).toFixed(2) + 'K';
  if (Number.isInteger(value)) return value.toLocaleString();
  return value.toFixed(2);
}

export function MetricsPage() {
  const [activeTab, setActiveTab] = useState<MetricsTab>('metrics');
  const [metrics, setMetrics] = useState<MetricRow[]>([]);
  const [asyncMetrics, setAsyncMetrics] = useState<MetricRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [search, setSearch] = useState('');

  const loadData = useCallback(async () => {
    setLoading(true);
    try {
      const [m, am] = await Promise.all([fetchMetrics(), fetchAsyncMetrics()]);
      setMetrics(m as MetricRow[]);
      setAsyncMetrics(am as MetricRow[]);
    } catch (err) {
      console.error('Failed to load metrics:', err);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    loadData();
  }, [loadData]);

  // Auto-refresh every 30 seconds
  useEffect(() => {
    const interval = setInterval(loadData, 30000);
    return () => clearInterval(interval);
  }, [loadData]);

  const tabs: { id: MetricsTab; label: string; icon: typeof BarChart2 }[] = [
    { id: 'metrics', label: 'Metrics', icon: BarChart2 },
    { id: 'async', label: 'Async Metrics', icon: Clock },
  ];

  const currentData = activeTab === 'metrics' ? metrics : asyncMetrics;

  const filteredData = useMemo(() => {
    if (!search) return currentData;
    const lowerSearch = search.toLowerCase();
    return currentData.filter(row =>
      row.metric.toLowerCase().includes(lowerSearch) ||
      row.description.toLowerCase().includes(lowerSearch)
    );
  }, [currentData, search]);

  const columnDefs: ColDef<MetricRow>[] = useMemo(() => [
    {
      headerName: 'Metric',
      field: 'metric',
      sortable: true,
      resizable: true,
      width: 300,
    },
    {
      headerName: 'Value',
      field: 'value',
      sortable: true,
      resizable: true,
      width: 150,
      cellStyle: { textAlign: 'right', color: '#60a5fa' },
      valueFormatter: (params) => formatValue(params.value),
      comparator: (a, b) => Number(a) - Number(b),
    },
    {
      headerName: 'Description',
      field: 'description',
      sortable: true,
      resizable: true,
      flex: 1,
      cellStyle: { color: '#9ca3af' },
    },
  ], []);

  const defaultColDef = useMemo<ColDef>(() => ({
    resizable: true,
    suppressMovable: true,
  }), []);

  return (
    <div className="h-full flex flex-col">
      <div className="border-b border-gray-700 px-4 flex items-center gap-1 shrink-0">
        {tabs.map(({ id, label, icon: Icon }) => (
          <button
            key={id}
            onClick={() => setActiveTab(id)}
            className={`flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium border-b-2 -mb-px transition-colors ${
              activeTab === id
                ? 'border-blue-500 text-blue-400'
                : 'border-transparent text-gray-400 hover:text-gray-300'
            }`}
          >
            <Icon className="w-3 h-3" />
            {label}
          </button>
        ))}
        <div className="ml-auto flex items-center gap-2">
          <div className="relative">
            <Search className="w-3 h-3 absolute left-2 top-1/2 -translate-y-1/2 text-gray-500" />
            <input
              type="text"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              placeholder="Search metrics..."
              className="pl-7 pr-2 py-1 text-xs bg-gray-800 border border-gray-700 rounded text-white placeholder-gray-500 w-48"
            />
          </div>
          <span className="text-xs text-gray-400">{filteredData.length} metrics</span>
          <button
            onClick={loadData}
            disabled={loading}
            className="p-1 bg-gray-700 hover:bg-gray-600 rounded text-gray-300 disabled:opacity-50"
            title="Refresh"
          >
            <RefreshCw className={`w-3.5 h-3.5 ${loading ? 'animate-spin' : ''}`} />
          </button>
        </div>
      </div>

      <div className="flex-1 overflow-hidden p-4">
        <AgGridReact<MetricRow>
          theme={darkTheme}
          rowData={filteredData}
          columnDefs={columnDefs}
          defaultColDef={defaultColDef}
          loading={loading}
          animateRows={false}
          suppressCellFocus={true}
          enableCellTextSelection={true}
          getRowId={(params) => params.data.metric}
        />
      </div>
    </div>
  );
}
