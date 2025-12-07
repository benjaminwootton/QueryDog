import { useState, useMemo, useEffect, useCallback } from 'react';
import { AgGridReact } from 'ag-grid-react';
import { AllCommunityModule, ModuleRegistry, themeAlpine } from 'ag-grid-community';
import type { ColDef } from 'ag-grid-community';
import { BarChart2, Clock, RefreshCw, Search, Zap } from 'lucide-react';
import { fetchMetrics, fetchAsyncMetrics, fetchEvents } from '../../services/api';

ModuleRegistry.registerModules([AllCommunityModule]);

// Create dark theme with JetBrains Mono for cells, lighter weight
const darkTheme = themeAlpine.withParams({
  backgroundColor: '#111827',
  headerBackgroundColor: '#1f2937',
  oddRowBackgroundColor: '#111827',
  rowHoverColor: '#1f2937',
  borderColor: '#374151',
  foregroundColor: '#9ca3af',
  headerTextColor: '#f3f4f6',
  fontFamily: '"JetBrains Mono", ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace',
  fontSize: 9,
  headerFontSize: 11,
  headerFontWeight: 600,
  cellTextColor: '#9ca3af',
  rowHeight: 26,
  headerHeight: 30,
});

type MetricsTab = 'metrics' | 'async' | 'events';

interface MetricRow {
  metric: string;
  value: number | string;
  description: string;
}

interface EventRow {
  event: string;
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
  const [events, setEvents] = useState<EventRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [search, setSearch] = useState('');

  const loadData = useCallback(async () => {
    setLoading(true);
    try {
      const [m, am, ev] = await Promise.all([fetchMetrics(), fetchAsyncMetrics(), fetchEvents()]);
      setMetrics(m as unknown as MetricRow[]);
      setAsyncMetrics(am as unknown as MetricRow[]);
      setEvents(ev as unknown as EventRow[]);
    } catch (err) {
      console.error('Failed to load metrics:', err);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    loadData();
  }, [loadData]);

  const tabs: { id: MetricsTab; label: string; icon: typeof BarChart2 }[] = [
    { id: 'metrics', label: 'Metrics', icon: BarChart2 },
    { id: 'async', label: 'Async Metrics', icon: Clock },
    { id: 'events', label: 'Events', icon: Zap },
  ];

  const currentData = activeTab === 'events' ? events : (activeTab === 'metrics' ? metrics : asyncMetrics);
  const nameField = activeTab === 'events' ? 'event' : 'metric';

  const filteredData = useMemo(() => {
    if (!search) return currentData;
    const lowerSearch = search.toLowerCase();
    return currentData.filter(row => {
      const name = (row as MetricRow).metric || (row as EventRow).event || '';
      return name.toLowerCase().includes(lowerSearch) ||
        row.description.toLowerCase().includes(lowerSearch);
    });
  }, [currentData, search]);

  const columnDefs = useMemo((): ColDef<MetricRow | EventRow>[] => [
    {
      headerName: activeTab === 'events' ? 'Event' : 'Metric',
      field: nameField as 'metric' | 'event',
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
      sort: 'desc',
      cellStyle: { textAlign: 'right', color: '#60a5fa' } as const,
      valueFormatter: (params) => formatValue(params.value),
      comparator: (a, b) => Number(a) - Number(b),
    },
    {
      headerName: 'Description',
      field: 'description',
      sortable: true,
      resizable: true,
      flex: 1,
      cellStyle: { color: '#9ca3af' } as const,
    },
  ], [activeTab, nameField]);

  const defaultColDef = useMemo<ColDef>(() => ({
    resizable: true,
    suppressMovable: true,
  }), []);

  return (
    <div className="h-full flex flex-col">
      {/* Filter bar - consistent with other pages */}
      <div className="bg-gray-900/50 border-b border-gray-700 px-4 py-2 flex items-center justify-between shrink-0">
        <div className="flex items-center gap-3">
          <div className="relative flex items-center">
            <Search className="absolute left-2 w-3 h-3 text-gray-400" />
            <input
              type="text"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              placeholder="Search metrics..."
              className="bg-gray-800 border border-gray-600 rounded pl-6 pr-6 py-0.5 text-white text-xs w-64"
            />
          </div>
          <button
            onClick={loadData}
            disabled={loading}
            className="p-1 bg-gray-700 hover:bg-gray-600 rounded text-gray-300 disabled:opacity-50"
            title="Refresh"
          >
            <RefreshCw className={`w-3.5 h-3.5 ${loading ? 'animate-spin' : ''}`} />
          </button>
        </div>
        <div className="flex items-center gap-4 text-xs">
          <span className="text-gray-400">
            Total: <span className="text-white font-medium">{filteredData.length.toLocaleString()}</span> {activeTab === 'events' ? 'events' : 'metrics'}
          </span>
        </div>
      </div>

      {/* Tabs */}
      <div className="border-b border-gray-700 mx-4 flex items-center gap-1 shrink-0">
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
      </div>

      {/* Table */}
      <div className="flex-1 overflow-hidden p-4">
        <AgGridReact<MetricRow | EventRow>
          theme={darkTheme}
          rowData={filteredData}
          columnDefs={columnDefs}
          defaultColDef={defaultColDef}
          loading={loading}
          animateRows={false}
          suppressCellFocus={true}
          enableCellTextSelection={true}
          getRowId={(params) => (params.data as MetricRow).metric || (params.data as EventRow).event}
        />
      </div>
    </div>
  );
}
