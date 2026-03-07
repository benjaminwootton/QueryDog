import { useState, useMemo, useEffect, useCallback } from 'react';
import { AgGridReact } from 'ag-grid-react';
import { AllCommunityModule, ModuleRegistry, themeAlpine } from 'ag-grid-community';
import type { ColDef } from 'ag-grid-community';
import { Settings, HardDrive, Database, BarChart2, Clock, Zap, AlertTriangle, Search, RefreshCw, LayoutDashboard } from 'lucide-react';
import { SystemTable } from '../SystemTable';
import { DashboardTab } from './DashboardTab';
import {
  fetchSettings,
  fetchSettingsColumns,
  fetchDisks,
  fetchDisksColumns,
  fetchStoragePolicies,
  fetchStoragePoliciesColumns,
  fetchMetrics,
  fetchAsyncMetrics,
  fetchEvents,
  fetchErrors,
  fetchErrorsColumns,
} from '../../services/api';

ModuleRegistry.registerModules([AllCommunityModule]);

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

type InstanceTab = 'dashboard' | 'metrics' | 'async' | 'events' | 'errors' | 'settings' | 'disks' | 'storagePolicies';

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

const DISKS_DEFAULT_VISIBLE_FIELDS = [
  'name',
  'path',
  'free_space',
  'total_space',
  'unreserved_space',
  'keep_free_space',
  'type',
];

const STORAGE_POLICIES_DEFAULT_VISIBLE_FIELDS = [
  'policy_name',
  'volume_name',
  'volume_priority',
  'disks',
  'max_data_part_size',
  'move_factor',
];

export function InstancePage() {
  const [activeTab, setActiveTab] = useState<InstanceTab>('dashboard');
  const [metrics, setMetrics] = useState<MetricRow[]>([]);
  const [asyncMetrics, setAsyncMetrics] = useState<MetricRow[]>([]);
  const [events, setEvents] = useState<EventRow[]>([]);
  const [metricsLoading, setMetricsLoading] = useState(true);
  const [search, setSearch] = useState('');

  const loadMetricsData = useCallback(async () => {
    setMetricsLoading(true);
    try {
      const [m, am, ev] = await Promise.all([fetchMetrics(), fetchAsyncMetrics(), fetchEvents()]);
      setMetrics(m as unknown as MetricRow[]);
      setAsyncMetrics(am as unknown as MetricRow[]);
      setEvents(ev as unknown as EventRow[]);
    } catch (err) {
      console.error('Failed to load metrics:', err);
    } finally {
      setMetricsLoading(false);
    }
  }, []);

  useEffect(() => {
    loadMetricsData();
  }, [loadMetricsData]);

  const isMetricsTab = activeTab === 'metrics' || activeTab === 'async' || activeTab === 'events';
  const isDashboardTab = activeTab === 'dashboard';

  const currentData = activeTab === 'events' ? events : (activeTab === 'metrics' ? metrics : asyncMetrics);
  const nameField = activeTab === 'events' ? 'event' : 'metric';

  const filteredData = useMemo(() => {
    if (!isMetricsTab || !search) return currentData;
    const lowerSearch = search.toLowerCase();
    return currentData.filter(row => {
      const name = (row as MetricRow).metric || (row as EventRow).event || '';
      return name.toLowerCase().includes(lowerSearch) ||
        row.description.toLowerCase().includes(lowerSearch);
    });
  }, [currentData, search, isMetricsTab]);

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
    sortingOrder: ['desc', 'asc'],
  }), []);

  const tabs: { id: InstanceTab; label: string; icon: typeof Settings }[] = [
    { id: 'dashboard', label: 'Dashboard', icon: LayoutDashboard },
    { id: 'metrics', label: 'Metrics', icon: BarChart2 },
    { id: 'async', label: 'Async Metrics', icon: Clock },
    { id: 'events', label: 'Events', icon: Zap },
    { id: 'errors', label: 'Errors', icon: AlertTriangle },
    { id: 'settings', label: 'Settings', icon: Settings },
    { id: 'disks', label: 'Disks', icon: HardDrive },
    { id: 'storagePolicies', label: 'Storage Policies', icon: Database },
  ];

  // Get dashboard metrics count from localStorage
  const getDashboardMetricsCount = () => {
    const source = localStorage.getItem('dashboardSource') || 'metrics';
    const savedMetrics = localStorage.getItem('dashboardMetricsLog');
    const savedAsync = localStorage.getItem('dashboardAsyncLog');

    try {
      if (source === 'async' && savedAsync) {
        return JSON.parse(savedAsync).length;
      }
      if (savedMetrics) {
        return JSON.parse(savedMetrics).length;
      }
    } catch {
      // ignore
    }
    return 0;
  };

  return (
    <div className="h-full flex flex-col">
      {/* Header bar - always visible */}
      <div className="bg-gray-900/50 border-b border-gray-700 px-1.5 py-2 flex items-center justify-between shrink-0">
        {isDashboardTab ? (
          <>
            <div className="flex items-center gap-3">
              <LayoutDashboard className="w-4 h-4 text-blue-400" />
              <span className="text-xs text-gray-400">
                Real-time metrics dashboard
              </span>
            </div>
            <div className="flex items-center gap-4 text-xs">
              <span className="text-gray-400">
                Total: <span className="text-white font-medium">{getDashboardMetricsCount()}</span> metrics on dashboard
              </span>
            </div>
          </>
        ) : (
          <>
            <div className="flex items-center gap-3">
              <div className="relative flex items-center">
                <Search className="absolute left-2 w-3 h-3 text-gray-400" />
                <input
                  type="text"
                  value={search}
                  onChange={(e) => setSearch(e.target.value)}
                  placeholder={`Search ${activeTab === 'events' ? 'events' : activeTab === 'settings' ? 'settings' : activeTab === 'errors' ? 'errors' : activeTab === 'disks' ? 'disks' : activeTab === 'storagePolicies' ? 'storage policies' : 'metrics'}...`}
                  className="bg-gray-800 border border-gray-600 rounded pl-6 pr-6 py-0.5 text-white text-xs w-64"
                />
              </div>
              {isMetricsTab && (
                <button
                  onClick={loadMetricsData}
                  disabled={metricsLoading}
                  className="p-1 bg-gray-700 hover:bg-gray-600 rounded text-gray-300 disabled:opacity-50"
                  title="Refresh"
                >
                  <RefreshCw className={`w-3.5 h-3.5 ${metricsLoading ? 'animate-spin' : ''}`} />
                </button>
              )}
            </div>
            {isMetricsTab && (
              <div className="flex items-center gap-4 text-xs">
                <span className="text-gray-400">
                  Total: <span className="text-white font-medium">{filteredData.length.toLocaleString()}</span> {activeTab === 'events' ? 'events' : 'metrics'}
                </span>
              </div>
            )}
          </>
        )}
      </div>

      <div className="border-b border-gray-700 px-1.5 flex items-center gap-1 shrink-0">
        {tabs.map(({ id, label, icon: Icon }) => (
          <button
            key={id}
            onClick={() => { setActiveTab(id); setSearch(''); }}
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

      <div className={`flex-1 overflow-hidden ${isDashboardTab ? '' : 'px-1.5 pt-3 pb-4'}`}>
        {isDashboardTab && <DashboardTab />}
        {isMetricsTab && (
          <AgGridReact<MetricRow | EventRow>
            theme={darkTheme}
            rowData={filteredData}
            columnDefs={columnDefs}
            defaultColDef={defaultColDef}
            loading={metricsLoading}
            animateRows={false}
            suppressCellFocus={true}
            enableCellTextSelection={true}
            getRowId={(params) => (params.data as MetricRow).metric || (params.data as EventRow).event}
          />
        )}
        {activeTab === 'errors' && (
          <SystemTable
            fetchData={fetchErrors}
            fetchColumns={fetchErrorsColumns}
            getRowId={(data) => `${data.name}-${data.code}-${data.last_error_time}`}
            hideHeader
            search={search}
          />
        )}
        {activeTab === 'settings' && (
          <SystemTable
            fetchData={fetchSettings}
            fetchColumns={fetchSettingsColumns}
            getRowId={(data) => String(data.name)}
            hideHeader
            columnWidthOverrides={{ name: 400 }}
            search={search}
          />
        )}
        {activeTab === 'disks' && (
          <SystemTable
            fetchData={fetchDisks}
            fetchColumns={fetchDisksColumns}
            defaultVisibleFields={DISKS_DEFAULT_VISIBLE_FIELDS}
            getRowId={(data) => String(data.name)}
            hideHeader
            search={search}
          />
        )}
        {activeTab === 'storagePolicies' && (
          <SystemTable
            fetchData={fetchStoragePolicies}
            fetchColumns={fetchStoragePoliciesColumns}
            defaultVisibleFields={STORAGE_POLICIES_DEFAULT_VISIBLE_FIELDS}
            getRowId={(data) => `${data.policy_name}-${data.volume_name}`}
            hideHeader
            search={search}
          />
        )}
      </div>
    </div>
  );
}
