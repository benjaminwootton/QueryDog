import { useState, useEffect, useCallback } from 'react';
import { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, Legend } from 'recharts';
import { Settings, RefreshCw, X, Check, BarChart2, Clock, Layers } from 'lucide-react';
import {
  fetchMetricLogTimeSeries,
  fetchMetricLogColumns,
  fetchMetricLogDefaults,
  fetchAsyncMetricLogTimeSeries,
  fetchAsyncMetricLogColumns,
  fetchAsyncMetricLogDefaults,
  type MetricLogTimeSeriesPoint,
  type MetricLogColumn,
  type AsyncMetricLogColumn,
} from '../../services/api';

type DashboardSource = 'metrics' | 'async' | 'bgpool';
type TimeRangeMinutes = 15 | 60 | 360 | 1440;

// Background pool metrics (fixed set)
const BACKGROUND_POOL_METRICS = [
  'CurrentMetric_BackgroundMergesAndMutationsPoolSize',
  'CurrentMetric_BackgroundMergesAndMutationsPoolTask',
  'CurrentMetric_BackgroundFetchesPoolSize',
  'CurrentMetric_BackgroundFetchesPoolTask',
  'CurrentMetric_BackgroundMovePoolSize',
  'CurrentMetric_BackgroundMovePoolTask',
  'CurrentMetric_BackgroundCommonPoolSize',
  'CurrentMetric_BackgroundCommonPoolTask',
];

// Format large numbers with K, M, B suffixes
function formatValue(value: number | string | null | undefined): string {
  if (value == null) return '0';
  const num = typeof value === 'string' ? parseFloat(value) : value;
  if (isNaN(num)) return '0';
  if (Math.abs(num) >= 1000000000) return (num / 1000000000).toFixed(1) + 'B';
  if (Math.abs(num) >= 1000000) return (num / 1000000).toFixed(1) + 'M';
  if (Math.abs(num) >= 1000) return (num / 1000).toFixed(1) + 'K';
  if (Number.isInteger(num)) return num.toLocaleString();
  return num.toFixed(2);
}

// Format metric name for display
function formatMetricName(name: string, source: DashboardSource): string {
  if (source === 'metrics' || source === 'bgpool') {
    return name
      .replace(/^(CurrentMetric_|ProfileEvent_)/, '')
      .replace(/([A-Z])/g, ' $1')
      .trim();
  }
  // Async metrics: handle dots and underscores
  return name
    .replace(/\./g, ' ')
    .replace(/_/g, ' ')
    .replace(/([A-Z])/g, ' $1')
    .trim();
}

// Generate a consistent color for each metric
function getMetricColor(index: number): string {
  const colors = [
    '#60a5fa', // blue
    '#34d399', // green
    '#f472b6', // pink
    '#fbbf24', // amber
    '#a78bfa', // purple
    '#fb923c', // orange
    '#22d3ee', // cyan
    '#f87171', // red
    '#4ade80', // lime
    '#c084fc', // violet
  ];
  return colors[index % colors.length];
}

interface MetricChartProps {
  metric: string;
  data: MetricLogTimeSeriesPoint[];
  colorIndex: number;
  source: DashboardSource;
}

function MetricChart({ metric, data, colorIndex, source }: MetricChartProps) {
  const color = getMetricColor(colorIndex);
  const displayName = formatMetricName(metric, source);

  // Get the latest value
  const latestValue = data.length > 0 ? Number(data[data.length - 1][metric]) || 0 : 0;

  return (
    <div className="bg-gray-800 rounded-lg p-3 border border-gray-700">
      <div className="flex items-center justify-between mb-2">
        <div className="text-xs text-gray-400 truncate flex-1" title={metric}>
          {displayName}
        </div>
        <div className="text-sm font-mono ml-2" style={{ color }}>
          {formatValue(latestValue)}
        </div>
      </div>
      <div className="h-24">
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={data} margin={{ top: 5, right: 5, left: 0, bottom: 5 }}>
            <XAxis
              dataKey="time"
              tick={false}
              axisLine={{ stroke: '#374151' }}
              tickLine={false}
            />
            <YAxis
              tick={{ fontSize: 9, fill: '#6b7280' }}
              tickFormatter={formatValue}
              axisLine={{ stroke: '#374151' }}
              tickLine={false}
              width={40}
            />
            <Tooltip
              contentStyle={{
                backgroundColor: '#1f2937',
                border: '1px solid #374151',
                borderRadius: '4px',
                fontSize: '11px',
              }}
              labelStyle={{ color: '#9ca3af' }}
              formatter={(value: number | string) => [formatValue(value), displayName]}
              labelFormatter={(label) => new Date(label).toLocaleTimeString()}
            />
            <Line
              type="monotone"
              dataKey={metric}
              stroke={color}
              strokeWidth={1.5}
              dot={false}
              isAnimationActive={false}
            />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}

// Combined chart for background pool (Size vs Task for each pool type)
interface PoolChartProps {
  poolName: string;
  sizeMetric: string;
  taskMetric: string;
  data: MetricLogTimeSeriesPoint[];
  colorIndex: number;
}

function PoolChart({ poolName, sizeMetric, taskMetric, data, colorIndex }: PoolChartProps) {
  const sizeColor = getMetricColor(colorIndex * 2);
  const taskColor = getMetricColor(colorIndex * 2 + 1);

  // Get the latest values
  const latestSize = data.length > 0 ? Number(data[data.length - 1][sizeMetric]) || 0 : 0;
  const latestTask = data.length > 0 ? Number(data[data.length - 1][taskMetric]) || 0 : 0;

  return (
    <div className="bg-gray-800 rounded-lg p-3 border border-gray-700">
      <div className="flex items-center justify-between mb-2">
        <div className="text-xs text-gray-400 truncate flex-1" title={poolName}>
          {poolName}
        </div>
        <div className="flex items-center gap-3 text-xs font-mono">
          <span style={{ color: sizeColor }}>Size: {formatValue(latestSize)}</span>
          <span style={{ color: taskColor }}>Tasks: {formatValue(latestTask)}</span>
        </div>
      </div>
      <div className="h-28">
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={data} margin={{ top: 5, right: 5, left: 0, bottom: 5 }}>
            <XAxis
              dataKey="time"
              tick={false}
              axisLine={{ stroke: '#374151' }}
              tickLine={false}
            />
            <YAxis
              tick={{ fontSize: 9, fill: '#6b7280' }}
              tickFormatter={formatValue}
              axisLine={{ stroke: '#374151' }}
              tickLine={false}
              width={40}
            />
            <Tooltip
              contentStyle={{
                backgroundColor: '#1f2937',
                border: '1px solid #374151',
                borderRadius: '4px',
                fontSize: '11px',
              }}
              labelStyle={{ color: '#9ca3af' }}
              formatter={(value: number | string, name: string) => [
                formatValue(value),
                name.includes('Size') ? 'Pool Size' : 'Active Tasks',
              ]}
              labelFormatter={(label) => new Date(label).toLocaleTimeString()}
            />
            <Legend
              verticalAlign="top"
              height={20}
              formatter={(value) => (
                <span className="text-xs">
                  {value.includes('Size') ? 'Pool Size' : 'Active Tasks'}
                </span>
              )}
            />
            <Line
              type="monotone"
              dataKey={sizeMetric}
              stroke={sizeColor}
              strokeWidth={1.5}
              dot={false}
              isAnimationActive={false}
              name="Size"
            />
            <Line
              type="monotone"
              dataKey={taskMetric}
              stroke={taskColor}
              strokeWidth={1.5}
              dot={false}
              isAnimationActive={false}
              name="Tasks"
            />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}

interface SettingsModalProps {
  isOpen: boolean;
  onClose: () => void;
  source: DashboardSource;
  metricsLogColumns: MetricLogColumn[];
  asyncLogColumns: AsyncMetricLogColumn[];
  selectedMetricsLog: string[];
  selectedAsyncLog: string[];
  onSaveMetricsLog: (metrics: string[]) => void;
  onSaveAsyncLog: (metrics: string[]) => void;
}

function SettingsModal({
  isOpen,
  onClose,
  source,
  metricsLogColumns,
  asyncLogColumns,
  selectedMetricsLog,
  selectedAsyncLog,
  onSaveMetricsLog,
  onSaveAsyncLog,
}: SettingsModalProps) {
  const [activeSettingsTab, setActiveSettingsTab] = useState<'metrics' | 'async'>(
    source === 'async' ? 'async' : 'metrics'
  );
  const [localMetricsLog, setLocalMetricsLog] = useState<string[]>(selectedMetricsLog);
  const [localAsyncLog, setLocalAsyncLog] = useState<string[]>(selectedAsyncLog);
  const [search, setSearch] = useState('');

  useEffect(() => {
    setLocalMetricsLog(selectedMetricsLog);
    setLocalAsyncLog(selectedAsyncLog);
    setActiveSettingsTab(source === 'async' ? 'async' : 'metrics');
  }, [selectedMetricsLog, selectedAsyncLog, source, isOpen]);

  if (!isOpen) return null;

  const toggleMetric = (name: string, tab: 'metrics' | 'async') => {
    if (tab === 'metrics') {
      setLocalMetricsLog(prev =>
        prev.includes(name) ? prev.filter(m => m !== name) : [...prev, name]
      );
    } else {
      setLocalAsyncLog(prev =>
        prev.includes(name) ? prev.filter(m => m !== name) : [...prev, name]
      );
    }
  };

  const handleSave = () => {
    onSaveMetricsLog(localMetricsLog);
    onSaveAsyncLog(localAsyncLog);
    onClose();
  };

  // Filter and categorize metrics for the active tab
  const renderMetricsList = () => {
    if (activeSettingsTab === 'metrics') {
      const filtered = metricsLogColumns.filter(m =>
        m.name.toLowerCase().includes(search.toLowerCase())
      );
      const currentMetrics = filtered.filter(m => m.name.startsWith('CurrentMetric_'));
      const profileEvents = filtered.filter(m => m.name.startsWith('ProfileEvent_'));

      return (
        <div className="grid grid-cols-2 gap-4">
          <div>
            <div className="text-xs font-semibold text-gray-400 mb-2">
              Current Metrics ({currentMetrics.length})
            </div>
            <div className="max-h-48 overflow-y-auto bg-gray-800 rounded border border-gray-700">
              {currentMetrics.map(m => (
                <label
                  key={m.name}
                  className="flex items-center gap-2 px-2 py-1.5 hover:bg-gray-700/50 cursor-pointer border-b border-gray-700/50 last:border-0"
                >
                  <input
                    type="checkbox"
                    checked={localMetricsLog.includes(m.name)}
                    onChange={() => toggleMetric(m.name, 'metrics')}
                    className="rounded bg-gray-700 border-gray-600 text-blue-500 focus:ring-blue-500 focus:ring-offset-gray-800"
                  />
                  <span className="text-xs text-gray-300 truncate" title={m.name}>
                    {formatMetricName(m.name, 'metrics')}
                  </span>
                </label>
              ))}
            </div>
          </div>
          <div>
            <div className="text-xs font-semibold text-gray-400 mb-2">
              Profile Events ({profileEvents.length})
            </div>
            <div className="max-h-48 overflow-y-auto bg-gray-800 rounded border border-gray-700">
              {profileEvents.map(m => (
                <label
                  key={m.name}
                  className="flex items-center gap-2 px-2 py-1.5 hover:bg-gray-700/50 cursor-pointer border-b border-gray-700/50 last:border-0"
                >
                  <input
                    type="checkbox"
                    checked={localMetricsLog.includes(m.name)}
                    onChange={() => toggleMetric(m.name, 'metrics')}
                    className="rounded bg-gray-700 border-gray-600 text-blue-500 focus:ring-blue-500 focus:ring-offset-gray-800"
                  />
                  <span className="text-xs text-gray-300 truncate" title={m.name}>
                    {formatMetricName(m.name, 'metrics')}
                  </span>
                </label>
              ))}
            </div>
          </div>
        </div>
      );
    } else {
      const filtered = asyncLogColumns.filter(m =>
        m.name.toLowerCase().includes(search.toLowerCase())
      );

      return (
        <div>
          <div className="text-xs font-semibold text-gray-400 mb-2">
            Async Metrics ({filtered.length})
          </div>
          <div className="max-h-80 overflow-y-auto bg-gray-800 rounded border border-gray-700">
            {filtered.map(m => (
              <label
                key={m.name}
                className="flex items-center gap-2 px-2 py-1.5 hover:bg-gray-700/50 cursor-pointer border-b border-gray-700/50 last:border-0"
              >
                <input
                  type="checkbox"
                  checked={localAsyncLog.includes(m.name)}
                  onChange={() => toggleMetric(m.name, 'async')}
                  className="rounded bg-gray-700 border-gray-600 text-blue-500 focus:ring-blue-500 focus:ring-offset-gray-800"
                />
                <span className="text-xs text-gray-300 truncate" title={m.name}>
                  {m.name}
                </span>
              </label>
            ))}
          </div>
        </div>
      );
    }
  };

  const currentSelected = activeSettingsTab === 'metrics' ? localMetricsLog : localAsyncLog;

  return (
    <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50" onClick={onClose}>
      <div
        className="bg-gray-900 rounded-lg border border-gray-700 w-[650px] max-h-[80vh] flex flex-col"
        onClick={e => e.stopPropagation()}
      >
        <div className="flex items-center justify-between p-3 border-b border-gray-700">
          <h3 className="text-sm font-semibold text-white">Dashboard Settings</h3>
          <button onClick={onClose} className="text-gray-400 hover:text-white">
            <X className="w-4 h-4" />
          </button>
        </div>

        {/* Settings Tabs */}
        <div className="flex border-b border-gray-700 px-3">
          <button
            onClick={() => { setActiveSettingsTab('metrics'); setSearch(''); }}
            className={`flex items-center gap-1.5 px-3 py-2 text-xs font-medium border-b-2 -mb-px ${
              activeSettingsTab === 'metrics'
                ? 'border-blue-500 text-blue-400'
                : 'border-transparent text-gray-400 hover:text-gray-300'
            }`}
          >
            <BarChart2 className="w-3 h-3" />
            Metrics Log
          </button>
          <button
            onClick={() => { setActiveSettingsTab('async'); setSearch(''); }}
            className={`flex items-center gap-1.5 px-3 py-2 text-xs font-medium border-b-2 -mb-px ${
              activeSettingsTab === 'async'
                ? 'border-blue-500 text-blue-400'
                : 'border-transparent text-gray-400 hover:text-gray-300'
            }`}
          >
            <Clock className="w-3 h-3" />
            Async Metrics Log
          </button>
        </div>

        <div className="p-3 border-b border-gray-700">
          <input
            type="text"
            value={search}
            onChange={e => setSearch(e.target.value)}
            placeholder="Search metrics..."
            className="w-full bg-gray-800 border border-gray-600 rounded px-3 py-1.5 text-xs text-white placeholder-gray-500"
          />
        </div>

        <div className="flex-1 overflow-y-auto p-3">
          {renderMetricsList()}
        </div>

        <div className="flex items-center justify-between p-3 border-t border-gray-700">
          <div className="text-xs text-gray-400">
            {currentSelected.length} metrics selected
          </div>
          <div className="flex items-center gap-2">
            <button
              onClick={onClose}
              className="px-3 py-1.5 text-xs text-gray-400 hover:text-white"
            >
              Cancel
            </button>
            <button
              onClick={handleSave}
              className="flex items-center gap-1.5 px-3 py-1.5 text-xs bg-blue-600 hover:bg-blue-700 text-white rounded"
            >
              <Check className="w-3 h-3" />
              Save
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

// Time range button component
interface TimeRangeButtonProps {
  minutes: TimeRangeMinutes;
  label: string;
  selected: TimeRangeMinutes;
  onClick: (minutes: TimeRangeMinutes) => void;
}

function TimeRangeButton({ minutes, label, selected, onClick }: TimeRangeButtonProps) {
  return (
    <button
      onClick={() => onClick(minutes)}
      className={`px-1.5 py-0.5 rounded text-xs ${
        selected === minutes
          ? 'bg-blue-600 text-white'
          : 'bg-gray-700 hover:bg-gray-600 text-gray-300'
      }`}
    >
      {label}
    </button>
  );
}

export function DashboardTab() {
  const [activeSource, setActiveSource] = useState<DashboardSource>('metrics');
  const [timeRangeMinutes, setTimeRangeMinutes] = useState<TimeRangeMinutes>(60);
  const [metricsData, setMetricsData] = useState<MetricLogTimeSeriesPoint[]>([]);
  const [asyncData, setAsyncData] = useState<MetricLogTimeSeriesPoint[]>([]);
  const [bgPoolData, setBgPoolData] = useState<MetricLogTimeSeriesPoint[]>([]);
  const [loading, setLoading] = useState(true);
  const [columnsLoaded, setColumnsLoaded] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [selectedMetricsLog, setSelectedMetricsLog] = useState<string[]>([]);
  const [selectedAsyncLog, setSelectedAsyncLog] = useState<string[]>([]);
  const [metricsLogColumns, setMetricsLogColumns] = useState<MetricLogColumn[]>([]);
  const [asyncLogColumns, setAsyncLogColumns] = useState<AsyncMetricLogColumn[]>([]);
  const [settingsOpen, setSettingsOpen] = useState(false);

  // Load available columns and defaults on mount
  useEffect(() => {
    async function loadColumns() {
      try {
        const [metricsColumns, metricsDefaults, asyncColumns, asyncDefaults] = await Promise.all([
          fetchMetricLogColumns(),
          fetchMetricLogDefaults(),
          fetchAsyncMetricLogColumns(),
          fetchAsyncMetricLogDefaults(),
        ]);

        setMetricsLogColumns(metricsColumns);
        setAsyncLogColumns(asyncColumns);

        // Load saved metrics from localStorage or use defaults
        const savedMetrics = localStorage.getItem('dashboardMetricsLog');
        const savedAsync = localStorage.getItem('dashboardAsyncLog');
        const savedTimeRange = localStorage.getItem('dashboardTimeRange');

        if (savedMetrics) {
          try {
            const parsed = JSON.parse(savedMetrics);
            const validMetrics = parsed.filter((m: string) =>
              metricsColumns.some(c => c.name === m)
            );
            setSelectedMetricsLog(validMetrics.length > 0 ? validMetrics : metricsDefaults);
          } catch {
            setSelectedMetricsLog(metricsDefaults);
          }
        } else {
          setSelectedMetricsLog(metricsDefaults);
        }

        if (savedAsync) {
          try {
            const parsed = JSON.parse(savedAsync);
            const validMetrics = parsed.filter((m: string) =>
              asyncColumns.some(c => c.name === m)
            );
            setSelectedAsyncLog(validMetrics.length > 0 ? validMetrics : asyncDefaults);
          } catch {
            setSelectedAsyncLog(asyncDefaults);
          }
        } else {
          setSelectedAsyncLog(asyncDefaults);
        }

        if (savedTimeRange) {
          const parsed = parseInt(savedTimeRange, 10);
          if ([15, 60, 360, 1440].includes(parsed)) {
            setTimeRangeMinutes(parsed as TimeRangeMinutes);
          }
        }

        setColumnsLoaded(true);
      } catch (err) {
        console.error('Failed to load columns:', err);
        setError(err instanceof Error ? err.message : 'Failed to load columns');
        setColumnsLoaded(true);
      }
    }
    loadColumns();
  }, []);

  // Determine bucket size based on time range
  const getBucketSize = useCallback((): 'second' | 'minute' | 'hour' => {
    if (timeRangeMinutes <= 15) return 'second';
    if (timeRangeMinutes <= 360) return 'minute';
    return 'hour';
  }, [timeRangeMinutes]);

  const loadData = useCallback(async () => {
    setLoading(true);
    setError(null);

    const end = new Date();
    const start = new Date(end.getTime() - timeRangeMinutes * 60 * 1000);
    const bucket = getBucketSize();

    try {
      // Load data based on active source
      if (activeSource === 'metrics' && selectedMetricsLog.length > 0) {
        const result = await fetchMetricLogTimeSeries(start, end, bucket, selectedMetricsLog);
        setMetricsData(result);
      } else if (activeSource === 'async' && selectedAsyncLog.length > 0) {
        const result = await fetchAsyncMetricLogTimeSeries(start, end, bucket, selectedAsyncLog);
        setAsyncData(result);
      } else if (activeSource === 'bgpool') {
        const result = await fetchMetricLogTimeSeries(start, end, bucket, BACKGROUND_POOL_METRICS);
        setBgPoolData(result);
      }
    } catch (err) {
      console.error('Failed to load dashboard data:', err);
      setError(err instanceof Error ? err.message : 'Failed to load data');
    } finally {
      setLoading(false);
    }
  }, [activeSource, selectedMetricsLog, selectedAsyncLog, timeRangeMinutes, getBucketSize]);

  // Load data when source, metrics, or time range change
  useEffect(() => {
    if (columnsLoaded) {
      loadData();
    }
  }, [loadData, columnsLoaded]);

  // Auto-refresh every 30 seconds
  useEffect(() => {
    const interval = setInterval(loadData, 30000);
    return () => clearInterval(interval);
  }, [loadData]);

  const handleSaveMetricsLog = (metrics: string[]) => {
    setSelectedMetricsLog(metrics);
    localStorage.setItem('dashboardMetricsLog', JSON.stringify(metrics));
  };

  const handleSaveAsyncLog = (metrics: string[]) => {
    setSelectedAsyncLog(metrics);
    localStorage.setItem('dashboardAsyncLog', JSON.stringify(metrics));
  };

  const handleTimeRangeChange = (minutes: TimeRangeMinutes) => {
    setTimeRangeMinutes(minutes);
    localStorage.setItem('dashboardTimeRange', String(minutes));
  };

  const getTimeRangeLabel = () => {
    switch (timeRangeMinutes) {
      case 15: return '15 min';
      case 60: return '1 hour';
      case 360: return '6 hours';
      case 1440: return '24 hours';
      default: return `${timeRangeMinutes} min`;
    }
  };

  const currentData = activeSource === 'metrics' ? metricsData : activeSource === 'async' ? asyncData : bgPoolData;
  const currentMetrics = activeSource === 'metrics' ? selectedMetricsLog : activeSource === 'async' ? selectedAsyncLog : BACKGROUND_POOL_METRICS;

  // Background pool pairs (Size + Task for each pool type)
  const bgPoolPairs = [
    { name: 'Merges & Mutations', size: 'CurrentMetric_BackgroundMergesAndMutationsPoolSize', task: 'CurrentMetric_BackgroundMergesAndMutationsPoolTask' },
    { name: 'Fetches', size: 'CurrentMetric_BackgroundFetchesPoolSize', task: 'CurrentMetric_BackgroundFetchesPoolTask' },
    { name: 'Move', size: 'CurrentMetric_BackgroundMovePoolSize', task: 'CurrentMetric_BackgroundMovePoolTask' },
    { name: 'Common', size: 'CurrentMetric_BackgroundCommonPoolSize', task: 'CurrentMetric_BackgroundCommonPoolTask' },
  ];

  if (error) {
    return (
      <div className="h-full flex items-center justify-center">
        <div className="text-center">
          <div className="text-red-400 text-sm mb-2">{error}</div>
          <button
            onClick={loadData}
            className="text-xs text-blue-400 hover:text-blue-300"
          >
            Retry
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="h-full flex flex-col">
      {/* Header */}
      <div className="bg-gray-900/50 border-b border-gray-700 px-3 py-2 flex items-center justify-between shrink-0">
        <div className="flex items-center gap-4">
          {/* Source Tabs */}
          <div className="flex items-center gap-1 bg-gray-800 rounded p-0.5">
            <button
              onClick={() => setActiveSource('metrics')}
              className={`flex items-center gap-1.5 px-2.5 py-1 text-xs font-medium rounded transition-colors ${
                activeSource === 'metrics'
                  ? 'bg-gray-700 text-white'
                  : 'text-gray-400 hover:text-gray-300'
              }`}
            >
              <BarChart2 className="w-3 h-3" />
              Metrics Log
            </button>
            <button
              onClick={() => setActiveSource('async')}
              className={`flex items-center gap-1.5 px-2.5 py-1 text-xs font-medium rounded transition-colors ${
                activeSource === 'async'
                  ? 'bg-gray-700 text-white'
                  : 'text-gray-400 hover:text-gray-300'
              }`}
            >
              <Clock className="w-3 h-3" />
              Async Log
            </button>
            <button
              onClick={() => setActiveSource('bgpool')}
              className={`flex items-center gap-1.5 px-2.5 py-1 text-xs font-medium rounded transition-colors ${
                activeSource === 'bgpool'
                  ? 'bg-gray-700 text-white'
                  : 'text-gray-400 hover:text-gray-300'
              }`}
            >
              <Layers className="w-3 h-3" />
              Background Pool
            </button>
          </div>

          {/* Time Range Selector */}
          <div className="flex items-center gap-1">
            <TimeRangeButton minutes={15} label="15m" selected={timeRangeMinutes} onClick={handleTimeRangeChange} />
            <TimeRangeButton minutes={60} label="1h" selected={timeRangeMinutes} onClick={handleTimeRangeChange} />
            <TimeRangeButton minutes={360} label="6h" selected={timeRangeMinutes} onClick={handleTimeRangeChange} />
            <TimeRangeButton minutes={1440} label="24h" selected={timeRangeMinutes} onClick={handleTimeRangeChange} />
          </div>

          <span className="text-xs text-gray-500">
            Last {getTimeRangeLabel()} • {currentMetrics.length} metrics
          </span>
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={loadData}
            disabled={loading}
            className="p-1 bg-gray-700 hover:bg-gray-600 rounded text-gray-300 disabled:opacity-50"
            title="Refresh"
          >
            <RefreshCw className={`w-3.5 h-3.5 ${loading ? 'animate-spin' : ''}`} />
          </button>
          {activeSource !== 'bgpool' && (
            <button
              onClick={() => setSettingsOpen(true)}
              className="p-1 bg-gray-700 hover:bg-gray-600 rounded text-gray-300"
              title="Settings"
            >
              <Settings className="w-3.5 h-3.5" />
            </button>
          )}
        </div>
      </div>

      {/* Charts Grid */}
      <div className="flex-1 overflow-y-auto p-3">
        {!columnsLoaded || (loading && currentData.length === 0) ? (
          <div className="h-full flex items-center justify-center">
            <div className="text-gray-400 text-sm">Loading dashboard...</div>
          </div>
        ) : currentData.length === 0 ? (
          <div className="h-full flex items-center justify-center">
            <div className="text-gray-400 text-sm">
              {activeSource === 'bgpool'
                ? 'No background pool data available. Make sure metric_log is enabled.'
                : `No data available. Make sure ${activeSource === 'metrics' ? 'metric_log' : 'asynchronous_metric_log'} is enabled.`
              }
            </div>
          </div>
        ) : activeSource === 'bgpool' ? (
          <div className="grid grid-cols-2 gap-3">
            {bgPoolPairs.map((pool, index) => (
              <PoolChart
                key={pool.name}
                poolName={pool.name}
                sizeMetric={pool.size}
                taskMetric={pool.task}
                data={bgPoolData}
                colorIndex={index}
              />
            ))}
          </div>
        ) : (
          <div className="grid grid-cols-2 gap-3">
            {currentMetrics.map((metric, index) => (
              <MetricChart
                key={metric}
                metric={metric}
                data={currentData}
                colorIndex={index}
                source={activeSource}
              />
            ))}
          </div>
        )}
      </div>

      {/* Settings Modal */}
      <SettingsModal
        isOpen={settingsOpen}
        onClose={() => setSettingsOpen(false)}
        source={activeSource}
        metricsLogColumns={metricsLogColumns}
        asyncLogColumns={asyncLogColumns}
        selectedMetricsLog={selectedMetricsLog}
        selectedAsyncLog={selectedAsyncLog}
        onSaveMetricsLog={handleSaveMetricsLog}
        onSaveAsyncLog={handleSaveAsyncLog}
      />
    </div>
  );
}
