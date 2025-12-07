import { AreaChart, Area, BarChart, Bar, ScatterChart, Scatter, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid } from 'recharts';
import { useQueryStore } from '../stores/queryStore';
import type { ChartMetric, ChartAggregation } from '../stores/queryStore';
import { format, addSeconds, addMinutes, addHours } from 'date-fns';
import { Activity, Clock, HardDrive, Rows3, ArrowDownToLine, ArrowUpFromLine } from 'lucide-react';
import { useCallback, useMemo } from 'react';

const AGGREGATION_LABELS: Record<ChartAggregation, string> = {
  avg: 'Average',
  sum: 'Sum',
  min: 'Min',
  max: 'Max',
};

function getChartConfig(metric: ChartMetric, aggregation: ChartAggregation, isScatter = false) {
  const formatDuration = (v: number) => v >= 1000 ? (v / 1000).toFixed(2) + 's' : v.toFixed(0) + 'ms';
  const formatMemory = (v: number) => {
    if (v >= 1073741824) return (v / 1073741824).toFixed(1) + ' GB';
    if (v >= 1048576) return (v / 1048576).toFixed(1) + ' MB';
    if (v >= 1024) return (v / 1024).toFixed(1) + ' KB';
    return v.toFixed(0) + ' B';
  };
  const formatRows = (v: number) => {
    if (v >= 1000000) return (v / 1000000).toFixed(1) + 'M';
    if (v >= 1000) return (v / 1000).toFixed(1) + 'K';
    return v.toFixed(0);
  };

  // For scatter charts, use simple labels without aggregation prefix
  const aggPrefix = isScatter ? '' : `${AGGREGATION_LABELS[aggregation]} `;

  const configs: Record<ChartMetric, {
    dataKey: string;
    label: string;
    color: string;
    formatter: (value: number) => string;
  }> = {
    count: {
      dataKey: 'count',
      label: 'Queries',
      color: '#3b82f6',
      formatter: (v) => v.toLocaleString(),
    },
    duration: {
      dataKey: `${aggregation}_duration`,
      label: `${aggPrefix}Duration`.trim(),
      color: '#10b981',
      formatter: formatDuration,
    },
    memory: {
      dataKey: `${aggregation}_memory`,
      label: `${aggPrefix}Memory`.trim(),
      color: '#f59e0b',
      formatter: formatMemory,
    },
    read_rows: {
      dataKey: `${aggregation}_read_rows`,
      label: `${aggPrefix}Read Rows`.trim(),
      color: '#06b6d4',
      formatter: formatRows,
    },
    written_rows: {
      dataKey: `${aggregation}_written_rows`,
      label: `${aggPrefix}Written Rows`.trim(),
      color: '#ec4899',
      formatter: formatRows,
    },
    result_rows: {
      dataKey: `${aggregation}_result_rows`,
      label: `${aggPrefix}Result Rows`.trim(),
      color: '#8b5cf6',
      formatter: formatRows,
    },
  };

  return configs[metric];
}

export function TimelineChart() {
  const { timeSeries, stackedTimeSeries, entries, bucketSize, loading, chartMetric, chartType, chartAggregation, setChartMetric, setTimeRange, setSearch } = useQueryStore();

  // For Count metric, allow line and stacked but not scatter
  // For non-Count metrics, don't allow stacked charts (they only make sense for count by query_kind)
  let effectiveChartType = chartType;
  if (chartMetric === 'count' && chartType === 'scatter') {
    effectiveChartType = 'bar';
  } else if (chartMetric !== 'count' && (chartType === 'stacked-bar' || chartType === 'stacked-line')) {
    effectiveChartType = 'bar';
  }

  const config = useMemo(() => {
    const cfg = getChartConfig(chartMetric, chartAggregation, effectiveChartType === 'scatter');
    return cfg;
  }, [chartMetric, chartAggregation, effectiveChartType]);

  // Get the value field for scatter data based on metric
  const getScatterValue = useCallback((entry: typeof entries[0]) => {
    switch (chartMetric) {
      case 'count': return 1;
      case 'duration': return entry.query_duration_ms;
      case 'memory': return entry.memory_usage;
      case 'read_rows': return entry.read_rows;
      case 'written_rows': return entry.written_rows;
      case 'result_rows': return entry.result_rows;
      default: return entry.query_duration_ms;
    }
  }, [chartMetric]);

  // Prepare scatter data from entries
  const scatterData = useMemo(() => {
    if (effectiveChartType !== 'scatter') return [];
    return entries.map(entry => ({
      time: new Date(entry.event_time).getTime(),
      value: getScatterValue(entry),
      query_id: entry.query_id,
      query: entry.query?.substring(0, 100),
      query_kind: entry.query_kind,
    }));
  }, [entries, effectiveChartType, getScatterValue]);

  // Get color based on query kind
  const getScatterColor = (queryKind: string) => {
    switch (queryKind) {
      case 'Insert': return '#60a5fa'; // light blue
      case 'Select': return '#10b981'; // green
      default: return '#9ca3af'; // gray for others
    }
  };

  // Handle click on scatter point to search by query_id
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const handleScatterClick = useCallback((data: any) => {
    const query_id = data?.query_id;
    if (query_id) {
      setSearch(query_id);
    }
  }, [setSearch]);

  const formatTime = (time: string) => {
    const date = new Date(time);
    switch (bucketSize) {
      case 'second':
        return format(date, 'HH:mm:ss');
      case 'hour':
        return format(date, 'MMM d HH:mm');
      default:
        return format(date, 'HH:mm');
    }
  };

  // Handle click on chart to filter to that time bucket
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const handleChartClick = useCallback((data: any) => {
    console.log('Chart clicked, data:', data);

    // Use activeLabel which contains the time string directly
    const timeStr = data?.activeLabel;
    console.log('Time string:', timeStr);

    if (!timeStr) {
      console.log('No time data found in click event');
      return;
    }

    // ClickHouse returns time like "2025-12-05 10:30:00" - convert space to T for proper Date parsing
    const clickedTime = new Date(timeStr.replace(' ', 'T'));
    console.log('Parsed clickedTime:', clickedTime, 'getTime:', clickedTime.getTime());

    if (isNaN(clickedTime.getTime())) {
      console.error('Invalid time value from chart:', timeStr);
      return;
    }

    // Set both start and end to the same clicked time
    console.log('Setting time range:', { start: clickedTime.toISOString(), end: clickedTime.toISOString() });
    setTimeRange({ start: clickedTime, end: clickedTime });
  }, [setTimeRange]);

  const tabs: { metric: ChartMetric; label: string; icon: typeof Activity }[] = [
    { metric: 'count', label: 'Count', icon: Activity },
    { metric: 'duration', label: 'Duration', icon: Clock },
    { metric: 'memory', label: 'Memory Usage', icon: HardDrive },
    { metric: 'read_rows', label: 'Read Rows', icon: ArrowDownToLine },
    { metric: 'written_rows', label: 'Written Rows', icon: ArrowUpFromLine },
    { metric: 'result_rows', label: 'Result Rows', icon: Rows3 },
  ];

  const renderTabs = () => (
    <div className="flex gap-1 border-b border-gray-700 mb-2">
      {tabs.map(({ metric, label, icon: Icon }) => (
        <button
          key={metric}
          onClick={() => setChartMetric(metric)}
          className={`flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium border-b-2 -mb-px transition-colors ${
            chartMetric === metric
              ? 'border-blue-500 text-blue-400'
              : 'border-transparent text-gray-400 hover:text-gray-300'
          }`}
        >
          <Icon className="w-3 h-3" />
          {label}
        </button>
      ))}
    </div>
  );

  if (loading && timeSeries.length === 0) {
    return (
      <div>
        {renderTabs()}
        <div className="h-72 bg-gray-900 border border-gray-700 rounded flex items-center justify-center">
          <span className="text-gray-400 text-xs">Loading...</span>
        </div>
      </div>
    );
  }

  const formatDuration = (v: number) => v >= 1000 ? (v / 1000).toFixed(2) + 's' : v.toFixed(0) + 'ms';

  const formatScatterTime = (timestamp: number) => {
    const date = new Date(timestamp);
    switch (bucketSize) {
      case 'second':
        return format(date, 'HH:mm:ss');
      case 'hour':
        return format(date, 'MMM d HH:mm');
      default:
        return format(date, 'HH:mm');
    }
  };

  // Render scatter chart
  if (effectiveChartType === 'scatter') {
    return (
      <div>
        {renderTabs()}
        <div className="h-72 bg-gray-900 border border-gray-700 rounded cursor-pointer">
          <ResponsiveContainer width="100%" height="100%">
            <ScatterChart margin={{ top: 15, right: 15, left: 5, bottom: 5 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis
                type="number"
                dataKey="time"
                domain={['dataMin', 'dataMax']}
                tickFormatter={formatScatterTime}
                tick={{ fontSize: 9, fill: '#9ca3af' }}
                axisLine={{ stroke: '#374151' }}
                tickLine={{ stroke: '#374151' }}
              />
              <YAxis
                type="number"
                dataKey="value"
                tick={{ fontSize: 9, fill: '#9ca3af' }}
                axisLine={{ stroke: '#374151' }}
                tickLine={{ stroke: '#374151' }}
                width={60}
                tickFormatter={config.formatter}
              />
              <Tooltip
                contentStyle={{
                  backgroundColor: '#1f2937',
                  border: '1px solid #374151',
                  borderRadius: '4px',
                  fontSize: '11px',
                }}
                itemStyle={{ color: '#ffffff' }}
                labelStyle={{ color: '#ffffff' }}
                formatter={(value: number, name: string) => {
                  if (name === 'value') return [config.formatter(value), config.label];
                  if (name === 'time') return [formatScatterTime(value), 'Time'];
                  return [value, name];
                }}
                labelFormatter={() => ''}
              />
              <Scatter
                data={scatterData}
                onClick={handleScatterClick}
                // eslint-disable-next-line @typescript-eslint/no-explicit-any
                shape={(props: any) => {
                  const color = getScatterColor(props.payload?.query_kind);
                  return (
                    <circle
                      cx={props.cx}
                      cy={props.cy}
                      r={3}
                      fill={color}
                      fillOpacity={0.7}
                      style={{ cursor: 'pointer' }}
                    />
                  );
                }}
              />
            </ScatterChart>
          </ResponsiveContainer>
        </div>
      </div>
    );
  }

  // Render stacked bar chart
  if (effectiveChartType === 'stacked-bar') {
    return (
      <div>
        {renderTabs()}
        <div className="h-72 bg-gray-900 border border-gray-700 rounded cursor-pointer">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={stackedTimeSeries} margin={{ top: 15, right: 15, left: 5, bottom: 5 }} onClick={handleChartClick}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis
                dataKey="time"
                tickFormatter={formatTime}
                tick={{ fontSize: 9, fill: '#9ca3af' }}
                axisLine={{ stroke: '#374151' }}
                tickLine={{ stroke: '#374151' }}
              />
              <YAxis
                tick={{ fontSize: 9, fill: '#9ca3af' }}
                axisLine={{ stroke: '#374151' }}
                tickLine={{ stroke: '#374151' }}
                width={50}
                tickFormatter={(v) => v.toLocaleString()}
              />
              <Tooltip
                contentStyle={{
                  backgroundColor: '#1f2937',
                  border: '1px solid #374151',
                  borderRadius: '4px',
                  fontSize: '11px',
                }}
                cursor={{ fill: 'transparent' }}
                labelFormatter={formatTime}
                formatter={(value: number, name: string) => [value.toLocaleString(), name]}
                itemSorter={(item) => {
                  const order = { Select: 0, Insert: 1, Delete: 2, Other: 3 };
                  return order[item.dataKey as keyof typeof order] ?? 4;
                }}
              />
              <Bar dataKey="Select" stackId="1" fill="#10b981" fillOpacity={0.8} />
              <Bar dataKey="Insert" stackId="1" fill="#60a5fa" fillOpacity={0.8} />
              <Bar dataKey="Delete" stackId="1" fill="#ef4444" fillOpacity={0.8} />
              <Bar dataKey="Other" stackId="1" fill="#a855f7" fillOpacity={0.8} />
            </BarChart>
          </ResponsiveContainer>
        </div>
        <div className="flex gap-4 mt-2 justify-center text-xs">
          <div className="flex items-center gap-1">
            <div className="w-3 h-3 rounded" style={{ backgroundColor: '#10b981' }} />
            <span className="text-gray-400">Select</span>
          </div>
          <div className="flex items-center gap-1">
            <div className="w-3 h-3 rounded" style={{ backgroundColor: '#60a5fa' }} />
            <span className="text-gray-400">Insert</span>
          </div>
          <div className="flex items-center gap-1">
            <div className="w-3 h-3 rounded" style={{ backgroundColor: '#ef4444' }} />
            <span className="text-gray-400">Delete</span>
          </div>
          <div className="flex items-center gap-1">
            <div className="w-3 h-3 rounded" style={{ backgroundColor: '#a855f7' }} />
            <span className="text-gray-400">Other</span>
          </div>
        </div>
      </div>
    );
  }

  // Render stacked line (area) chart
  if (effectiveChartType === 'stacked-line') {
    return (
      <div>
        {renderTabs()}
        <div className="h-72 bg-gray-900 border border-gray-700 rounded cursor-pointer">
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={stackedTimeSeries} margin={{ top: 15, right: 15, left: 5, bottom: 5 }} onClick={handleChartClick}>
              <defs>
                <linearGradient id="color-select" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#10b981" stopOpacity={0.6} />
                  <stop offset="95%" stopColor="#10b981" stopOpacity={0.1} />
                </linearGradient>
                <linearGradient id="color-insert" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#60a5fa" stopOpacity={0.6} />
                  <stop offset="95%" stopColor="#60a5fa" stopOpacity={0.1} />
                </linearGradient>
                <linearGradient id="color-delete" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#ef4444" stopOpacity={0.6} />
                  <stop offset="95%" stopColor="#ef4444" stopOpacity={0.1} />
                </linearGradient>
                <linearGradient id="color-other" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#a855f7" stopOpacity={0.6} />
                  <stop offset="95%" stopColor="#a855f7" stopOpacity={0.1} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis
                dataKey="time"
                tickFormatter={formatTime}
                tick={{ fontSize: 9, fill: '#9ca3af' }}
                axisLine={{ stroke: '#374151' }}
                tickLine={{ stroke: '#374151' }}
              />
              <YAxis
                tick={{ fontSize: 9, fill: '#9ca3af' }}
                axisLine={{ stroke: '#374151' }}
                tickLine={{ stroke: '#374151' }}
                width={50}
                tickFormatter={(v) => v.toLocaleString()}
              />
              <Tooltip
                contentStyle={{
                  backgroundColor: '#1f2937',
                  border: '1px solid #374151',
                  borderRadius: '4px',
                  fontSize: '11px',
                }}
                labelFormatter={formatTime}
                formatter={(value: number, name: string) => [value.toLocaleString(), name]}
                itemSorter={(item) => {
                  const order = { Select: 0, Insert: 1, Delete: 2, Other: 3 };
                  return order[item.dataKey as keyof typeof order] ?? 4;
                }}
              />
              <Area
                type="monotone"
                dataKey="Select"
                stackId="1"
                stroke="#10b981"
                fill="url(#color-select)"
              />
              <Area
                type="monotone"
                dataKey="Insert"
                stackId="1"
                stroke="#60a5fa"
                fill="url(#color-insert)"
              />
              <Area
                type="monotone"
                dataKey="Delete"
                stackId="1"
                stroke="#ef4444"
                fill="url(#color-delete)"
              />
              <Area
                type="monotone"
                dataKey="Other"
                stackId="1"
                stroke="#a855f7"
                fill="url(#color-other)"
              />
            </AreaChart>
          </ResponsiveContainer>
        </div>
        <div className="flex gap-4 mt-2 justify-center text-xs">
          <div className="flex items-center gap-1">
            <div className="w-3 h-3 rounded" style={{ backgroundColor: '#10b981' }} />
            <span className="text-gray-400">Select</span>
          </div>
          <div className="flex items-center gap-1">
            <div className="w-3 h-3 rounded" style={{ backgroundColor: '#60a5fa' }} />
            <span className="text-gray-400">Insert</span>
          </div>
          <div className="flex items-center gap-1">
            <div className="w-3 h-3 rounded" style={{ backgroundColor: '#ef4444' }} />
            <span className="text-gray-400">Delete</span>
          </div>
          <div className="flex items-center gap-1">
            <div className="w-3 h-3 rounded" style={{ backgroundColor: '#a855f7' }} />
            <span className="text-gray-400">Other</span>
          </div>
        </div>
      </div>
    );
  }

  // Render bar chart
  if (effectiveChartType === 'bar') {
    return (
      <div>
        {renderTabs()}
        <div className="h-72 bg-gray-900 border border-gray-700 rounded cursor-pointer">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={timeSeries} margin={{ top: 15, right: 15, left: 5, bottom: 5 }} onClick={handleChartClick}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis
                dataKey="time"
                tickFormatter={formatTime}
                tick={{ fontSize: 9, fill: '#9ca3af' }}
                axisLine={{ stroke: '#374151' }}
                tickLine={{ stroke: '#374151' }}
              />
              <YAxis
                tick={{ fontSize: 9, fill: '#9ca3af' }}
                axisLine={{ stroke: '#374151' }}
                tickLine={{ stroke: '#374151' }}
                width={50}
                tickFormatter={config.formatter}
              />
              <Tooltip
                contentStyle={{
                  backgroundColor: '#1f2937',
                  border: '1px solid #374151',
                  borderRadius: '4px',
                  fontSize: '11px',
                }}
                cursor={{ fill: 'transparent' }}
                labelFormatter={formatTime}
                formatter={(value) => [config.formatter(Number(value)), config.label]}
              />
              <Bar
                dataKey={config.dataKey}
                fill={config.color}
                fillOpacity={0.8}
              />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>
    );
  }

  return (
    <div>
      {renderTabs()}
      <div className="h-72 bg-gray-900 border border-gray-700 rounded cursor-pointer">
        <ResponsiveContainer width="100%" height="100%">
          <AreaChart data={timeSeries} margin={{ top: 15, right: 15, left: 5, bottom: 5 }} onClick={handleChartClick}>
            <defs>
              <linearGradient id={`color-${chartMetric}-${chartAggregation}`} x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor={config.color} stopOpacity={0.3} />
                <stop offset="95%" stopColor={config.color} stopOpacity={0} />
              </linearGradient>
            </defs>
            <XAxis
              dataKey="time"
              tickFormatter={formatTime}
              tick={{ fontSize: 9, fill: '#9ca3af' }}
              axisLine={{ stroke: '#374151' }}
              tickLine={{ stroke: '#374151' }}
            />
            <YAxis
              tick={{ fontSize: 9, fill: '#9ca3af' }}
              axisLine={{ stroke: '#374151' }}
              tickLine={{ stroke: '#374151' }}
              width={50}
              tickFormatter={config.formatter}
            />
            <Tooltip
              contentStyle={{
                backgroundColor: '#1f2937',
                border: '1px solid #374151',
                borderRadius: '4px',
                fontSize: '11px',
              }}
              labelFormatter={formatTime}
              formatter={(value) => [config.formatter(Number(value)), config.label]}
            />
            <Area
              key={`${chartMetric}-${chartAggregation}`}
              type="monotone"
              dataKey={config.dataKey}
              stroke={config.color}
              fillOpacity={1}
              fill={`url(#color-${chartMetric}-${chartAggregation})`}
            />
          </AreaChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
