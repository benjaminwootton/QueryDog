import { AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer } from 'recharts';
import { useQueryStore } from '../stores/queryStore';
import type { ChartMetric } from '../stores/queryStore';
import { format } from 'date-fns';
import { Activity, Clock, HardDrive } from 'lucide-react';

const CHART_CONFIGS: Record<ChartMetric, {
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
    dataKey: 'avg_duration',
    label: 'Avg Duration',
    color: '#10b981',
    formatter: (v) => v >= 1000 ? (v / 1000).toFixed(2) + 's' : v.toFixed(0) + 'ms',
  },
  memory: {
    dataKey: 'avg_memory',
    label: 'Avg Memory',
    color: '#f59e0b',
    formatter: (v) => {
      if (v >= 1073741824) return (v / 1073741824).toFixed(1) + ' GB';
      if (v >= 1048576) return (v / 1048576).toFixed(1) + ' MB';
      if (v >= 1024) return (v / 1024).toFixed(1) + ' KB';
      return v.toFixed(0) + ' B';
    },
  },
};

export function TimelineChart() {
  const { timeSeries, bucketSize, loading, chartMetric, setChartMetric } = useQueryStore();

  const config = CHART_CONFIGS[chartMetric];

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

  const tabs: { metric: ChartMetric; label: string; icon: typeof Activity }[] = [
    { metric: 'count', label: 'Count', icon: Activity },
    { metric: 'duration', label: 'Duration', icon: Clock },
    { metric: 'memory', label: 'Memory', icon: HardDrive },
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
        <div className="h-36 bg-gray-900 border border-gray-700 rounded flex items-center justify-center">
          <span className="text-gray-400 text-xs">Loading...</span>
        </div>
      </div>
    );
  }

  return (
    <div>
      {renderTabs()}
      <div className="h-36 bg-gray-900 border border-gray-700 rounded">
        <ResponsiveContainer width="100%" height="100%">
          <AreaChart data={timeSeries} margin={{ top: 15, right: 15, left: 5, bottom: 5 }}>
            <defs>
              <linearGradient id={`color-${chartMetric}`} x1="0" y1="0" x2="0" y2="1">
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
              formatter={(value: number) => [config.formatter(value), config.label]}
            />
            <Area
              type="monotone"
              dataKey={config.dataKey}
              stroke={config.color}
              fillOpacity={1}
              fill={`url(#color-${chartMetric})`}
            />
          </AreaChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
