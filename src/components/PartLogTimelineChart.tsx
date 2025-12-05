import { useState } from 'react';
import { AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid } from 'recharts';
import { useQueryStore } from '../stores/queryStore';
import { Activity, Clock } from 'lucide-react';
import { format } from 'date-fns';

type PartLogChartMetric = 'count' | 'duration';

const CHART_CONFIGS: Record<PartLogChartMetric, {
  dataKey: string;
  label: string;
  color: string;
  formatter: (value: number) => string;
}> = {
  count: {
    dataKey: 'count',
    label: 'Events',
    color: '#10b981',
    formatter: (v) => v.toLocaleString(),
  },
  duration: {
    dataKey: 'avg_duration',
    label: 'Avg Duration',
    color: '#3b82f6',
    formatter: (v) => v >= 1000 ? (v / 1000).toFixed(2) + 's' : v.toFixed(0) + 'ms',
  },
};

export function PartLogTimelineChart() {
  const { partLogTimeSeries, bucketSize, partLogLoading } = useQueryStore();
  const [chartMetric, setChartMetric] = useState<PartLogChartMetric>('count');

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

  const tabs: { metric: PartLogChartMetric; label: string; icon: typeof Activity }[] = [
    { metric: 'count', label: 'Count', icon: Activity },
    { metric: 'duration', label: 'Duration', icon: Clock },
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

  if (partLogLoading && partLogTimeSeries.length === 0) {
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
          <AreaChart data={partLogTimeSeries} margin={{ top: 15, right: 15, left: 5, bottom: 5 }}>
            <defs>
              <linearGradient id={`partLog-${chartMetric}`} x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor={config.color} stopOpacity={0.3} />
                <stop offset="95%" stopColor={config.color} stopOpacity={0} />
              </linearGradient>
            </defs>
            <CartesianGrid strokeDasharray="3 3" stroke="#374151" vertical={false} />
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
              type="monotone"
              dataKey={config.dataKey}
              stroke={config.color}
              strokeWidth={1.5}
              fillOpacity={1}
              fill={`url(#partLog-${chartMetric})`}
              isAnimationActive={false}
            />
          </AreaChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
