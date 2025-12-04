import { useState, useEffect } from 'react';
import { BarChart, Bar, XAxis, YAxis, Tooltip, Cell } from 'recharts';
import { useQueryStore } from '../stores/queryStore';
import { fetchHistogram } from '../services/api';
import type { HistogramData } from '../types/queryLog';
import { HISTOGRAM_FIELDS } from '../types/queryLog';

const COLORS = ['#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6', '#ec4899', '#06b6d4', '#84cc16'];

interface HistogramCardProps {
  field: string;
  label: string;
  data: HistogramData[];
  loading: boolean;
  onFilterClick: (field: string, value: string) => void;
}

const BAR_HEIGHT = 15;
const MAX_ITEMS = 10;

function HistogramCard({ field, label, data, loading, onFilterClick }: HistogramCardProps) {
  const displayData = data.slice(0, MAX_ITEMS);
  const chartHeight = displayData.length * (BAR_HEIGHT + 8) + 20; // bar height + gap + padding

  if (loading) {
    return (
      <div className="bg-gray-900 border border-gray-700 rounded p-3">
        <h3 className="text-xs font-semibold text-gray-400 mb-2">{label}</h3>
        <div className="h-64 flex items-center justify-center text-gray-500 text-xs">Loading...</div>
      </div>
    );
  }

  if (data.length === 0) {
    return (
      <div className="bg-gray-900 border border-gray-700 rounded p-3">
        <h3 className="text-xs font-semibold text-gray-400 mb-2">{label}</h3>
        <div className="h-64 flex items-center justify-center text-gray-500 text-xs">No data</div>
      </div>
    );
  }

  const minBoxHeight = 200;

  return (
    <div className="bg-gray-900 border border-gray-700 rounded p-3">
      <h3 className="text-xs font-semibold text-gray-400 mb-3">{label}</h3>
      <div style={{ minHeight: minBoxHeight }}>
        <BarChart data={displayData} layout="vertical" margin={{ top: 0, right: 10, left: 0, bottom: 0 }} barSize={BAR_HEIGHT} width={280} height={chartHeight}>
          <XAxis type="number" tick={{ fontSize: 9, fill: '#9ca3af' }} axisLine={false} tickLine={false} />
          <YAxis
            type="category"
            dataKey="name"
            tick={{ fontSize: 10, fill: '#d1d5db' }}
            axisLine={false}
            tickLine={false}
            width={100}
            tickFormatter={(value) => {
              const str = String(value || '(empty)');
              return str.length > 15 ? str.substring(0, 15) + '...' : str;
            }}
          />
          <Tooltip
            contentStyle={{
              backgroundColor: '#1f2937',
              border: '1px solid #374151',
              borderRadius: '4px',
              fontSize: '11px',
            }}
            cursor={{ fill: 'rgba(255,255,255,0.05)' }}
            formatter={(value: number, name: string, props: { payload: HistogramData }) => [
              value.toLocaleString(),
              props.payload.name || '(empty)'
            ]}
          />
          <Bar
            dataKey="count"
            cursor="pointer"
            onClick={(data) => onFilterClick(field, data.name as string)}
            label={{
              position: 'right',
              fill: '#9ca3af',
              fontSize: 9,
              formatter: (value: number) => value.toLocaleString(),
            }}
          >
            {displayData.map((_, index) => (
              <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
            ))}
          </Bar>
        </BarChart>
      </div>
    </div>
  );
}

export function HistogramsTab() {
  const { timeRange, search, fieldFilters, setFieldFilter } = useQueryStore();
  const [histogramData, setHistogramData] = useState<Record<string, HistogramData[]>>({});
  const [loading, setLoading] = useState<Record<string, boolean>>({});

  useEffect(() => {
    const loadHistograms = async () => {
      const newLoading: Record<string, boolean> = {};
      HISTOGRAM_FIELDS.forEach((f) => (newLoading[f.field] = true));
      setLoading(newLoading);

      const results: Record<string, HistogramData[]> = {};

      await Promise.all(
        HISTOGRAM_FIELDS.map(async ({ field }) => {
          try {
            const data = await fetchHistogram(field, timeRange, search, fieldFilters);
            results[field] = data;
          } catch (error) {
            console.error(`Failed to load histogram for ${field}:`, error);
            results[field] = [];
          }
        })
      );

      setHistogramData(results);
      setLoading({});
    };

    loadHistograms();
  }, [timeRange, search, fieldFilters]);

  const handleFilterClick = (field: string, value: string) => {
    const current = fieldFilters[field] || [];
    if (!current.includes(value)) {
      setFieldFilter(field, [...current, value]);
    }
  };

  return (
    <div className="p-3 overflow-auto h-full">
      <div className="grid grid-cols-4 gap-3">
        {HISTOGRAM_FIELDS.map(({ field, label }) => (
          <HistogramCard
            key={field}
            field={field}
            label={label}
            data={histogramData[field] || []}
            loading={loading[field] || false}
            onFilterClick={handleFilterClick}
          />
        ))}
      </div>
    </div>
  );
}
