import { useQueryStore } from '../stores/queryStore';
import type { BucketSize } from '../types/queryLog';
import type { ChartAggregation, ChartType } from '../stores/queryStore';

const AGGREGATION_LABELS: Record<ChartAggregation, string> = {
  avg: 'Average',
  sum: 'Sum',
  min: 'Min',
  max: 'Max',
};

interface TimeRangeSelectorProps {
  usePartLogMetric?: boolean;
}

export function TimeRangeSelector({ usePartLogMetric = false }: TimeRangeSelectorProps) {
  const { timeRange, setTimeRange, bucketSize, setBucketSize, chartType, setChartType, chartMetric, partLogChartMetric, chartAggregation, setChartAggregation } = useQueryStore();

  // When Count metric is selected, allow line and stacked but not scatter
  // Use partLogChartMetric when on the Part Log page
  const effectiveMetric = usePartLogMetric ? partLogChartMetric : chartMetric;
  const isCountMetric = effectiveMetric === 'count';
  const effectiveChartType = isCountMetric && chartType === 'scatter' ? 'line' : chartType;

  const formatForInput = (date: Date) => {
    return date.toISOString().slice(0, 16);
  };

  const handleStartChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const newStart = new Date(e.target.value);
    if (!isNaN(newStart.getTime())) {
      setTimeRange({ ...timeRange, start: newStart });
    }
  };

  const handleEndChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const newEnd = new Date(e.target.value);
    if (!isNaN(newEnd.getTime())) {
      setTimeRange({ ...timeRange, end: newEnd });
    }
  };

  const setQuickRange = (minutes: number) => {
    const end = new Date();
    const start = new Date(end.getTime() - minutes * 60 * 1000);
    setTimeRange({ start, end });
  };

  return (
    <div className="flex items-center gap-2 text-xs">
      <div className="flex items-center gap-1">
        <label className="text-gray-400">From:</label>
        <input
          type="datetime-local"
          value={formatForInput(timeRange.start)}
          onChange={handleStartChange}
          className="bg-gray-800 border border-gray-600 rounded px-1.5 py-0.5 text-white text-xs"
        />
      </div>
      <div className="flex items-center gap-1">
        <label className="text-gray-400">To:</label>
        <input
          type="datetime-local"
          value={formatForInput(timeRange.end)}
          onChange={handleEndChange}
          className="bg-gray-800 border border-gray-600 rounded px-1.5 py-0.5 text-white text-xs"
        />
      </div>
      <div className="flex gap-1 ml-2">
        <button
          onClick={() => setQuickRange(15)}
          className="px-1.5 py-0.5 bg-gray-700 hover:bg-gray-600 rounded text-gray-300"
        >
          15m
        </button>
        <button
          onClick={() => setQuickRange(60)}
          className="px-1.5 py-0.5 bg-gray-700 hover:bg-gray-600 rounded text-gray-300"
        >
          1h
        </button>
        <button
          onClick={() => setQuickRange(360)}
          className="px-1.5 py-0.5 bg-gray-700 hover:bg-gray-600 rounded text-gray-300"
        >
          6h
        </button>
        <button
          onClick={() => setQuickRange(1440)}
          className="px-1.5 py-0.5 bg-gray-700 hover:bg-gray-600 rounded text-gray-300"
        >
          24h
        </button>
      </div>
      <div className="flex items-center gap-1 ml-2 border-l border-gray-600 pl-2">
        <label className="text-gray-400">Type:</label>
        <select
          value={effectiveChartType}
          onChange={(e) => setChartType(e.target.value as ChartType)}
          className="bg-gray-800 border border-gray-600 rounded px-1.5 py-0.5 text-white text-xs"
        >
          <option value="bar">Bar</option>
          {isCountMetric && <option value="stacked-bar">Stacked Bar</option>}
          <option value="line">Line</option>
          {isCountMetric && <option value="stacked-line">Stacked Line</option>}
          {!isCountMetric && <option value="scatter">Scatter</option>}
        </select>
      </div>
      {(effectiveChartType === 'line' || effectiveChartType === 'bar' || effectiveChartType === 'stacked-bar' || effectiveChartType === 'stacked-line') && (
        <>
          <div className="flex items-center gap-1 ml-2 border-l border-gray-600 pl-2">
            <label className="text-gray-400">Bucket:</label>
            <select
              value={bucketSize}
              onChange={(e) => setBucketSize(e.target.value as BucketSize)}
              className="bg-gray-800 border border-gray-600 rounded px-1.5 py-0.5 text-white text-xs"
            >
              <option value="second">Second</option>
              <option value="minute">Minute</option>
              <option value="hour">Hour</option>
            </select>
          </div>
          {!isCountMetric && (
            <div className="flex items-center gap-1 ml-2 border-l border-gray-600 pl-2">
              <label className="text-gray-400">Aggregation:</label>
              <select
                value={chartAggregation}
                onChange={(e) => setChartAggregation(e.target.value as ChartAggregation)}
                className="bg-gray-800 border border-gray-600 rounded px-1.5 py-0.5 text-white text-xs"
              >
                {(Object.keys(AGGREGATION_LABELS) as ChartAggregation[]).map((agg) => (
                  <option key={agg} value={agg}>
                    {AGGREGATION_LABELS[agg]}
                  </option>
                ))}
              </select>
            </div>
          )}
        </>
      )}
    </div>
  );
}
