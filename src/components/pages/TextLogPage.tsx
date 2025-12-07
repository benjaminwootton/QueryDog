import { useState, useEffect, useCallback, useMemo } from 'react';
import { FileText, ChevronLeft, ChevronRight, Filter, X, Search, Activity, AlertTriangle } from 'lucide-react';
import { AgGridReact } from 'ag-grid-react';
import { AllCommunityModule, ModuleRegistry, themeAlpine } from 'ag-grid-community';
import type { ColDef, ICellRendererParams, RowClickedEvent, SortChangedEvent } from 'ag-grid-community';
import { useQueryStore } from '../../stores/queryStore';
import {
  fetchTextLog,
  fetchTextLogCount,
  fetchTextLogTimeSeries,
  fetchTextLogDistinct,
  type TextLogEntry,
  type TextLogTimeSeriesPoint,
} from '../../services/api';
import { TimeRangeSelector } from '../TimeRangeSelector';
import { AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer } from 'recharts';
import { addSeconds, addMinutes, addHours } from 'date-fns';

// Register AG Grid Community modules
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

function formatDateTime(dateStr: string): string {
  const date = new Date(dateStr);
  return date.toLocaleString('en-US', {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  });
}

function getLevelColor(level: string): string {
  switch (level) {
    case 'Fatal':
      return 'text-red-500 bg-red-500/20';
    case 'Error':
      return 'text-red-400 bg-red-400/20';
    case 'Warning':
      return 'text-yellow-400 bg-yellow-400/20';
    case 'Information':
      return 'text-blue-400 bg-blue-400/20';
    case 'Debug':
      return 'text-gray-400 bg-gray-400/20';
    case 'Trace':
      return 'text-gray-500 bg-gray-500/20';
    default:
      return 'text-gray-400 bg-gray-400/20';
  }
}

export function TextLogPage() {
  const { timeRange, bucketSize, setTimeRange } = useQueryStore();

  const [entries, setEntries] = useState<TextLogEntry[]>([]);
  const [timeSeries, setTimeSeries] = useState<TextLogTimeSeriesPoint[]>([]);
  const [totalCount, setTotalCount] = useState(0);
  const [loading, setLoading] = useState(false);
  const [search, setSearch] = useState('');
  const [searchInput, setSearchInput] = useState('');
  const [filters, setFilters] = useState<Record<string, string[]>>({});
  const [sortField, setSortField] = useState('event_time');
  const [sortOrder, setSortOrder] = useState<'ASC' | 'DESC'>('DESC');
  const [currentPage, setCurrentPage] = useState(0);
  const [selectedEntry, setSelectedEntry] = useState<TextLogEntry | null>(null);

  // Filter dropdown state
  const [filterDropdownOpen, setFilterDropdownOpen] = useState(false);
  const [levelOptions, setLevelOptions] = useState<string[]>([]);

  // Chart tab state
  const [chartTab, setChartTab] = useState<'count' | 'errors'>('count');

  // Handle tab change with filter update
  const handleChartTabChange = useCallback((tab: 'count' | 'errors') => {
    setChartTab(tab);
    if (tab === 'errors') {
      setFilters(prev => ({ ...prev, level: ['Error', 'Warning', 'Fatal'] }));
    } else {
      // When switching back to count, remove the level filter
      setFilters(prev => {
        const { level: _, ...rest } = prev;
        return rest;
      });
    }
    setCurrentPage(0);
  }, []);

  const pageSize = 500;

  const loadData = useCallback(async () => {
    setLoading(true);
    try {
      const [data, count, series] = await Promise.all([
        fetchTextLog(timeRange, search, sortField, sortOrder, filters, pageSize, currentPage * pageSize),
        fetchTextLogCount(timeRange, search, filters),
        fetchTextLogTimeSeries(timeRange, bucketSize, filters),
      ]);
      setEntries(data);
      setTotalCount(count);
      setTimeSeries(series);
    } catch (error) {
      console.error('Error loading text log:', error);
    } finally {
      setLoading(false);
    }
  }, [timeRange, search, sortField, sortOrder, filters, currentPage, bucketSize]);

  // Load filter options
  useEffect(() => {
    fetchTextLogDistinct('level', timeRange)
      .then(setLevelOptions)
      .catch(console.error);
  }, [timeRange]);

  useEffect(() => {
    loadData();
  }, [loadData]);

  const handleSearch = () => {
    setSearch(searchInput);
    setCurrentPage(0);
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter') {
      handleSearch();
    }
  };

  const toggleFilter = (field: string, value: string) => {
    setFilters(prev => {
      const current = prev[field] || [];
      const newValues = current.includes(value)
        ? current.filter(v => v !== value)
        : [...current, value];

      if (newValues.length === 0) {
        const { [field]: _, ...rest } = prev;
        return rest;
      }
      return { ...prev, [field]: newValues };
    });
    setCurrentPage(0);
  };

  const clearFilters = () => {
    setFilters({});
    setSearch('');
    setSearchInput('');
    setCurrentPage(0);
  };

  // AG Grid column definitions
  const columnDefs: ColDef<TextLogEntry>[] = useMemo(() => [
    {
      headerName: 'Time',
      field: 'event_time',
      width: 160,
      sortable: true,
      sort: 'desc',
      valueFormatter: (params) => formatDateTime(params.value),
    },
    {
      headerName: 'Level',
      field: 'level',
      width: 90,
      sortable: true,
      cellRenderer: (params: ICellRendererParams<TextLogEntry>) => (
        <span className={`px-1.5 py-0.5 rounded text-[10px] font-medium ${getLevelColor(params.value)}`}>
          {params.value}
        </span>
      ),
    },
    {
      headerName: 'Logger',
      field: 'logger_name',
      width: 180,
      sortable: true,
      tooltipField: 'logger_name',
    },
    {
      headerName: 'Message',
      field: 'message',
      flex: 1,
      minWidth: 300,
      sortable: false,
      tooltipField: 'message',
    },
  ], []);

  const defaultColDef = useMemo(() => ({
    resizable: true,
  }), []);

  const onSortChanged = useCallback((event: SortChangedEvent) => {
    const sortModel = event.api.getColumnState().find(c => c.sort);
    if (sortModel) {
      setSortField(sortModel.colId);
      setSortOrder(sortModel.sort === 'asc' ? 'ASC' : 'DESC');
    }
  }, []);

  const onRowClicked = useCallback((event: RowClickedEvent<TextLogEntry>) => {
    if (event.data) {
      setSelectedEntry(selectedEntry?.event_time === event.data.event_time &&
                       selectedEntry?.message === event.data.message ? null : event.data);
    }
  }, [selectedEntry]);

  // Handle click on chart to filter to that time bucket
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const handleChartClick = useCallback((data: any) => {
    const timeStr = data?.activeLabel;
    if (!timeStr) return;

    const clickedTime = new Date(timeStr.replace(' ', 'T'));
    if (isNaN(clickedTime.getTime())) return;

    let endTime: Date;
    switch (bucketSize) {
      case 'second':
        endTime = addSeconds(clickedTime, 1);
        break;
      case 'hour':
        endTime = addHours(clickedTime, 1);
        break;
      default:
        endTime = addMinutes(clickedTime, 1);
        break;
    }

    setTimeRange({ start: clickedTime, end: endTime });

    // If clicking on Errors & Warnings chart, also set the level filter
    if (chartTab === 'errors') {
      setFilters(prev => ({ ...prev, level: ['Error', 'Warning', 'Fatal'] }));
      setCurrentPage(0);
    }
  }, [bucketSize, setTimeRange, chartTab]);

  const totalPages = Math.ceil(totalCount / pageSize);
  const startRow = currentPage * pageSize + 1;
  const endRow = Math.min((currentPage + 1) * pageSize, totalCount);
  const activeFilterCount = Object.values(filters).filter(v => v.length > 0).length + (search ? 1 : 0);

  return (
    <div className="h-full flex flex-col">
      {/* Filter bar */}
      <div className="bg-gray-900/50 border-b border-gray-700 px-4 py-2 flex items-center justify-between shrink-0">
        <div className="flex items-center gap-3">
          {/* Filter dropdown */}
          <div className="relative">
            <button
              onClick={() => setFilterDropdownOpen(!filterDropdownOpen)}
              className={`flex items-center gap-1 px-2 py-0.5 rounded text-xs ${
                activeFilterCount > 0
                  ? 'bg-blue-600 text-white'
                  : 'bg-gray-700 hover:bg-gray-600 text-gray-300'
              }`}
            >
              <Filter className="w-3 h-3" />
              <span>Filters</span>
              {activeFilterCount > 0 && (
                <span className="bg-white/20 px-1 rounded text-xs">{activeFilterCount}</span>
              )}
            </button>

            {filterDropdownOpen && (
              <>
                <div className="fixed inset-0 z-40" onClick={() => setFilterDropdownOpen(false)} />
                <div className="absolute left-0 top-full mt-1 bg-gray-800 border border-gray-600 rounded shadow-lg z-50 min-w-[180px]">
                  <div className="flex items-center justify-between p-2 border-b border-gray-700">
                    <span className="text-xs font-semibold text-gray-300">Level</span>
                    <button onClick={() => setFilterDropdownOpen(false)} className="text-gray-400 hover:text-white">
                      <X className="w-3 h-3" />
                    </button>
                  </div>
                  <div className="p-2 space-y-1">
                    {levelOptions.map(level => (
                      <label
                        key={level}
                        className="flex items-center gap-2 cursor-pointer hover:bg-gray-700/50 rounded px-1 py-0.5"
                      >
                        <input
                          type="checkbox"
                          checked={filters.level?.includes(level) || false}
                          onChange={() => toggleFilter('level', level)}
                          className="w-3 h-3 rounded border-gray-500 bg-gray-700 text-blue-500 focus:ring-blue-500 focus:ring-offset-0"
                        />
                        <span className={`text-xs ${getLevelColor(level)} px-1.5 py-0.5 rounded`}>
                          {level}
                        </span>
                      </label>
                    ))}
                  </div>
                </div>
              </>
            )}
          </div>

          <TimeRangeSelector />

          {/* Search - styled like SearchBar component */}
          <div className="relative flex items-center">
            <Search className="absolute left-2 w-3 h-3 text-gray-400" />
            <input
              type="text"
              value={searchInput}
              onChange={(e) => setSearchInput(e.target.value)}
              onKeyDown={handleKeyDown}
              placeholder="Search messages..."
              className="bg-gray-800 border border-gray-600 rounded pl-6 pr-6 py-0.5 text-white text-xs w-64"
            />
            {searchInput && (
              <button
                onClick={() => { setSearchInput(''); setSearch(''); }}
                className="absolute right-2 text-gray-400 hover:text-white"
              >
                <X className="w-3 h-3" />
              </button>
            )}
          </div>
        </div>

        <div className="flex items-center gap-4 text-xs">
          <span className="text-gray-400">
            Total: <span className="text-white font-medium">{totalCount.toLocaleString()}</span> entries
          </span>
        </div>
      </div>

      {/* Active filters display */}
      {activeFilterCount > 0 && (
        <div className="px-4 py-2 bg-gray-800/50 border-b border-gray-700 flex items-center gap-2 flex-wrap">
          <span className="text-xs text-gray-400">Active:</span>
          {Object.entries(filters).map(([field, values]) =>
            values.map(value => (
              <span
                key={`${field}-${value}`}
                className="inline-flex items-center gap-1 px-2 py-0.5 bg-blue-600/30 text-blue-300 text-xs rounded"
              >
                {field}: {value}
                <button onClick={() => toggleFilter(field, value)} className="hover:text-white">
                  <X className="w-3 h-3" />
                </button>
              </span>
            ))
          )}
          {search && (
            <span className="inline-flex items-center gap-1 px-2 py-0.5 bg-green-600/30 text-green-300 text-xs rounded">
              search: {search}
              <button onClick={() => { setSearch(''); setSearchInput(''); }} className="hover:text-white">
                <X className="w-3 h-3" />
              </button>
            </span>
          )}
        </div>
      )}

      {/* Timeline Chart */}
      <div className="px-4 pt-4 pb-3 shrink-0">
        <div className="flex gap-1 border-b border-gray-700 mb-2">
          <button
            onClick={() => handleChartTabChange('count')}
            className={`flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium border-b-2 -mb-px transition-colors ${
              chartTab === 'count'
                ? 'border-blue-500 text-blue-400'
                : 'border-transparent text-gray-400 hover:text-gray-300'
            }`}
          >
            <Activity className="w-3 h-3" />
            Count
          </button>
          <button
            onClick={() => handleChartTabChange('errors')}
            className={`flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium border-b-2 -mb-px transition-colors ${
              chartTab === 'errors'
                ? 'border-blue-500 text-blue-400'
                : 'border-transparent text-gray-400 hover:text-gray-300'
            }`}
          >
            <AlertTriangle className="w-3 h-3" />
            Errors & Warnings
          </button>
        </div>
        <div className="h-36 bg-gray-900 border border-gray-700 rounded relative cursor-pointer">
          {loading && timeSeries.length === 0 && (
            <div className="absolute inset-0 flex items-center justify-center">
              <span className="text-gray-400 text-xs">Loading...</span>
            </div>
          )}
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={timeSeries} margin={{ top: 15, right: 15, left: 5, bottom: 5 }} onClick={handleChartClick}>
              <defs>
                <linearGradient id="color-total" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#3b82f6" stopOpacity={0.3} />
                  <stop offset="95%" stopColor="#3b82f6" stopOpacity={0} />
                </linearGradient>
                <linearGradient id="color-errors" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#ef4444" stopOpacity={0.3} />
                  <stop offset="95%" stopColor="#ef4444" stopOpacity={0} />
                </linearGradient>
                <linearGradient id="color-warnings" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#eab308" stopOpacity={0.3} />
                  <stop offset="95%" stopColor="#eab308" stopOpacity={0} />
                </linearGradient>
              </defs>
              <XAxis
                dataKey="time"
                tickFormatter={(value) => {
                  const date = new Date(value);
                  return date.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' });
                }}
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
                labelFormatter={(value) => new Date(value).toLocaleString()}
              />
              {chartTab === 'count' && (
                <Area type="monotone" dataKey="count" name="Total" stroke="#3b82f6" fillOpacity={1} fill="url(#color-total)" />
              )}
              {chartTab === 'errors' && (
                <>
                  <Area type="monotone" dataKey="errors" name="Errors" stroke="#ef4444" fillOpacity={1} fill="url(#color-errors)" />
                  <Area type="monotone" dataKey="warnings" name="Warnings" stroke="#eab308" fillOpacity={1} fill="url(#color-warnings)" />
                </>
              )}
            </AreaChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Tabs */}
      <div className="border-b border-gray-700 mx-4 flex items-center gap-1 shrink-0">
        <button
          className="flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium border-b-2 -mb-px transition-colors border-blue-500 text-blue-400"
        >
          <FileText className="w-3 h-3" />
          Log Entries
        </button>
        <div className="ml-auto flex items-center gap-4">
          {totalPages > 1 && (
            <div className="flex items-center gap-2 text-xs">
              <span className="text-gray-400">
                {startRow.toLocaleString()}-{endRow.toLocaleString()} of {totalCount.toLocaleString()}
              </span>
              <div className="flex items-center gap-1">
                <button
                  onClick={() => setCurrentPage(currentPage - 1)}
                  disabled={currentPage === 0}
                  className="p-1 bg-gray-700 hover:bg-gray-600 rounded text-gray-300 disabled:opacity-50 disabled:cursor-not-allowed"
                  title="Previous page"
                >
                  <ChevronLeft className="w-4 h-4" />
                </button>
                <span className="text-gray-300 px-2">
                  {currentPage + 1} / {totalPages}
                </span>
                <button
                  onClick={() => setCurrentPage(currentPage + 1)}
                  disabled={currentPage >= totalPages - 1}
                  className="p-1 bg-gray-700 hover:bg-gray-600 rounded text-gray-300 disabled:opacity-50 disabled:cursor-not-allowed"
                  title="Next page"
                >
                  <ChevronRight className="w-4 h-4" />
                </button>
              </div>
            </div>
          )}
        </div>
      </div>

      {/* Log entries table - AG Grid */}
      <div className="flex-1 overflow-hidden p-4">
        <AgGridReact
          theme={darkTheme}
          rowData={entries}
          columnDefs={columnDefs}
          defaultColDef={defaultColDef}
          onSortChanged={onSortChanged}
          onRowClicked={onRowClicked}
          getRowId={(params) => `${params.data.event_time}-${params.data.message?.slice(0, 50)}`}
          rowSelection="single"
          suppressCellFocus={true}
          enableCellTextSelection={true}
          loading={loading}
          animateRows={false}
        />
      </div>

      {/* Detail panel */}
      {selectedEntry && (
        <div className="border-t border-gray-700 bg-gray-800/50 p-4 shrink-0 max-h-[300px] overflow-auto">
          <div className="flex items-center justify-between mb-3">
            <h3 className="text-sm font-semibold text-white">Log Entry Details</h3>
            <button onClick={() => setSelectedEntry(null)} className="text-gray-400 hover:text-white">
              <X className="w-4 h-4" />
            </button>
          </div>
          <div className="grid grid-cols-4 gap-4 mb-3 text-xs">
            <div>
              <div className="text-gray-500 uppercase">Time</div>
              <div className="text-gray-300 font-mono">{formatDateTime(selectedEntry.event_time)}</div>
            </div>
            <div>
              <div className="text-gray-500 uppercase">Level</div>
              <span className={`px-1.5 py-0.5 rounded text-[10px] font-medium ${getLevelColor(selectedEntry.level)}`}>
                {selectedEntry.level}
              </span>
            </div>
            <div>
              <div className="text-gray-500 uppercase">Logger</div>
              <div className="text-gray-300 font-mono">{selectedEntry.logger_name}</div>
            </div>
            <div>
              <div className="text-gray-500 uppercase">Thread</div>
              <div className="text-gray-300 font-mono">{selectedEntry.thread_name} ({selectedEntry.thread_id})</div>
            </div>
          </div>
          {selectedEntry.query_id && (
            <div className="mb-3 text-xs">
              <div className="text-gray-500 uppercase">Query ID</div>
              <div className="text-gray-300 font-mono">{selectedEntry.query_id}</div>
            </div>
          )}
          <div className="text-xs">
            <div className="text-gray-500 uppercase mb-1">Message</div>
            <pre className="bg-gray-900 p-3 rounded text-gray-200 whitespace-pre-wrap font-mono text-[11px] max-h-32 overflow-auto">
              {selectedEntry.message}
            </pre>
          </div>
          {selectedEntry.source_file && (
            <div className="mt-2 text-xs text-gray-500">
              Source: {selectedEntry.source_file}:{selectedEntry.source_line}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
