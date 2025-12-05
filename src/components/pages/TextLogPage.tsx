import { useState, useEffect, useCallback } from 'react';
import { FileText, ChevronLeft, ChevronRight, Filter, X, Search, Activity } from 'lucide-react';
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
import { AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer, Legend } from 'recharts';

function formatDateTime(dateStr: string): string {
  const date = new Date(dateStr);
  return date.toLocaleString('en-US', {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
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
  const { timeRange, bucketSize } = useQueryStore();

  const [entries, setEntries] = useState<TextLogEntry[]>([]);
  const [timeSeries, setTimeSeries] = useState<TextLogTimeSeriesPoint[]>([]);
  const [totalCount, setTotalCount] = useState(0);
  const [loading, setLoading] = useState(false);
  const [search, setSearch] = useState('');
  const [searchInput, setSearchInput] = useState('');
  const [filters, setFilters] = useState<Record<string, string[]>>({});
  const [sortField] = useState('event_time');
  const [sortOrder] = useState<'ASC' | 'DESC'>('DESC');
  const [currentPage, setCurrentPage] = useState(0);
  const [selectedEntry, setSelectedEntry] = useState<TextLogEntry | null>(null);

  // Filter dropdown state
  const [filterDropdownOpen, setFilterDropdownOpen] = useState(false);
  const [levelOptions, setLevelOptions] = useState<string[]>([]);
  const [loggerOptions, setLoggerOptions] = useState<string[]>([]);

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
    Promise.all([
      fetchTextLogDistinct('level', timeRange),
      fetchTextLogDistinct('logger_name', timeRange),
    ]).then(([levels, loggers]) => {
      setLevelOptions(levels);
      setLoggerOptions(loggers.slice(0, 50)); // Limit to 50
    }).catch(console.error);
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
                <div className="absolute left-0 top-full mt-1 bg-gray-800 border border-gray-600 rounded shadow-lg z-50 min-w-[280px]">
                  <div className="flex items-center justify-between p-2 border-b border-gray-700">
                    <span className="text-xs font-semibold text-gray-300">Filters</span>
                    <div className="flex items-center gap-2">
                      {activeFilterCount > 0 && (
                        <button
                          onClick={clearFilters}
                          className="text-xs text-red-400 hover:text-red-300"
                        >
                          Clear all
                        </button>
                      )}
                      <button onClick={() => setFilterDropdownOpen(false)} className="text-gray-400 hover:text-white">
                        <X className="w-3 h-3" />
                      </button>
                    </div>
                  </div>
                  <div className="max-h-96 overflow-y-auto">
                    {/* Level filter */}
                    <div className="p-2 border-b border-gray-700/50">
                      <div className="text-[10px] text-gray-400 uppercase mb-1">Level</div>
                      <div className="flex flex-wrap gap-1">
                        {levelOptions.map(level => (
                          <button
                            key={level}
                            onClick={() => toggleFilter('level', level)}
                            className={`px-2 py-1 text-xs rounded transition-colors ${
                              filters.level?.includes(level)
                                ? 'bg-blue-600 text-white'
                                : 'bg-gray-700 text-gray-300 hover:bg-gray-600'
                            }`}
                          >
                            {level}
                          </button>
                        ))}
                      </div>
                    </div>

                    {/* Logger filter */}
                    <div className="p-2">
                      <div className="text-[10px] text-gray-400 uppercase mb-1">Logger (top 50)</div>
                      <div className="max-h-40 overflow-y-auto">
                        <div className="flex flex-wrap gap-1">
                          {loggerOptions.map(logger => (
                            <button
                              key={logger}
                              onClick={() => toggleFilter('logger_name', logger)}
                              className={`px-2 py-0.5 text-[10px] rounded transition-colors ${
                                filters.logger_name?.includes(logger)
                                  ? 'bg-blue-600 text-white'
                                  : 'bg-gray-700 text-gray-300 hover:bg-gray-600'
                              }`}
                            >
                              {logger}
                            </button>
                          ))}
                        </div>
                      </div>
                    </div>
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
            className="flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium border-b-2 -mb-px transition-colors border-blue-500 text-blue-400"
          >
            <Activity className="w-3 h-3" />
            Count
          </button>
        </div>
        <div className="h-36 bg-gray-900 border border-gray-700 rounded">
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={timeSeries} margin={{ top: 15, right: 15, left: 5, bottom: 5 }}>
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
              <Legend wrapperStyle={{ fontSize: '10px' }} />
              <Area type="monotone" dataKey="count" name="Total" stroke="#3b82f6" fillOpacity={1} fill="url(#color-total)" />
              <Area type="monotone" dataKey="errors" name="Errors" stroke="#ef4444" fillOpacity={1} fill="url(#color-errors)" />
              <Area type="monotone" dataKey="warnings" name="Warnings" stroke="#eab308" fillOpacity={1} fill="url(#color-warnings)" />
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

      {/* Log entries table */}
      <div className="flex-1 overflow-auto p-4">
        <div className="border border-gray-700 rounded overflow-hidden">
          <table className="w-full text-xs">
            <thead className="bg-gray-800 sticky top-0 z-10">
              <tr>
                <th className="w-36 px-2 py-2 text-left text-gray-400 font-medium border-b border-gray-700">Time</th>
                <th className="w-20 px-2 py-2 text-left text-gray-400 font-medium border-b border-gray-700">Level</th>
                <th className="w-40 px-2 py-2 text-left text-gray-400 font-medium border-b border-gray-700">Logger</th>
                <th className="px-2 py-2 text-left text-gray-400 font-medium border-b border-gray-700">Message</th>
              </tr>
            </thead>
            <tbody>
              {entries.map((entry, idx) => (
                <tr
                  key={`${entry.event_time}-${idx}`}
                  className={`border-b border-gray-800 hover:bg-gray-800/50 cursor-pointer ${
                    idx % 2 === 0 ? 'bg-gray-900/30' : ''
                  } ${selectedEntry === entry ? 'bg-blue-900/30' : ''}`}
                  onClick={() => setSelectedEntry(selectedEntry === entry ? null : entry)}
                >
                  <td className="px-2 py-1.5 text-gray-400 font-mono whitespace-nowrap">
                    {formatDateTime(entry.event_time)}
                  </td>
                  <td className="px-2 py-1.5">
                    <span className={`px-1.5 py-0.5 rounded text-[10px] font-medium ${getLevelColor(entry.level)}`}>
                      {entry.level}
                    </span>
                  </td>
                  <td className="px-2 py-1.5 text-gray-300 font-mono truncate max-w-[160px]" title={entry.logger_name}>
                    {entry.logger_name}
                  </td>
                  <td className="px-2 py-1.5 text-gray-200 truncate max-w-[600px]" title={entry.message}>
                    {entry.message}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
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
