import { Database, BarChart3 } from 'lucide-react';
import { TimeRangeSelector } from '../TimeRangeSelector';
import { SearchBar } from '../SearchBar';
import { TimelineChart } from '../TimelineChart';
import { QueryTable } from '../QueryTable';
import { HistogramsTab } from '../HistogramsTab';
import { ColumnSelector } from '../ColumnSelector';
import { FilterPanel } from '../FilterPanel';
import { useQueryStore } from '../../stores/queryStore';

export function QueriesPage() {
  const { activeTab, setActiveTab, totalCount, fieldFilters } = useQueryStore();

  const activeFilterCount = Object.values(fieldFilters).filter((v) => v.length > 0).length;

  return (
    <div className="h-full flex flex-col">
      {/* Filter bar */}
      <div className="bg-gray-900/50 border-b border-gray-700 px-4 py-2 flex items-center justify-between shrink-0">
        <div className="flex items-center gap-3">
          <FilterPanel />
          <TimeRangeSelector />
          <SearchBar />
        </div>
        <div className="flex items-center gap-4 text-xs">
          <span className="text-gray-400">
            Total: <span className="text-white font-medium">{totalCount.toLocaleString()}</span> queries
          </span>
          {activeFilterCount > 0 && (
            <span className="text-blue-400">
              {activeFilterCount} filter{activeFilterCount > 1 ? 's' : ''} active
            </span>
          )}
        </div>
      </div>

      {/* Timeline Chart */}
      <div className="px-4 pt-4 pb-3 shrink-0">
        <TimelineChart />
      </div>

      {/* Tabs */}
      <div className="border-b border-gray-700 mx-4 flex items-center gap-1 shrink-0">
        <button
          onClick={() => setActiveTab('queries')}
          className={`flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium border-b-2 -mb-px transition-colors ${
            activeTab === 'queries'
              ? 'border-blue-500 text-blue-400'
              : 'border-transparent text-gray-400 hover:text-gray-300'
          }`}
        >
          <Database className="w-3 h-3" />
          Queries
        </button>
        <button
          onClick={() => setActiveTab('histograms')}
          className={`flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium border-b-2 -mb-px transition-colors ${
            activeTab === 'histograms'
              ? 'border-blue-500 text-blue-400'
              : 'border-transparent text-gray-400 hover:text-gray-300'
          }`}
        >
          <BarChart3 className="w-3 h-3" />
          Histograms
        </button>
        {activeTab === 'queries' && (
          <div className="ml-auto">
            <ColumnSelector />
          </div>
        )}
      </div>

      {/* Main Content */}
      <div className="flex-1 overflow-hidden p-4">
        {activeTab === 'queries' && <QueryTable />}
        {activeTab === 'histograms' && <HistogramsTab />}
      </div>
    </div>
  );
}
