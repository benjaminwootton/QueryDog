import { useState, useCallback } from 'react';
import { Activity, GitMerge, Zap } from 'lucide-react';
import { SystemTable } from '../SystemTable';
import { ActivityFilterPanel } from '../ActivityFilterPanel';
import {
  fetchProcesses,
  fetchProcessesColumns,
  fetchProcessesDistinct,
  fetchMerges,
  fetchMergesColumns,
  fetchMergesDistinct,
  fetchMutations,
  fetchMutationsColumns,
  fetchMutationsDistinct,
} from '../../services/api';

type ActivityTab = 'processes' | 'merges' | 'mutations';

const MUTATIONS_DEFAULT_VISIBLE_FIELDS = [
  'create_time',
  'database',
  'table',
  'mutation_id',
  'command',
  'parts_to_do',
  'is_done',
  'latest_fail_reason',
];

const PROCESSES_FILTERABLE_FIELDS = [
  { field: 'user', label: 'User' },
  { field: 'query_kind', label: 'Query Kind' },
  { field: 'current_database', label: 'Database' },
  { field: 'client_name', label: 'Client Name' },
  { field: 'is_initial_query', label: 'Initial Query' },
];

const MERGES_FILTERABLE_FIELDS = [
  { field: 'database', label: 'Database' },
  { field: 'table', label: 'Table' },
  { field: 'merge_type', label: 'Merge Type' },
  { field: 'merge_algorithm', label: 'Algorithm' },
];

const MUTATIONS_FILTERABLE_FIELDS = [
  { field: 'database', label: 'Database' },
  { field: 'table', label: 'Table' },
  { field: 'is_done', label: 'Is Done' },
];

export function ActivityPage() {
  const [activeTab, setActiveTab] = useState<ActivityTab>('processes');

  // Processes filters
  const [processesFilters, setProcessesFilters] = useState<Record<string, string[]>>({});
  const handleProcessesFilterChange = useCallback((field: string, values: string[]) => {
    setProcessesFilters((prev) => ({ ...prev, [field]: values }));
  }, []);
  const handleClearProcessesFilter = useCallback((field: string) => {
    setProcessesFilters((prev) => {
      const { [field]: _, ...rest } = prev;
      return rest;
    });
  }, []);
  const handleClearAllProcessesFilters = useCallback(() => {
    setProcessesFilters({});
  }, []);
  const fetchProcessesWithFilters = useCallback(
    () => fetchProcesses(processesFilters),
    [processesFilters]
  );

  // Merges filters
  const [mergesFilters, setMergesFilters] = useState<Record<string, string[]>>({});
  const handleMergesFilterChange = useCallback((field: string, values: string[]) => {
    setMergesFilters((prev) => ({ ...prev, [field]: values }));
  }, []);
  const handleClearMergesFilter = useCallback((field: string) => {
    setMergesFilters((prev) => {
      const { [field]: _, ...rest } = prev;
      return rest;
    });
  }, []);
  const handleClearAllMergesFilters = useCallback(() => {
    setMergesFilters({});
  }, []);
  const fetchMergesWithFilters = useCallback(
    () => fetchMerges(mergesFilters),
    [mergesFilters]
  );

  // Mutations filters
  const [mutationsFilters, setMutationsFilters] = useState<Record<string, string[]>>({});
  const handleMutationsFilterChange = useCallback((field: string, values: string[]) => {
    setMutationsFilters((prev) => ({ ...prev, [field]: values }));
  }, []);
  const handleClearMutationsFilter = useCallback((field: string) => {
    setMutationsFilters((prev) => {
      const { [field]: _, ...rest } = prev;
      return rest;
    });
  }, []);
  const handleClearAllMutationsFilters = useCallback(() => {
    setMutationsFilters({});
  }, []);
  const fetchMutationsWithFilters = useCallback(
    () => fetchMutations(mutationsFilters),
    [mutationsFilters]
  );

  const processesFilterCount = Object.values(processesFilters).filter((v) => v.length > 0).length;
  const mergesFilterCount = Object.values(mergesFilters).filter((v) => v.length > 0).length;
  const mutationsFilterCount = Object.values(mutationsFilters).filter((v) => v.length > 0).length;

  const tabs: { id: ActivityTab; label: string; icon: typeof Activity }[] = [
    { id: 'processes', label: 'Processes', icon: Activity },
    { id: 'merges', label: 'Merges', icon: GitMerge },
    { id: 'mutations', label: 'Mutations', icon: Zap },
  ];

  return (
    <div className="h-full flex flex-col">
      {/* Filter bar */}
      <div className="bg-gray-900/50 border-b border-gray-700 px-4 py-2 flex items-center justify-between shrink-0">
        <div className="flex items-center gap-3">
          {activeTab === 'processes' && (
            <ActivityFilterPanel
              filters={processesFilters}
              onFilterChange={handleProcessesFilterChange}
              onClearFilter={handleClearProcessesFilter}
              onClearAll={handleClearAllProcessesFilters}
              fetchDistinct={fetchProcessesDistinct}
              filterableFields={PROCESSES_FILTERABLE_FIELDS}
            />
          )}
          {activeTab === 'merges' && (
            <ActivityFilterPanel
              filters={mergesFilters}
              onFilterChange={handleMergesFilterChange}
              onClearFilter={handleClearMergesFilter}
              onClearAll={handleClearAllMergesFilters}
              fetchDistinct={fetchMergesDistinct}
              filterableFields={MERGES_FILTERABLE_FIELDS}
            />
          )}
          {activeTab === 'mutations' && (
            <ActivityFilterPanel
              filters={mutationsFilters}
              onFilterChange={handleMutationsFilterChange}
              onClearFilter={handleClearMutationsFilter}
              onClearAll={handleClearAllMutationsFilters}
              fetchDistinct={fetchMutationsDistinct}
              filterableFields={MUTATIONS_FILTERABLE_FIELDS}
            />
          )}
        </div>
        <div className="flex items-center gap-4 text-xs">
          {activeTab === 'processes' && processesFilterCount > 0 && (
            <span className="text-blue-400">
              {processesFilterCount} filter{processesFilterCount > 1 ? 's' : ''} active
            </span>
          )}
          {activeTab === 'merges' && mergesFilterCount > 0 && (
            <span className="text-blue-400">
              {mergesFilterCount} filter{mergesFilterCount > 1 ? 's' : ''} active
            </span>
          )}
          {activeTab === 'mutations' && mutationsFilterCount > 0 && (
            <span className="text-blue-400">
              {mutationsFilterCount} filter{mutationsFilterCount > 1 ? 's' : ''} active
            </span>
          )}
        </div>
      </div>

      {/* Tabs */}
      <div className="border-b border-gray-700 mx-4 flex items-center gap-1 shrink-0">
        {tabs.map(({ id, label, icon: Icon }) => (
          <button
            key={id}
            onClick={() => setActiveTab(id)}
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

      {/* Content */}
      <div className="flex-1 overflow-hidden p-4">
        {activeTab === 'processes' && (
          <SystemTable
            fetchData={fetchProcessesWithFilters}
            fetchColumns={fetchProcessesColumns}
            filters={processesFilters}
            getRowId={(data) => String(data.query_id)}
            hideTitle
          />
        )}
        {activeTab === 'merges' && (
          <SystemTable
            fetchData={fetchMergesWithFilters}
            fetchColumns={fetchMergesColumns}
            filters={mergesFilters}
            getRowId={(data) => `${data.database}-${data.table}-${data.result_part_name}`}
            hideTitle
          />
        )}
        {activeTab === 'mutations' && (
          <SystemTable
            fetchData={fetchMutationsWithFilters}
            fetchColumns={fetchMutationsColumns}
            defaultVisibleFields={MUTATIONS_DEFAULT_VISIBLE_FIELDS}
            filters={mutationsFilters}
            getRowId={(data) => `${data.database}-${data.table}-${data.mutation_id}`}
            hideTitle
          />
        )}
      </div>
    </div>
  );
}
