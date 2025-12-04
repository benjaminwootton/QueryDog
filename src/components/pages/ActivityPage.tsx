import { useState } from 'react';
import { Activity, GitMerge, Zap } from 'lucide-react';
import { SystemTable } from '../SystemTable';
import { fetchProcesses, fetchProcessesColumns, fetchMerges, fetchMergesColumns, fetchMutations, fetchMutationsColumns } from '../../services/api';

type ActivityTab = 'processes' | 'merges' | 'mutations';

export function ActivityPage() {
  const [activeTab, setActiveTab] = useState<ActivityTab>('processes');

  const tabs: { id: ActivityTab; label: string; icon: typeof Activity }[] = [
    { id: 'processes', label: 'Processes', icon: Activity },
    { id: 'merges', label: 'Merges', icon: GitMerge },
    { id: 'mutations', label: 'Mutations', icon: Zap },
  ];

  return (
    <div className="h-full flex flex-col">
      <div className="border-b border-gray-700 px-4 flex items-center gap-1 shrink-0">
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

      <div className="flex-1 overflow-hidden">
        {activeTab === 'processes' && (
          <SystemTable
            title="system.processes"
            fetchData={fetchProcesses}
            fetchColumns={fetchProcessesColumns}
            getRowId={(data) => String(data.query_id)}
            autoRefresh={true}
            refreshInterval={30000}
          />
        )}
        {activeTab === 'merges' && (
          <SystemTable
            title="system.merges"
            fetchData={fetchMerges}
            fetchColumns={fetchMergesColumns}
            getRowId={(data) => `${data.database}-${data.table}-${data.result_part_name}`}
            autoRefresh={true}
            refreshInterval={30000}
          />
        )}
        {activeTab === 'mutations' && (
          <SystemTable
            title="system.mutations"
            fetchData={fetchMutations}
            fetchColumns={fetchMutationsColumns}
            getRowId={(data) => `${data.database}-${data.table}-${data.mutation_id}`}
            autoRefresh={true}
            refreshInterval={30000}
          />
        )}
      </div>
    </div>
  );
}
