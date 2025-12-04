import { useState } from 'react';
import { HardDrive, FileText } from 'lucide-react';
import { SystemTable } from '../SystemTable';
import { PartLogTable } from '../PartLogTable';
import { PartLogColumnSelector } from '../PartLogColumnSelector';
import { fetchParts, fetchPartsColumns } from '../../services/api';

type PartsTab = 'parts' | 'partlog';

export function PartsPage() {
  const [activeTab, setActiveTab] = useState<PartsTab>('parts');

  const tabs: { id: PartsTab; label: string; icon: typeof HardDrive }[] = [
    { id: 'parts', label: 'Parts', icon: HardDrive },
    { id: 'partlog', label: 'Part Log', icon: FileText },
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
        {activeTab === 'partlog' && (
          <div className="ml-auto">
            <PartLogColumnSelector />
          </div>
        )}
      </div>

      <div className="flex-1 overflow-hidden">
        {activeTab === 'parts' && (
          <SystemTable
            title="system.parts"
            fetchData={fetchParts}
            fetchColumns={fetchPartsColumns}
            getRowId={(data) => `${data.database}-${data.table}-${data.name}`}
          />
        )}
        {activeTab === 'partlog' && (
          <div className="h-full p-4">
            <PartLogTable />
          </div>
        )}
      </div>
    </div>
  );
}
