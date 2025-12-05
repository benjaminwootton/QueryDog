import { useState } from 'react';
import { Users, Settings } from 'lucide-react';
import { SystemTable } from '../SystemTable';
import { fetchUsers, fetchUsersColumns, fetchSettings, fetchSettingsColumns } from '../../services/api';

type InstanceTab = 'users' | 'settings';

export function InstancePage() {
  const [activeTab, setActiveTab] = useState<InstanceTab>('users');

  const tabs: { id: InstanceTab; label: string; icon: typeof Users }[] = [
    { id: 'users', label: 'Users', icon: Users },
    { id: 'settings', label: 'Settings', icon: Settings },
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

      <div className="flex-1 overflow-hidden p-4">
        {activeTab === 'users' && (
          <SystemTable
            fetchData={fetchUsers}
            fetchColumns={fetchUsersColumns}
            getRowId={(data) => String(data.name)}
            hideHeader
          />
        )}
        {activeTab === 'settings' && (
          <SystemTable
            fetchData={fetchSettings}
            fetchColumns={fetchSettingsColumns}
            getRowId={(data) => String(data.name)}
            hideHeader
          />
        )}
      </div>
    </div>
  );
}
