import { useState } from 'react';
import { Settings, HardDrive, Database } from 'lucide-react';
import { SystemTable } from '../SystemTable';
import {
  fetchSettings,
  fetchSettingsColumns,
  fetchDisks,
  fetchDisksColumns,
  fetchStoragePolicies,
  fetchStoragePoliciesColumns,
} from '../../services/api';

type InstanceTab = 'settings' | 'disks' | 'storagePolicies';

const DISKS_DEFAULT_VISIBLE_FIELDS = [
  'name',
  'path',
  'free_space',
  'total_space',
  'unreserved_space',
  'keep_free_space',
  'type',
];

const STORAGE_POLICIES_DEFAULT_VISIBLE_FIELDS = [
  'policy_name',
  'volume_name',
  'volume_priority',
  'disks',
  'max_data_part_size',
  'move_factor',
];

export function InstancePage() {
  const [activeTab, setActiveTab] = useState<InstanceTab>('settings');

  const tabs: { id: InstanceTab; label: string; icon: typeof Settings }[] = [
    { id: 'settings', label: 'Settings', icon: Settings },
    { id: 'disks', label: 'Disks', icon: HardDrive },
    { id: 'storagePolicies', label: 'Storage Policies', icon: Database },
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

      <div className="flex-1 overflow-hidden px-4 pt-3 pb-4">
        {activeTab === 'settings' && (
          <SystemTable
            fetchData={fetchSettings}
            fetchColumns={fetchSettingsColumns}
            getRowId={(data) => String(data.name)}
            hideHeader
            columnWidthOverrides={{ name: 400 }}
          />
        )}
        {activeTab === 'disks' && (
          <SystemTable
            fetchData={fetchDisks}
            fetchColumns={fetchDisksColumns}
            defaultVisibleFields={DISKS_DEFAULT_VISIBLE_FIELDS}
            getRowId={(data) => String(data.name)}
            hideHeader
          />
        )}
        {activeTab === 'storagePolicies' && (
          <SystemTable
            fetchData={fetchStoragePolicies}
            fetchColumns={fetchStoragePoliciesColumns}
            defaultVisibleFields={STORAGE_POLICIES_DEFAULT_VISIBLE_FIELDS}
            getRowId={(data) => `${data.policy_name}-${data.volume_name}`}
            hideHeader
          />
        )}
      </div>
    </div>
  );
}
