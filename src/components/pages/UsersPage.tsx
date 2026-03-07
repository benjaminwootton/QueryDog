import { useState } from 'react';
import { Users, Shield, Key, Gauge, Lock, Clock } from 'lucide-react';
import { SystemTable } from '../SystemTable';
import {
  fetchUsers,
  fetchUsersColumns,
  fetchGrants,
  fetchGrantsColumns,
  fetchRoles,
  fetchRolesColumns,
  fetchRoleGrants,
  fetchRoleGrantsColumns,
  fetchQuotas,
  fetchQuotasColumns,
  fetchQuotaUsage,
  fetchQuotaUsageColumns,
  fetchQuotaLimits,
  fetchQuotaLimitsColumns,
  fetchRowPolicies,
  fetchRowPoliciesColumns,
  fetchSessionLog,
  fetchSessionLogColumns,
} from '../../services/api';

type UsersTab = 'users' | 'grants' | 'roles' | 'quotas' | 'row-policies' | 'sessions';
type RolesSubTab = 'roles' | 'role-grants';
type QuotasSubTab = 'quotas' | 'quota-usage' | 'quota-limits';

export function UsersPage() {
  const [activeTab, setActiveTab] = useState<UsersTab>('users');
  const [rolesSubTab, setRolesSubTab] = useState<RolesSubTab>('roles');
  const [quotasSubTab, setQuotasSubTab] = useState<QuotasSubTab>('quotas');

  const tabs: { id: UsersTab; label: string; icon: typeof Users }[] = [
    { id: 'users', label: 'Users', icon: Users },
    { id: 'grants', label: 'Grants', icon: Shield },
    { id: 'roles', label: 'Roles', icon: Key },
    { id: 'quotas', label: 'Quotas', icon: Gauge },
    { id: 'row-policies', label: 'Row Policies', icon: Lock },
    { id: 'sessions', label: 'Sessions', icon: Clock },
  ];

  const rolesSubTabs: { id: RolesSubTab; label: string }[] = [
    { id: 'roles', label: 'Roles' },
    { id: 'role-grants', label: 'Role Grants' },
  ];

  const quotasSubTabs: { id: QuotasSubTab; label: string }[] = [
    { id: 'quotas', label: 'Quotas' },
    { id: 'quota-usage', label: 'Usage' },
    { id: 'quota-limits', label: 'Limits' },
  ];

  return (
    <div className="h-full flex flex-col">
      <div className="border-b border-gray-700 px-1.5 flex items-center gap-1 shrink-0">
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

      <div className="flex-1 overflow-hidden px-1.5 pt-3 pb-4 flex flex-col">
        {activeTab === 'users' && (
          <SystemTable
            fetchData={fetchUsers}
            fetchColumns={fetchUsersColumns}
            getRowId={(data) => String(data.name)}
            hideHeader
          />
        )}

        {activeTab === 'grants' && (
          <SystemTable
            fetchData={fetchGrants}
            fetchColumns={fetchGrantsColumns}
            getRowId={(data) => `${data.user_name}-${data.role_name}-${data.access_type}-${data.database}-${data.table}`}
            hideHeader
          />
        )}

        {activeTab === 'roles' && (
          <div className="flex flex-col h-full">
            <div className="flex items-center gap-1 mb-2">
              {rolesSubTabs.map(({ id, label }) => (
                <button
                  key={id}
                  onClick={() => setRolesSubTab(id)}
                  className={`px-2 py-1 text-xs font-medium rounded transition-colors ${
                    rolesSubTab === id
                      ? 'bg-blue-600 text-white'
                      : 'bg-gray-700 text-gray-300 hover:bg-gray-600'
                  }`}
                >
                  {label}
                </button>
              ))}
            </div>
            <div className="flex-1 overflow-hidden">
              {rolesSubTab === 'roles' && (
                <SystemTable
                  fetchData={fetchRoles}
                  fetchColumns={fetchRolesColumns}
                  getRowId={(data) => String(data.name)}
                  hideHeader
                />
              )}
              {rolesSubTab === 'role-grants' && (
                <SystemTable
                  fetchData={fetchRoleGrants}
                  fetchColumns={fetchRoleGrantsColumns}
                  getRowId={(data) => `${data.user_name}-${data.role_name}-${data.granted_role_name}`}
                  hideHeader
                />
              )}
            </div>
          </div>
        )}

        {activeTab === 'quotas' && (
          <div className="flex flex-col h-full">
            <div className="flex items-center gap-1 mb-2">
              {quotasSubTabs.map(({ id, label }) => (
                <button
                  key={id}
                  onClick={() => setQuotasSubTab(id)}
                  className={`px-2 py-1 text-xs font-medium rounded transition-colors ${
                    quotasSubTab === id
                      ? 'bg-blue-600 text-white'
                      : 'bg-gray-700 text-gray-300 hover:bg-gray-600'
                  }`}
                >
                  {label}
                </button>
              ))}
            </div>
            <div className="flex-1 overflow-hidden">
              {quotasSubTab === 'quotas' && (
                <SystemTable
                  fetchData={fetchQuotas}
                  fetchColumns={fetchQuotasColumns}
                  getRowId={(data) => String(data.name)}
                  hideHeader
                />
              )}
              {quotasSubTab === 'quota-usage' && (
                <SystemTable
                  fetchData={fetchQuotaUsage}
                  fetchColumns={fetchQuotaUsageColumns}
                  getRowId={(data) => `${data.quota_name}-${data.quota_key}`}
                  hideHeader
                />
              )}
              {quotasSubTab === 'quota-limits' && (
                <SystemTable
                  fetchData={fetchQuotaLimits}
                  fetchColumns={fetchQuotaLimitsColumns}
                  getRowId={(data) => `${data.quota_name}-${data.duration}`}
                  hideHeader
                />
              )}
            </div>
          </div>
        )}

        {activeTab === 'row-policies' && (
          <SystemTable
            fetchData={fetchRowPolicies}
            fetchColumns={fetchRowPoliciesColumns}
            getRowId={(data) => `${data.short_name}-${data.database}-${data.table_name}`}
            hideHeader
          />
        )}

        {activeTab === 'sessions' && (
          <SystemTable
            fetchData={fetchSessionLog}
            fetchColumns={fetchSessionLogColumns}
            getRowId={(data) => `${data.session_id}-${data.event_time}`}
            hideHeader
          />
        )}
      </div>
    </div>
  );
}
