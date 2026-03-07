import { useState } from 'react';
import { Network, RefreshCw, Server, GitBranch, Clock, Database } from 'lucide-react';
import { SystemTable } from '../SystemTable';
import {
  fetchReplicationQueue,
  fetchReplicationQueueColumns,
  fetchReplicationQueueGrouped,
  fetchReplicas,
  fetchReplicasColumns,
  fetchClusters,
  fetchClustersColumns,
  fetchReplicatedFetches,
  fetchReplicatedFetchesColumns,
  fetchDistributedDdlQueue,
  fetchDistributedDdlQueueColumns,
  fetchZookeeper,
  fetchZookeeperColumns,
} from '../../services/api';

type ClusterTab = 'replication-queue' | 'replicas' | 'clusters' | 'fetches' | 'distributed-ddl' | 'zookeeper';
type ReplicationQueueSubTab = 'detailed' | 'grouped';

export function ClusterPage() {
  const [activeTab, setActiveTab] = useState<ClusterTab>('replication-queue');
  const [replicationSubTab, setReplicationSubTab] = useState<ReplicationQueueSubTab>('detailed');

  const tabs: { id: ClusterTab; label: string; icon: typeof Network }[] = [
    { id: 'replication-queue', label: 'Replication Queue', icon: RefreshCw },
    { id: 'replicas', label: 'Replicas', icon: Server },
    { id: 'clusters', label: 'Clusters', icon: Network },
    { id: 'fetches', label: 'Fetches', icon: GitBranch },
    { id: 'distributed-ddl', label: 'Distributed DDL', icon: Clock },
    { id: 'zookeeper', label: 'ZooKeeper', icon: Database },
  ];

  const replicationSubTabs: { id: ReplicationQueueSubTab; label: string }[] = [
    { id: 'detailed', label: 'Detailed' },
    { id: 'grouped', label: 'Grouped by Error' },
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

      <div className="flex-1 overflow-hidden px-4 pt-3 pb-4 flex flex-col">
        {activeTab === 'replication-queue' && (
          <div className="flex flex-col h-full">
            <div className="flex items-center gap-1 mb-2">
              {replicationSubTabs.map(({ id, label }) => (
                <button
                  key={id}
                  onClick={() => setReplicationSubTab(id)}
                  className={`px-2 py-1 text-xs font-medium rounded transition-colors ${
                    replicationSubTab === id
                      ? 'bg-blue-600 text-white'
                      : 'bg-gray-700 text-gray-300 hover:bg-gray-600'
                  }`}
                >
                  {label}
                </button>
              ))}
            </div>
            <div className="flex-1 overflow-hidden">
              {replicationSubTab === 'detailed' && (
                <SystemTable
                  fetchData={fetchReplicationQueue}
                  fetchColumns={fetchReplicationQueueColumns}
                  getRowId={(data) => `${data.database}-${data.table}-${data.replica_name}-${data.position}`}
                  hideHeader
                  defaultVisibleFields={['database', 'table', 'replica_name', 'is_leader', 'is_readonly', 'future_parts', 'parts_to_check', 'queue_size', 'last_queue_update_exception']}
                />
              )}
              {replicationSubTab === 'grouped' && (
                <SystemTable
                  fetchData={fetchReplicationQueueGrouped}
                  getRowId={(data) => `${data.table}-${data.last_exception}-${data.postpone_reason}`}
                  hideHeader
                />
              )}
            </div>
          </div>
        )}

        {activeTab === 'replicas' && (
          <SystemTable
            fetchData={fetchReplicas}
            fetchColumns={fetchReplicasColumns}
            getRowId={(data) => `${data.database}-${data.table}-${data.replica_name}`}
            hideHeader
            defaultVisibleFields={['database', 'table', 'replica_name', 'is_leader', 'is_readonly', 'future_parts', 'parts_to_check', 'queue_size', 'inserts_in_queue', 'merges_in_queue', 'last_queue_update_exception']}
          />
        )}

        {activeTab === 'clusters' && (
          <SystemTable
            fetchData={fetchClusters}
            fetchColumns={fetchClustersColumns}
            getRowId={(data) => `${data.cluster}-${data.shard_num}-${data.replica_num}`}
            hideHeader
          />
        )}

        {activeTab === 'fetches' && (
          <SystemTable
            fetchData={fetchReplicatedFetches}
            fetchColumns={fetchReplicatedFetchesColumns}
            getRowId={(data) => `${data.database}-${data.table}-${data.result_part_name}`}
            hideHeader
          />
        )}

        {activeTab === 'distributed-ddl' && (
          <SystemTable
            fetchData={fetchDistributedDdlQueue}
            fetchColumns={fetchDistributedDdlQueueColumns}
            getRowId={(data) => String(data.entry)}
            hideHeader
          />
        )}

        {activeTab === 'zookeeper' && (
          <SystemTable
            fetchData={fetchZookeeper}
            fetchColumns={fetchZookeeperColumns}
            getRowId={(data) => String(data.path)}
            hideHeader
          />
        )}
      </div>
    </div>
  );
}
