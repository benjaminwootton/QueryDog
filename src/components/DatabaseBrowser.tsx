import { useState, useEffect } from 'react';
import {
  ReactFlow,
  type Node,
  type Edge,
  Controls,
  Background,
  useNodesState,
  useEdgesState,
  Handle,
  Position,
  BackgroundVariant,
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import { X, Database, Table, Layers, Box, ChevronRight, Loader2, Users, User, Columns, FileText, Sparkles, Zap } from 'lucide-react';
import {
  fetchBrowserDatabases,
  fetchBrowserTables,
  fetchBrowserPartitions,
  fetchBrowserParts,
  fetchBrowserColumns,
  fetchBrowserProjections,
  fetchBrowserProjectionParts,
  fetchBrowserIndexes,
  fetchUsers,
  type BrowserDatabase,
  type BrowserTable,
  type BrowserPartition,
  type BrowserPart,
  type BrowserColumn,
  type BrowserProjection,
  type BrowserProjectionPart,
  type BrowserIndex,
} from '../services/api';

interface BrowserUser {
  name: string;
  id: string;
  storage: string;
  auth_type: string;
  auth_params: string;
  host_ip: string[];
  host_names: string[];
  host_names_regexp: string[];
  host_names_like: string[];
  default_roles_all: number;
  default_roles_list: string[];
  default_roles_except: string[];
  grantees_any: number;
  grantees_list: string[];
  grantees_except: string[];
  default_database: string;
}

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
}

function formatNumber(num: number): string {
  if (num >= 1000000) return (num / 1000000).toFixed(1) + 'M';
  if (num >= 1000) return (num / 1000).toFixed(1) + 'K';
  return num.toLocaleString();
}

// Fixed width for all nodes
const nodeWidth = 'w-[140px]';

// Root category node (Databases, Users)
function RootNode({ data }: { data: { label: string; icon: 'databases' | 'users'; selected: boolean; onClick: () => void } }) {
  const Icon = data.icon === 'databases' ? Database : Users;
  const isDatabases = data.icon === 'databases';

  return (
    <div
      onClick={data.onClick}
      className={`${nodeWidth} px-2 py-1.5 rounded border cursor-pointer transition-all ${
        data.selected
          ? isDatabases
            ? 'bg-blue-600 border-blue-400 text-white'
            : 'bg-amber-600 border-amber-400 text-white'
          : isDatabases
            ? 'bg-gray-800 border-gray-600 text-gray-200 hover:border-blue-500'
            : 'bg-gray-800 border-gray-600 text-gray-200 hover:border-amber-500'
      }`}
    >
      <Handle type="source" position={Position.Right} className={isDatabases ? '!bg-blue-500' : '!bg-amber-500'} />
      <div className="flex items-center gap-1.5">
        <Icon className={`w-3 h-3 shrink-0 ${isDatabases ? 'text-blue-400' : 'text-amber-400'}`} />
        <span className="text-[9px] font-semibold uppercase tracking-wide">{data.label}</span>
      </div>
    </div>
  );
}

// User node
function UserNode({ data }: { data: { label: string; authType: string; storage: string } }) {
  const tooltip = `Auth: ${data.authType} | Storage: ${data.storage}`;
  return (
    <div
      title={tooltip}
      className={`${nodeWidth} px-2 py-1 rounded border bg-gray-800 border-gray-600 text-gray-200`}
    >
      <Handle type="target" position={Position.Left} className="!bg-amber-500" />
      <div className="flex items-center gap-1">
        <User className="w-2.5 h-2.5 text-amber-400 shrink-0" />
        <span className="text-[8px] font-medium uppercase tracking-wide truncate">{data.label}</span>
      </div>
    </div>
  );
}

// Custom node components
function DatabaseNode({ data }: { data: { label: string; engine: string; selected: boolean; onClick: () => void } }) {
  return (
    <div
      onClick={data.onClick}
      title={data.engine}
      className={`${nodeWidth} px-2 py-1 rounded border cursor-pointer transition-all ${
        data.selected
          ? 'bg-blue-600 border-blue-400 text-white'
          : 'bg-gray-800 border-gray-600 text-gray-200 hover:border-blue-500'
      }`}
    >
      <Handle type="target" position={Position.Left} className="!bg-blue-500" />
      <Handle type="source" position={Position.Right} className="!bg-blue-500" />
      <div className="flex items-center gap-1">
        <Database className="w-2.5 h-2.5 text-blue-400 shrink-0" />
        <span className="text-[8px] font-medium uppercase tracking-wide truncate">{data.label}</span>
      </div>
    </div>
  );
}

function TableNode({ data }: { data: { label: string; engine: string; rows: number; bytes: number; selected: boolean; onClick: () => void } }) {
  const tooltip = `${data.engine} | ${formatNumber(data.rows)} rows | ${formatBytes(data.bytes)}`;
  return (
    <div
      onClick={data.onClick}
      title={tooltip}
      className={`${nodeWidth} px-2 py-1 rounded border cursor-pointer transition-all ${
        data.selected
          ? 'bg-green-600 border-green-400 text-white'
          : 'bg-gray-800 border-gray-600 text-gray-200 hover:border-green-500'
      }`}
    >
      <Handle type="target" position={Position.Left} className="!bg-blue-500" />
      <Handle type="source" position={Position.Right} className="!bg-green-500" />
      <div className="flex items-center gap-1">
        <Table className="w-2.5 h-2.5 text-green-400 shrink-0" />
        <span className="text-[8px] font-medium uppercase tracking-wide truncate">{data.label}</span>
      </div>
    </div>
  );
}

function PartitionNode({ data }: { data: { label: string; partCount: number; rows: number; bytes: number; selected: boolean; onClick: () => void } }) {
  const tooltip = `${data.partCount} parts | ${formatNumber(data.rows)} rows`;
  return (
    <div
      onClick={data.onClick}
      title={tooltip}
      className={`${nodeWidth} px-2 py-1 rounded border cursor-pointer transition-all ${
        data.selected
          ? 'bg-purple-600 border-purple-400 text-white'
          : 'bg-gray-800 border-gray-600 text-gray-200 hover:border-purple-500'
      }`}
    >
      <Handle type="target" position={Position.Left} className="!bg-green-500" />
      <Handle type="source" position={Position.Right} className="!bg-purple-500" />
      <div className="flex items-center gap-1">
        <Layers className="w-2.5 h-2.5 text-purple-400 shrink-0" />
        <span className="text-[8px] font-medium uppercase tracking-wide truncate">{data.label}</span>
      </div>
    </div>
  );
}

function PartNode({ data }: { data: { label: string; rows: number; bytes: number; level: number } }) {
  const tooltip = `L${data.level} | ${formatNumber(data.rows)} rows | ${formatBytes(data.bytes)}`;
  return (
    <div
      title={tooltip}
      className={`${nodeWidth} px-2 py-1 rounded border bg-gray-800 border-gray-600 text-gray-200`}
    >
      <Handle type="target" position={Position.Left} className="!bg-purple-500" />
      <div className="flex items-center gap-1">
        <Box className="w-2.5 h-2.5 text-orange-400 shrink-0" />
        <span className="text-[8px] font-medium uppercase tracking-wide truncate">{data.label}</span>
      </div>
    </div>
  );
}

// Category node (Partitions, Columns, Projections, Indexes under a table)
function CategoryNode({ data }: { data: { label: string; category: 'partitions' | 'columns' | 'projections' | 'indexes'; count: number; selected: boolean; onClick: () => void } }) {
  const Icon = data.category === 'partitions' ? Layers : data.category === 'projections' ? Sparkles : data.category === 'indexes' ? Zap : Columns;
  const colorClass = data.category === 'partitions' ? 'purple' : data.category === 'projections' ? 'pink' : data.category === 'indexes' ? 'amber' : 'cyan';
  return (
    <div
      onClick={data.onClick}
      title={`${data.count} ${data.label.toLowerCase()}`}
      className={`${nodeWidth} px-2 py-1 rounded border cursor-pointer transition-all ${
        data.selected
          ? `bg-${colorClass}-600 border-${colorClass}-400 text-white`
          : `bg-gray-800 border-gray-600 text-gray-200 hover:border-${colorClass}-500`
      }`}
      style={{
        backgroundColor: data.selected ? (colorClass === 'purple' ? '#9333ea' : colorClass === 'pink' ? '#db2777' : colorClass === 'amber' ? '#d97706' : '#0891b2') : undefined,
        borderColor: data.selected ? (colorClass === 'purple' ? '#a855f7' : colorClass === 'pink' ? '#ec4899' : colorClass === 'amber' ? '#f59e0b' : '#22d3ee') : undefined,
      }}
    >
      <Handle type="target" position={Position.Left} className="!bg-green-500" />
      <Handle type="source" position={Position.Right} style={{ backgroundColor: colorClass === 'purple' ? '#a855f7' : colorClass === 'pink' ? '#ec4899' : colorClass === 'amber' ? '#f59e0b' : '#22d3ee' }} />
      <div className="flex items-center gap-1">
        <Icon className={`w-2.5 h-2.5 shrink-0`} style={{ color: colorClass === 'purple' ? '#c084fc' : colorClass === 'pink' ? '#f472b6' : colorClass === 'amber' ? '#fbbf24' : '#67e8f9' }} />
        <span className="text-[8px] font-medium uppercase tracking-wide">{data.label}</span>
        <span className="text-[8px] text-gray-500">({data.count})</span>
      </div>
    </div>
  );
}

// Projection node
function ProjectionNode({ data }: { data: { label: string; type: string; sortingKey: string; selected: boolean; onClick: () => void } }) {
  const tooltip = `Type: ${data.type} | Sort: ${data.sortingKey || 'none'}`;
  return (
    <div
      onClick={data.onClick}
      title={tooltip}
      className={`${nodeWidth} px-2 py-1 rounded border cursor-pointer transition-all ${
        data.selected
          ? 'bg-pink-600 border-pink-400 text-white'
          : 'bg-gray-800 border-gray-600 text-gray-200 hover:border-pink-500'
      }`}
    >
      <Handle type="target" position={Position.Left} className="!bg-pink-500" />
      <Handle type="source" position={Position.Right} className="!bg-pink-500" />
      <div className="flex items-center gap-1">
        <Sparkles className="w-2.5 h-2.5 text-pink-400 shrink-0" />
        <span className="text-[8px] font-medium uppercase tracking-wide truncate">{data.label}</span>
      </div>
    </div>
  );
}

// Projection Part node
function ProjectionPartNode({ data }: { data: { label: string; rows: number; bytes: number; parentPart: string } }) {
  const tooltip = `${formatNumber(data.rows)} rows | ${formatBytes(data.bytes)} | Parent: ${data.parentPart}`;
  return (
    <div
      title={tooltip}
      className={`${nodeWidth} px-2 py-1 rounded border bg-gray-800 border-gray-600 text-gray-200`}
    >
      <Handle type="target" position={Position.Left} className="!bg-pink-500" />
      <div className="flex items-center gap-1">
        <Box className="w-2.5 h-2.5 text-pink-400 shrink-0" />
        <span className="text-[8px] font-medium uppercase tracking-wide truncate">{data.label}</span>
      </div>
    </div>
  );
}

// Index node
function IndexNode({ data }: { data: { label: string; type: string; expr: string; granularity: number } }) {
  const tooltip = `Type: ${data.type} | Expr: ${data.expr} | Granularity: ${data.granularity}`;
  return (
    <div
      title={tooltip}
      className={`${nodeWidth} px-2 py-1 rounded border bg-gray-800 border-gray-600 text-gray-200`}
    >
      <Handle type="target" position={Position.Left} className="!bg-amber-500" />
      <div className="flex items-center gap-1">
        <Zap className="w-2.5 h-2.5 text-amber-400 shrink-0" />
        <span className="text-[8px] font-medium uppercase tracking-wide truncate">{data.label}</span>
      </div>
    </div>
  );
}

// Column node
function ColumnNode({ data }: { data: { label: string; type: string; isPrimaryKey: boolean; isPartitionKey: boolean; isSortingKey: boolean } }) {
  const badges: string[] = [];
  if (data.isPrimaryKey) badges.push('PK');
  if (data.isPartitionKey) badges.push('PART');
  if (data.isSortingKey) badges.push('SORT');
  const tooltip = `${data.type}${badges.length > 0 ? ' | ' + badges.join(', ') : ''}`;
  return (
    <div
      title={tooltip}
      className={`${nodeWidth} px-2 py-1 rounded border bg-gray-800 border-gray-600 text-gray-200`}
    >
      <Handle type="target" position={Position.Left} className="!bg-cyan-500" />
      <div className="flex items-center gap-1">
        <FileText className={`w-2.5 h-2.5 shrink-0 ${data.isPrimaryKey ? 'text-yellow-400' : 'text-cyan-400'}`} />
        <span className="text-[8px] font-medium tracking-wide truncate">{data.label}</span>
        {badges.length > 0 && (
          <span className="text-[7px] text-yellow-400 ml-auto">{badges[0]}</span>
        )}
      </div>
    </div>
  );
}

const nodeTypes = {
  root: RootNode,
  database: DatabaseNode,
  table: TableNode,
  category: CategoryNode,
  partition: PartitionNode,
  part: PartNode,
  column: ColumnNode,
  user: UserNode,
  projection: ProjectionNode,
  projectionPart: ProjectionPartNode,
  index: IndexNode,
};

interface DatabaseBrowserProps {
  onClose: () => void;
}

export function DatabaseBrowser({ onClose }: DatabaseBrowserProps) {
  const [databases, setDatabases] = useState<BrowserDatabase[]>([]);
  const [users, setUsers] = useState<BrowserUser[]>([]);
  const [tables, setTables] = useState<BrowserTable[]>([]);
  const [partitions, setPartitions] = useState<BrowserPartition[]>([]);
  const [parts, setParts] = useState<BrowserPart[]>([]);
  const [columns, setColumns] = useState<BrowserColumn[]>([]);
  const [projections, setProjections] = useState<BrowserProjection[]>([]);
  const [projectionParts, setProjectionParts] = useState<BrowserProjectionPart[]>([]);
  const [indexes, setIndexes] = useState<BrowserIndex[]>([]);

  const [selectedRoot, setSelectedRoot] = useState<'databases' | 'users' | null>(null);
  const [selectedDatabase, setSelectedDatabase] = useState<string | null>(null);
  const [selectedTable, setSelectedTable] = useState<string | null>(null);
  const [selectedTableCategory, setSelectedTableCategory] = useState<'partitions' | 'columns' | 'projections' | 'indexes' | null>(null);
  const [selectedPartition, setSelectedPartition] = useState<string | null>(null);
  const [selectedProjection, setSelectedProjection] = useState<string | null>(null);

  const [loading, setLoading] = useState(false);
  const [loadingTables, setLoadingTables] = useState(false);
  const [loadingPartitions, setLoadingPartitions] = useState(false);
  const [loadingParts, setLoadingParts] = useState(false);
  const [loadingColumns, setLoadingColumns] = useState(false);
  const [loadingProjections, setLoadingProjections] = useState(false);
  const [loadingProjectionParts, setLoadingProjectionParts] = useState(false);
  const [loadingIndexes, setLoadingIndexes] = useState(false);

  const [nodes, setNodes, onNodesChange] = useNodesState<Node>([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([]);

  // Load databases when Databases root is selected
  useEffect(() => {
    if (selectedRoot !== 'databases') {
      setDatabases([]);
      setSelectedDatabase(null);
      return;
    }
    setLoading(true);
    fetchBrowserDatabases()
      .then((dbs) => {
        // Deduplicate case-insensitive (e.g., INFORMATION_SCHEMA vs information_schema)
        const seen = new Set<string>();
        return dbs.filter((db) => {
          const lower = db.name.toLowerCase();
          if (seen.has(lower)) return false;
          seen.add(lower);
          return true;
        });
      })
      .then(setDatabases)
      .catch(console.error)
      .finally(() => setLoading(false));
  }, [selectedRoot]);

  // Load users when Users root is selected
  useEffect(() => {
    if (selectedRoot !== 'users') {
      setUsers([]);
      return;
    }
    setLoading(true);
    fetchUsers()
      .then((data) => setUsers(data as unknown as BrowserUser[]))
      .catch(console.error)
      .finally(() => setLoading(false));
  }, [selectedRoot]);

  // Load tables when database selected
  useEffect(() => {
    if (!selectedDatabase) {
      setTables([]);
      setSelectedTable(null);
      return;
    }
    setLoadingTables(true);
    setSelectedTable(null);
    setSelectedTableCategory(null);
    setPartitions([]);
    setColumns([]);
    setParts([]);
    fetchBrowserTables(selectedDatabase)
      .then(setTables)
      .catch(console.error)
      .finally(() => setLoadingTables(false));
  }, [selectedDatabase]);

  // Load partitions, columns, projections, and indexes when table selected
  useEffect(() => {
    if (!selectedDatabase || !selectedTable) {
      setPartitions([]);
      setColumns([]);
      setProjections([]);
      setIndexes([]);
      setSelectedTableCategory(null);
      setSelectedPartition(null);
      setSelectedProjection(null);
      setParts([]);
      setProjectionParts([]);
      return;
    }
    // Load all four when table is selected
    setLoadingPartitions(true);
    setLoadingColumns(true);
    setLoadingProjections(true);
    setLoadingIndexes(true);
    setSelectedTableCategory(null);
    setSelectedPartition(null);
    setSelectedProjection(null);
    setParts([]);
    setProjectionParts([]);

    Promise.all([
      fetchBrowserPartitions(selectedDatabase, selectedTable),
      fetchBrowserColumns(selectedDatabase, selectedTable),
      fetchBrowserProjections(selectedDatabase, selectedTable),
      fetchBrowserIndexes(selectedDatabase, selectedTable),
    ])
      .then(([partData, colData, projData, idxData]) => {
        setPartitions(partData);
        setColumns(colData);
        setProjections(projData);
        setIndexes(idxData);
      })
      .catch(console.error)
      .finally(() => {
        setLoadingPartitions(false);
        setLoadingColumns(false);
        setLoadingProjections(false);
        setLoadingIndexes(false);
      });
  }, [selectedDatabase, selectedTable]);

  // Load parts when partition selected
  useEffect(() => {
    if (!selectedDatabase || !selectedTable || !selectedPartition) {
      setParts([]);
      return;
    }
    setLoadingParts(true);
    fetchBrowserParts(selectedDatabase, selectedTable, selectedPartition)
      .then(setParts)
      .catch(console.error)
      .finally(() => setLoadingParts(false));
  }, [selectedDatabase, selectedTable, selectedPartition]);

  // Load projection parts when projection selected
  useEffect(() => {
    if (!selectedDatabase || !selectedTable || !selectedProjection) {
      setProjectionParts([]);
      return;
    }
    setLoadingProjectionParts(true);
    fetchBrowserProjectionParts(selectedDatabase, selectedTable, selectedProjection)
      .then(setProjectionParts)
      .catch(console.error)
      .finally(() => setLoadingProjectionParts(false));
  }, [selectedDatabase, selectedTable, selectedProjection]);

  // Build nodes and edges
  useEffect(() => {
    const newNodes: Node[] = [];
    const newEdges: Edge[] = [];

    const columnX = [50, 220, 390, 560, 730, 900];
    const nodeSpacing = 36;

    // Root nodes (always visible)
    newNodes.push({
      id: 'root-databases',
      type: 'root',
      position: { x: columnX[0], y: 50 },
      data: {
        label: 'Databases',
        icon: 'databases',
        selected: selectedRoot === 'databases',
        onClick: () => setSelectedRoot(selectedRoot === 'databases' ? null : 'databases'),
      },
    });

    newNodes.push({
      id: 'root-users',
      type: 'root',
      position: { x: columnX[0], y: 50 + nodeSpacing },
      data: {
        label: 'Users',
        icon: 'users',
        selected: selectedRoot === 'users',
        onClick: () => setSelectedRoot(selectedRoot === 'users' ? null : 'users'),
      },
    });

    // Database nodes (when Databases root is selected)
    if (selectedRoot === 'databases' && databases.length > 0) {
      databases.forEach((db, i) => {
        newNodes.push({
          id: `db-${db.name}`,
          type: 'database',
          position: { x: columnX[1], y: 50 + i * nodeSpacing },
          data: {
            label: db.name,
            engine: db.engine,
            selected: selectedDatabase === db.name,
            onClick: () => setSelectedDatabase(db.name === selectedDatabase ? null : db.name),
          },
        });
        newEdges.push({
          id: `edge-root-db-${db.name}`,
          source: 'root-databases',
          target: `db-${db.name}`,
          animated: false,
          style: { stroke: '#4b5563' },
        });
      });
    }

    // User nodes (when Users root is selected)
    if (selectedRoot === 'users' && users.length > 0) {
      users.forEach((user, i) => {
        newNodes.push({
          id: `user-${user.name}`,
          type: 'user',
          position: { x: columnX[1], y: 50 + i * nodeSpacing },
          data: {
            label: user.name,
            authType: user.auth_type || 'N/A',
            storage: user.storage || 'N/A',
          },
        });
        newEdges.push({
          id: `edge-root-user-${user.name}`,
          source: 'root-users',
          target: `user-${user.name}`,
          animated: false,
          style: { stroke: '#4b5563' },
        });
      });
    }

    // Table nodes
    if (selectedDatabase && tables.length > 0) {
      tables.forEach((tbl, i) => {
        const nodeId = `tbl-${tbl.name}`;
        newNodes.push({
          id: nodeId,
          type: 'table',
          position: { x: columnX[2], y: 50 + i * nodeSpacing },
          data: {
            label: tbl.name,
            engine: tbl.engine,
            rows: tbl.total_rows || 0,
            bytes: tbl.total_bytes || 0,
            selected: selectedTable === tbl.name,
            onClick: () => setSelectedTable(tbl.name === selectedTable ? null : tbl.name),
          },
        });
        newEdges.push({
          id: `edge-db-${selectedDatabase}-${tbl.name}`,
          source: `db-${selectedDatabase}`,
          target: nodeId,
          animated: false,
          style: { stroke: '#4b5563' },
        });
      });
    }

    // Category nodes (Partitions and Columns) when table is selected
    if (selectedTable) {
      // Partitions category
      newNodes.push({
        id: 'cat-partitions',
        type: 'category',
        position: { x: columnX[3], y: 50 },
        data: {
          label: 'Partitions',
          category: 'partitions',
          count: partitions.length,
          selected: selectedTableCategory === 'partitions',
          onClick: () => {
            setSelectedTableCategory(selectedTableCategory === 'partitions' ? null : 'partitions');
            setSelectedPartition(null);
            setSelectedProjection(null);
          },
        },
      });
      newEdges.push({
        id: `edge-tbl-${selectedTable}-partitions`,
        source: `tbl-${selectedTable}`,
        target: 'cat-partitions',
        animated: false,
        style: { stroke: '#4b5563' },
      });

      // Columns category
      newNodes.push({
        id: 'cat-columns',
        type: 'category',
        position: { x: columnX[3], y: 50 + nodeSpacing },
        data: {
          label: 'Columns',
          category: 'columns',
          count: columns.length,
          selected: selectedTableCategory === 'columns',
          onClick: () => {
            setSelectedTableCategory(selectedTableCategory === 'columns' ? null : 'columns');
            setSelectedPartition(null);
            setSelectedProjection(null);
          },
        },
      });
      newEdges.push({
        id: `edge-tbl-${selectedTable}-columns`,
        source: `tbl-${selectedTable}`,
        target: 'cat-columns',
        animated: false,
        style: { stroke: '#4b5563' },
      });

      // Projections category (only show if there are projections)
      if (projections.length > 0) {
        newNodes.push({
          id: 'cat-projections',
          type: 'category',
          position: { x: columnX[3], y: 50 + nodeSpacing * 2 },
          data: {
            label: 'Projections',
            category: 'projections',
            count: projections.length,
            selected: selectedTableCategory === 'projections',
            onClick: () => {
              setSelectedTableCategory(selectedTableCategory === 'projections' ? null : 'projections');
              setSelectedPartition(null);
              setSelectedProjection(null);
            },
          },
        });
        newEdges.push({
          id: `edge-tbl-${selectedTable}-projections`,
          source: `tbl-${selectedTable}`,
          target: 'cat-projections',
          animated: false,
          style: { stroke: '#4b5563' },
        });
      }

      // Indexes category (only show if there are indexes)
      if (indexes.length > 0) {
        // Calculate position based on whether projections exist
        const indexesY = 50 + nodeSpacing * (2 + (projections.length > 0 ? 1 : 0));
        newNodes.push({
          id: 'cat-indexes',
          type: 'category',
          position: { x: columnX[3], y: indexesY },
          data: {
            label: 'Indexes',
            category: 'indexes',
            count: indexes.length,
            selected: selectedTableCategory === 'indexes',
            onClick: () => {
              setSelectedTableCategory(selectedTableCategory === 'indexes' ? null : 'indexes');
              setSelectedPartition(null);
              setSelectedProjection(null);
            },
          },
        });
        newEdges.push({
          id: `edge-tbl-${selectedTable}-indexes`,
          source: `tbl-${selectedTable}`,
          target: 'cat-indexes',
          animated: false,
          style: { stroke: '#4b5563' },
        });
      }
    }

    // Partition nodes (when Partitions category is selected)
    if (selectedTableCategory === 'partitions' && partitions.length > 0) {
      partitions.forEach((part, i) => {
        const nodeId = `part-${part.partition_id}`;
        newNodes.push({
          id: nodeId,
          type: 'partition',
          position: { x: columnX[4], y: 50 + i * nodeSpacing },
          data: {
            label: part.partition_id || 'all',
            partCount: part.part_count,
            rows: part.total_rows || 0,
            bytes: part.total_bytes || 0,
            selected: selectedPartition === part.partition_id,
            onClick: () => setSelectedPartition(part.partition_id === selectedPartition ? null : part.partition_id),
          },
        });
        newEdges.push({
          id: `edge-cat-partitions-${part.partition_id}`,
          source: 'cat-partitions',
          target: nodeId,
          animated: false,
          style: { stroke: '#4b5563' },
        });
      });
    }

    // Column nodes (when Columns category is selected)
    if (selectedTableCategory === 'columns' && columns.length > 0) {
      columns.forEach((col, i) => {
        const nodeId = `col-${col.name}`;
        newNodes.push({
          id: nodeId,
          type: 'column',
          position: { x: columnX[4], y: 50 + i * nodeSpacing },
          data: {
            label: col.name,
            type: col.type,
            isPrimaryKey: col.is_in_primary_key === 1,
            isPartitionKey: col.is_in_partition_key === 1,
            isSortingKey: col.is_in_sorting_key === 1,
          },
        });
        newEdges.push({
          id: `edge-cat-columns-${col.name}`,
          source: 'cat-columns',
          target: nodeId,
          animated: false,
          style: { stroke: '#4b5563' },
        });
      });
    }

    // Projection nodes (when Projections category is selected)
    if (selectedTableCategory === 'projections' && projections.length > 0) {
      projections.forEach((proj, i) => {
        const nodeId = `proj-${proj.name}`;
        newNodes.push({
          id: nodeId,
          type: 'projection',
          position: { x: columnX[4], y: 50 + i * nodeSpacing },
          data: {
            label: proj.name,
            type: proj.type,
            sortingKey: proj.sorting_key,
            selected: selectedProjection === proj.name,
            onClick: () => setSelectedProjection(proj.name === selectedProjection ? null : proj.name),
          },
        });
        newEdges.push({
          id: `edge-cat-projections-${proj.name}`,
          source: 'cat-projections',
          target: nodeId,
          animated: false,
          style: { stroke: '#4b5563' },
        });
      });
    }

    // Index nodes (when Indexes category is selected)
    if (selectedTableCategory === 'indexes' && indexes.length > 0) {
      indexes.forEach((idx, i) => {
        const nodeId = `idx-${idx.name}`;
        newNodes.push({
          id: nodeId,
          type: 'index',
          position: { x: columnX[4], y: 50 + i * nodeSpacing },
          data: {
            label: idx.name,
            type: idx.type,
            expr: idx.expr,
            granularity: idx.granularity,
          },
        });
        newEdges.push({
          id: `edge-cat-indexes-${idx.name}`,
          source: 'cat-indexes',
          target: nodeId,
          animated: false,
          style: { stroke: '#4b5563' },
        });
      });
    }

    // Part nodes (when partition is selected)
    if (selectedPartition && parts.length > 0) {
      parts.forEach((p, i) => {
        const nodeId = `prt-${p.name}`;
        newNodes.push({
          id: nodeId,
          type: 'part',
          position: { x: columnX[5], y: 50 + i * nodeSpacing },
          data: {
            label: p.name,
            rows: p.rows || 0,
            bytes: p.bytes_on_disk || 0,
            level: p.level,
          },
        });
        newEdges.push({
          id: `edge-partition-${selectedPartition}-${p.name}`,
          source: `part-${selectedPartition}`,
          target: nodeId,
          animated: false,
          style: { stroke: '#4b5563' },
        });
      });
    }

    // Projection part nodes (when projection is selected)
    if (selectedProjection && projectionParts.length > 0) {
      projectionParts.forEach((pp, i) => {
        const nodeId = `projpart-${pp.part_name}`;
        newNodes.push({
          id: nodeId,
          type: 'projectionPart',
          position: { x: columnX[5], y: 50 + i * nodeSpacing },
          data: {
            label: pp.part_name,
            rows: pp.rows || 0,
            bytes: pp.bytes_on_disk || 0,
            parentPart: pp.parent_part_name || '',
          },
        });
        newEdges.push({
          id: `edge-projection-${selectedProjection}-${pp.part_name}`,
          source: `proj-${selectedProjection}`,
          target: nodeId,
          animated: false,
          style: { stroke: '#4b5563' },
        });
      });
    }

    setNodes(newNodes);
    setEdges(newEdges);
  }, [databases, users, tables, partitions, parts, columns, projections, projectionParts, indexes, selectedRoot, selectedDatabase, selectedTable, selectedTableCategory, selectedPartition, selectedProjection, setNodes, setEdges]);

  const isLoading = loading || loadingTables || loadingPartitions || loadingParts || loadingColumns || loadingProjections || loadingProjectionParts || loadingIndexes;

  return (
    <div className="fixed inset-0 bg-black/70 flex items-center justify-center z-50" onClick={onClose}>
      <div
        className="bg-gray-900 border border-gray-700 rounded-lg w-[95vw] h-[90vh] overflow-hidden flex flex-col"
        onClick={(e) => e.stopPropagation()}
      >
        {/* Header */}
        <div className="flex items-center justify-between p-3 border-b border-gray-700 shrink-0">
          <div className="flex items-center gap-3">
            <Database className="w-5 h-5 text-blue-400" />
            <h2 className="text-sm font-semibold text-white">Database Browser</h2>
            {isLoading && <Loader2 className="w-4 h-4 text-blue-400 animate-spin" />}
          </div>
          <button onClick={onClose} className="text-gray-400 hover:text-white">
            <X className="w-4 h-4" />
          </button>
        </div>

        {/* Breadcrumb */}
        <div className="px-4 py-2 border-b border-gray-700 flex items-center gap-2 text-xs shrink-0">
          <span className="text-gray-400">Path:</span>
          <div className="flex items-center gap-1">
            {selectedRoot ? (
              <>
                <span className={selectedRoot === 'databases' ? 'text-blue-400' : 'text-amber-400'}>
                  {selectedRoot === 'databases' ? 'Databases' : 'Users'}
                </span>
                {selectedRoot === 'databases' && selectedDatabase && (
                  <>
                    <ChevronRight className="w-3 h-3 text-gray-500" />
                    <span className="text-blue-400">{selectedDatabase}</span>
                  </>
                )}
                {selectedTable && (
                  <>
                    <ChevronRight className="w-3 h-3 text-gray-500" />
                    <span className="text-green-400">{selectedTable}</span>
                  </>
                )}
                {selectedPartition && (
                  <>
                    <ChevronRight className="w-3 h-3 text-gray-500" />
                    <span className="text-purple-400">{selectedPartition}</span>
                  </>
                )}
              </>
            ) : (
              <span className="text-gray-500">Select a category to browse</span>
            )}
          </div>
        </div>

        {/* Legend */}
        <div className="px-4 py-2 border-b border-gray-700 flex items-center gap-4 text-xs shrink-0">
          <div className="flex items-center gap-1">
            <Database className="w-3 h-3 text-blue-400" />
            <span className="text-gray-400">Database</span>
          </div>
          <div className="flex items-center gap-1">
            <Table className="w-3 h-3 text-green-400" />
            <span className="text-gray-400">Table</span>
          </div>
          <div className="flex items-center gap-1">
            <Layers className="w-3 h-3 text-purple-400" />
            <span className="text-gray-400">Partition</span>
          </div>
          <div className="flex items-center gap-1">
            <Box className="w-3 h-3 text-orange-400" />
            <span className="text-gray-400">Part</span>
          </div>
          <div className="flex items-center gap-1">
            <Columns className="w-3 h-3 text-cyan-400" />
            <span className="text-gray-400">Column</span>
          </div>
          <div className="flex items-center gap-1">
            <Sparkles className="w-3 h-3 text-pink-400" />
            <span className="text-gray-400">Projection</span>
          </div>
          <div className="flex items-center gap-1">
            <Zap className="w-3 h-3 text-amber-400" />
            <span className="text-gray-400">Index</span>
          </div>
          <div className="flex items-center gap-1">
            <User className="w-3 h-3 text-amber-400" />
            <span className="text-gray-400">User</span>
          </div>
        </div>

        {/* React Flow */}
        <div className="flex-1">
          <ReactFlow
            nodes={nodes}
            edges={edges}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            nodeTypes={nodeTypes}
            defaultViewport={{ x: 20, y: 20, zoom: 1 }}
            minZoom={0.5}
            maxZoom={1.5}
            proOptions={{ hideAttribution: true }}
          >
            <Background variant={BackgroundVariant.Dots} gap={16} size={1} color="#374151" />
            <Controls
              className="!bg-gray-800 !border-gray-600 !rounded [&>button]:!bg-gray-700 [&>button]:!border-gray-600 [&>button]:!text-gray-300 [&>button:hover]:!bg-gray-600 [&>button>svg]:!fill-gray-300"
            />
          </ReactFlow>
        </div>
      </div>
    </div>
  );
}
