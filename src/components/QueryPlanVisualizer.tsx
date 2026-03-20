import { useState, useMemo, useEffect } from 'react';
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
  MiniMap,
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import { X, Copy, Check, ChevronDown, ChevronRight, GitBranch, Layers, Database, Filter, Combine, ArrowRightLeft, Table, Sigma, Workflow, FileText, ClipboardPaste } from 'lucide-react';

// JSON Plan types from ClickHouse
interface ClickHouseIndex {
  Type: string;
  Name?: string;
  Description?: string;
  Keys?: string[];
  Condition?: string;
  'Search Algorithm'?: string;
  'Initial Parts'?: number;
  'Selected Parts'?: number;
  'Initial Granules'?: number;
  'Selected Granules'?: number;
}

interface ClickHousePlanNode {
  'Node Type': string;
  'Node Id': string;
  Description?: string;
  Plans?: ClickHousePlanNode[];
  Indexes?: ClickHouseIndex[];
}

interface ClickHousePlan {
  Plan: ClickHousePlanNode;
}

interface QueryPlanVisualizerProps {
  planJson: ClickHousePlan[] | null;
  onClose: () => void;
}

interface ParsedNode {
  id: string;
  label: string;
  type: string;
  description?: string;
  indexes?: ParsedIndex[];
  children: ParsedNode[];
}

interface ParsedIndex {
  type: string;
  name?: string;
  description?: string;
  keys?: string[];
  condition?: string;
  algorithm?: string;
  initialParts?: number;
  selectedParts?: number;
  initialGranules?: number;
  selectedGranules?: number;
}

// Node type colors based on operation
const NODE_COLORS: Record<string, { bg: string; border: string; icon: string }> = {
  ReadFromMergeTree: { bg: 'bg-green-800', border: 'border-green-500', icon: 'database' },
  MergeTreeSelect: { bg: 'bg-green-700', border: 'border-green-400', icon: 'database' },
  Expression: { bg: 'bg-blue-800', border: 'border-blue-500', icon: 'workflow' },
  ExpressionTransform: { bg: 'bg-blue-700', border: 'border-blue-400', icon: 'workflow' },
  Filter: { bg: 'bg-yellow-800', border: 'border-yellow-500', icon: 'filter' },
  FilterTransform: { bg: 'bg-yellow-700', border: 'border-yellow-400', icon: 'filter' },
  Join: { bg: 'bg-purple-800', border: 'border-purple-500', icon: 'combine' },
  JoiningTransform: { bg: 'bg-purple-700', border: 'border-purple-400', icon: 'combine' },
  Aggregating: { bg: 'bg-orange-800', border: 'border-orange-500', icon: 'sigma' },
  AggregatingTransform: { bg: 'bg-orange-700', border: 'border-orange-400', icon: 'sigma' },
  Resize: { bg: 'bg-gray-700', border: 'border-gray-500', icon: 'arrows' },
  FillingRightJoinSide: { bg: 'bg-indigo-700', border: 'border-indigo-400', icon: 'combine' },
  CreatingSets: { bg: 'bg-cyan-800', border: 'border-cyan-500', icon: 'layers' },
  VersionedCollapsingTransform: { bg: 'bg-pink-700', border: 'border-pink-400', icon: 'layers' },
  SimpleSquashingTransform: { bg: 'bg-teal-700', border: 'border-teal-400', icon: 'layers' },
  ColumnPermuteTransform: { bg: 'bg-lime-700', border: 'border-lime-400', icon: 'table' },
  FilterSortedStreamByRange: { bg: 'bg-amber-700', border: 'border-amber-400', icon: 'filter' },
  // Index types
  'Index:PrimaryKey': { bg: 'bg-emerald-700', border: 'border-emerald-400', icon: 'filter' },
  'Index:MinMax': { bg: 'bg-cyan-700', border: 'border-cyan-400', icon: 'filter' },
  'Index:Partition': { bg: 'bg-violet-700', border: 'border-violet-400', icon: 'filter' },
  'Index:Skip': { bg: 'bg-amber-700', border: 'border-amber-400', icon: 'filter' },
  'Index': { bg: 'bg-teal-700', border: 'border-teal-400', icon: 'filter' },
  default: { bg: 'bg-gray-800', border: 'border-gray-600', icon: 'workflow' },
};

function getNodeColor(label: string): { bg: string; border: string; icon: string } {
  for (const [key, value] of Object.entries(NODE_COLORS)) {
    if (label.includes(key)) {
      return value;
    }
  }
  return NODE_COLORS.default;
}

function getIcon(iconType: string) {
  switch (iconType) {
    case 'database': return Database;
    case 'filter': return Filter;
    case 'combine': return Combine;
    case 'sigma': return Sigma;
    case 'arrows': return ArrowRightLeft;
    case 'layers': return Layers;
    case 'table': return Table;
    case 'workflow':
    default: return Workflow;
  }
}

// Calculate filtering percentage
function getFilteringPercentage(selected: number | undefined, total: number | undefined): number | null {
  if (selected === undefined || total === undefined || total === 0) return null;
  return Math.round(((total - selected) / total) * 100);
}

// Custom node component for plan visualization
function PlanNode({ data }: { data: {
  label: string;
  type: string;
  description?: string;
  indexes?: ParsedIndex[];
} }) {
  const [expanded, setExpanded] = useState(false);
  const colors = getNodeColor(data.type === 'Index' ? `Index:${data.label}` : data.label);
  const Icon = getIcon(colors.icon);
  const hasDetails = data.description || (data.indexes && data.indexes.length > 0);

  // For index nodes embedded in data
  const isReadNode = data.label === 'ReadFromMergeTree';

  return (
    <div className={`relative ${colors.bg} ${colors.border} border rounded-lg shadow-lg min-w-[200px] max-w-[400px]`}>
      <Handle type="target" position={Position.Left} className="!bg-gray-400 !w-2 !h-2" />

      <div
        className="px-3 py-2 cursor-pointer flex items-center gap-2"
        onClick={() => hasDetails && setExpanded(!expanded)}
      >
        <Icon className="w-4 h-4 text-gray-300 shrink-0" />
        <div className="flex-1 min-w-0">
          <div className="text-xs font-medium text-white truncate" title={data.label}>
            {data.label}
          </div>
          {/* Show table name from description for ReadFromMergeTree */}
          {isReadNode && data.description && (
            <div className="text-[10px] text-green-300 font-mono truncate">
              {data.description}
            </div>
          )}
          {/* Show index summary for ReadFromMergeTree nodes */}
          {isReadNode && data.indexes && data.indexes.length > 0 && (
            <div className="flex flex-wrap gap-1 mt-1">
              {data.indexes.map((idx, i) => {
                const filterPct = getFilteringPercentage(idx.selectedGranules, idx.initialGranules);
                const didFilter = filterPct !== null && filterPct > 0;
                return (
                  <span
                    key={i}
                    className={`text-[9px] px-1.5 py-0.5 rounded ${didFilter ? 'bg-green-900/50 text-green-300' : 'bg-gray-700/50 text-gray-400'}`}
                  >
                    {idx.type}: {idx.selectedGranules}/{idx.initialGranules} granules
                    {didFilter && ` (${filterPct}%↓)`}
                  </span>
                );
              })}
            </div>
          )}
        </div>
        {hasDetails && (
          <div className="text-gray-400">
            {expanded ? <ChevronDown className="w-3 h-3" /> : <ChevronRight className="w-3 h-3" />}
          </div>
        )}
      </div>

      {expanded && hasDetails && (
        <div className="px-3 pb-2 border-t border-gray-600/50 mt-1 pt-2 space-y-2">
          {/* Description */}
          {data.description && !isReadNode && (
            <div className="text-[10px] text-gray-300 font-mono break-all">
              {data.description}
            </div>
          )}

          {/* Index details */}
          {data.indexes && data.indexes.map((idx, i) => (
            <div key={i} className="bg-gray-900/50 rounded p-2 space-y-1">
              <div className="text-[10px] font-medium text-white flex items-center gap-2">
                <span className="text-emerald-400">{idx.type}</span>
                {idx.name && <span className="text-gray-400">({idx.name})</span>}
              </div>
              {idx.keys && idx.keys.length > 0 && (
                <div className="text-[10px]">
                  <span className="text-gray-400">Keys: </span>
                  <span className="text-blue-300 font-mono">{idx.keys.join(', ')}</span>
                </div>
              )}
              {idx.condition && (
                <div className="text-[10px] break-all">
                  <span className="text-gray-400">Condition: </span>
                  <span className="text-yellow-300 font-mono text-[9px]">{idx.condition}</span>
                </div>
              )}
              <div className="flex gap-3 text-[10px]">
                {idx.selectedParts !== undefined && idx.initialParts !== undefined && (
                  <span>
                    <span className="text-gray-400">Parts: </span>
                    <span className={idx.selectedParts < idx.initialParts ? 'text-green-300' : 'text-gray-300'}>
                      {idx.selectedParts}/{idx.initialParts}
                    </span>
                  </span>
                )}
                {idx.selectedGranules !== undefined && idx.initialGranules !== undefined && (
                  <span>
                    <span className="text-gray-400">Granules: </span>
                    <span className={idx.selectedGranules < idx.initialGranules ? 'text-green-300' : 'text-gray-300'}>
                      {idx.selectedGranules}/{idx.initialGranules}
                    </span>
                  </span>
                )}
              </div>
              {idx.algorithm && (
                <div className="text-[10px]">
                  <span className="text-gray-400">Algorithm: </span>
                  <span className="text-purple-300">{idx.algorithm}</span>
                </div>
              )}
            </div>
          ))}
        </div>
      )}

      <Handle type="source" position={Position.Right} className="!bg-gray-400 !w-2 !h-2" />
    </div>
  );
}

const nodeTypes = {
  planNode: PlanNode,
};

// Parse JSON plan to internal format
function parseJsonPlan(plans: ClickHousePlan[]): ParsedNode[] {
  const result: ParsedNode[] = [];

  function convertNode(node: ClickHousePlanNode): ParsedNode {
    const parsed: ParsedNode = {
      id: node['Node Id'],
      label: node['Node Type'],
      type: node['Node Type'],
      description: node.Description,
      children: [],
    };

    // Parse indexes for ReadFromMergeTree nodes
    if (node.Indexes && node.Indexes.length > 0) {
      parsed.indexes = node.Indexes.map(idx => ({
        type: idx.Type,
        name: idx.Name,
        description: idx.Description,
        keys: idx.Keys,
        condition: idx.Condition,
        algorithm: idx['Search Algorithm'],
        initialParts: idx['Initial Parts'],
        selectedParts: idx['Selected Parts'],
        initialGranules: idx['Initial Granules'],
        selectedGranules: idx['Selected Granules'],
      }));
    }

    // Recursively process children
    if (node.Plans && node.Plans.length > 0) {
      parsed.children = node.Plans.map(convertNode);
    }

    return parsed;
  }

  for (const plan of plans) {
    if (plan.Plan) {
      result.push(convertNode(plan.Plan));
    }
  }

  return result;
}

// Convert parsed nodes to ReactFlow nodes and edges
// Layout: Left to right (deepest/leaf nodes on left, root on right)
function convertToFlowElements(parsedNodes: ParsedNode[]): { nodes: Node[]; edges: Edge[] } {
  const flowNodes: Node[] = [];
  const flowEdges: Edge[] = [];

  const NODE_HEIGHT = 80;
  const HORIZONTAL_SPACING = 320;
  const VERTICAL_SPACING = 100;

  // Calculate max depth of tree
  function getMaxDepth(node: ParsedNode): number {
    if (node.children.length === 0) return 0;
    return 1 + Math.max(...node.children.map(getMaxDepth));
  }

  // Calculate subtree height (for vertical spacing of siblings)
  function getSubtreeHeight(node: ParsedNode): number {
    if (node.children.length === 0) return NODE_HEIGHT;
    const childrenHeight = node.children.reduce((sum, child) => sum + getSubtreeHeight(child), 0);
    const spacingHeight = (node.children.length - 1) * VERTICAL_SPACING;
    return Math.max(NODE_HEIGHT, childrenHeight + spacingHeight);
  }

  // Process node: children are positioned to the LEFT of parent
  function processNode(node: ParsedNode, x: number, y: number, parentId?: string): void {
    const flowNode: Node = {
      id: node.id,
      type: 'planNode',
      position: { x, y },
      data: {
        label: node.label,
        type: node.type,
        description: node.description,
        indexes: node.indexes,
      },
    };
    flowNodes.push(flowNode);

    if (parentId) {
      // Edge from child (source, on left) to parent (target, on right)
      flowEdges.push({
        id: `edge-${node.id}-${parentId}`,
        source: node.id,
        target: parentId,
        type: 'smoothstep',
        style: { stroke: '#6b7280', strokeWidth: 2 },
        animated: false,
      });
    }

    // Position children to the LEFT of this node
    if (node.children.length > 0) {
      const totalHeight = node.children.reduce((sum, child) => sum + getSubtreeHeight(child), 0) + (node.children.length - 1) * VERTICAL_SPACING;
      let childY = y - totalHeight / 2 + NODE_HEIGHT / 2;

      for (const child of node.children) {
        const childHeight = getSubtreeHeight(child);
        processNode(child, x - HORIZONTAL_SPACING, childY + childHeight / 2 - NODE_HEIGHT / 2, node.id);
        childY += childHeight + VERTICAL_SPACING;
      }
    }
  }

  // Calculate max depth to position root nodes on the right
  const maxDepth = parsedNodes.length > 0 ? Math.max(...parsedNodes.map(getMaxDepth)) : 0;
  const rootX = maxDepth * HORIZONTAL_SPACING;

  // Process all root nodes
  const totalHeight = parsedNodes.reduce((sum, node) => sum + getSubtreeHeight(node), 0) + (parsedNodes.length - 1) * VERTICAL_SPACING;
  let startY = -totalHeight / 2;

  for (const node of parsedNodes) {
    const nodeHeight = getSubtreeHeight(node);
    processNode(node, rootX, startY + nodeHeight / 2);
    startY += nodeHeight + VERTICAL_SPACING;
  }

  return { nodes: flowNodes, edges: flowEdges };
}

export function QueryPlanVisualizer({ planJson, onClose }: QueryPlanVisualizerProps) {
  const [inputText, setInputText] = useState('');
  const [copied, setCopied] = useState(false);
  const [jsonData, setJsonData] = useState<ClickHousePlan[] | null>(planJson);

  // Update when prop changes
  useEffect(() => {
    if (planJson) {
      setJsonData(planJson);
      setInputText(JSON.stringify(planJson, null, 2));
    }
  }, [planJson]);

  // Parse the plan
  const parsedNodes = useMemo(() => {
    if (!jsonData) return [];
    try {
      return parseJsonPlan(jsonData);
    } catch (e) {
      console.error('Failed to parse plan:', e);
      return [];
    }
  }, [jsonData]);

  const { nodes: initialNodes, edges: initialEdges } = useMemo(
    () => convertToFlowElements(parsedNodes),
    [parsedNodes]
  );

  const [nodes, setNodes, onNodesChange] = useNodesState(initialNodes);
  const [edges, setEdges, onEdgesChange] = useEdgesState(initialEdges);

  // Update nodes when parsed data changes
  useEffect(() => {
    setNodes(initialNodes);
    setEdges(initialEdges);
  }, [initialNodes, initialEdges, setNodes, setEdges]);

  const handleCopy = async () => {
    await navigator.clipboard.writeText(inputText);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  const handlePaste = async () => {
    try {
      const text = await navigator.clipboard.readText();
      setInputText(text);
      const parsed = JSON.parse(text);
      setJsonData(Array.isArray(parsed) ? parsed : [parsed]);
    } catch (e) {
      console.error('Failed to parse clipboard JSON:', e);
    }
  };

  const handleTextChange = (text: string) => {
    setInputText(text);
    try {
      const parsed = JSON.parse(text);
      setJsonData(Array.isArray(parsed) ? parsed : [parsed]);
    } catch {
      // Invalid JSON, don't update
    }
  };

  const hasContent = jsonData !== null && parsedNodes.length > 0;

  // Count total nodes
  function countNodes(nodes: ParsedNode[]): number {
    return nodes.reduce((sum, n) => sum + 1 + countNodes(n.children), 0);
  }
  const totalNodeCount = countNodes(parsedNodes);

  return (
    <div className="fixed inset-0 bg-black/80 flex items-center justify-center z-[60]" onClick={onClose}>
      <div
        className="bg-gray-900 border border-gray-700 rounded-lg w-[95vw] h-[90vh] overflow-hidden flex flex-col"
        onClick={(e) => e.stopPropagation()}
      >
        {/* Header */}
        <div className="flex items-center justify-between p-3 border-b border-gray-700 shrink-0">
          <div className="flex items-center gap-3">
            <GitBranch className="w-5 h-5 text-blue-400" />
            <h2 className="text-sm font-semibold text-white">
              Query Plan Visualizer
            </h2>
          </div>
          <div className="flex items-center gap-2">
            <button
              onClick={handlePaste}
              className="flex items-center gap-1 px-2 py-1 text-xs text-gray-400 hover:text-white hover:bg-gray-700 rounded"
              title="Paste JSON from clipboard"
            >
              <ClipboardPaste className="w-3 h-3" />
              Paste JSON
            </button>
            <button
              onClick={handleCopy}
              disabled={!inputText}
              className="flex items-center gap-1 px-2 py-1 text-xs text-gray-400 hover:text-white hover:bg-gray-700 rounded disabled:opacity-50"
            >
              {copied ? <Check className="w-3 h-3 text-green-400" /> : <Copy className="w-3 h-3" />}
              {copied ? 'Copied' : 'Copy'}
            </button>
            <button onClick={onClose} className="text-gray-400 hover:text-white p-1">
              <X className="w-4 h-4" />
            </button>
          </div>
        </div>

        {/* Content */}
        <div className="flex-1 overflow-hidden flex">
          {/* Input Panel - Only show when no content */}
          {!hasContent && (
            <div className="flex-1 flex flex-col items-center justify-center p-8 text-center">
              <FileText className="w-16 h-16 text-gray-600 mb-4" />
              <h3 className="text-lg font-medium text-gray-300 mb-2">No Query Plan Data</h3>
              <p className="text-sm text-gray-500 mb-4">
                Run a query with EXPLAIN or paste JSON plan data to visualize it.
              </p>
              <div className="flex gap-3">
                <button
                  onClick={handlePaste}
                  className="flex items-center gap-2 px-4 py-2 bg-blue-600 hover:bg-blue-500 rounded text-white text-sm font-medium"
                >
                  <ClipboardPaste className="w-4 h-4" />
                  Paste JSON Plan
                </button>
              </div>
              <div className="mt-6 w-full max-w-2xl">
                <textarea
                  value={inputText}
                  onChange={(e) => handleTextChange(e.target.value)}
                  placeholder='Paste JSON plan here (e.g., from EXPLAIN json=1, indexes=1)'
                  className="w-full h-48 bg-gray-800 border border-gray-600 rounded p-3 text-sm font-mono text-gray-200 focus:outline-none focus:border-blue-500 resize-none"
                  spellCheck={false}
                />
              </div>
            </div>
          )}

          {/* Visualization Panel */}
          {hasContent && (
            <div className="flex-1 flex flex-col">
              {/* Controls bar */}
              <div className="flex items-center justify-between px-3 py-2 border-b border-gray-700 bg-gray-800/50">
                <div className="flex items-center gap-4">
                  <span className="text-xs text-gray-400">
                    {parsedNodes.length} root node{parsedNodes.length !== 1 ? 's' : ''} •
                    {totalNodeCount} total node{totalNodeCount !== 1 ? 's' : ''}
                  </span>
                </div>
                <div className="flex items-center gap-2">
                  <button
                    onClick={() => {
                      setJsonData(null);
                      setInputText('');
                    }}
                    className="px-2 py-1 text-xs text-gray-400 hover:text-white hover:bg-gray-700 rounded"
                  >
                    Clear
                  </button>
                </div>
              </div>

              {/* ReactFlow Canvas */}
              <div className="flex-1">
                <ReactFlow
                  nodes={nodes}
                  edges={edges}
                  onNodesChange={onNodesChange}
                  onEdgesChange={onEdgesChange}
                  nodeTypes={nodeTypes}
                  fitView
                  fitViewOptions={{ padding: 0.2 }}
                  minZoom={0.1}
                  maxZoom={2}
                  defaultEdgeOptions={{
                    type: 'smoothstep',
                    style: { stroke: '#6b7280', strokeWidth: 2 },
                  }}
                >
                  <Background color="#374151" variant={BackgroundVariant.Dots} gap={20} />
                  <Controls className="!bg-gray-800 !border-gray-600 !rounded [&>button]:!bg-gray-700 [&>button]:!border-gray-600 [&>button]:hover:!bg-gray-600 [&>button>svg]:!fill-gray-300" />
                  <MiniMap
                    className="!bg-gray-800 !border-gray-600"
                    nodeColor={(node) => {
                      const colors = getNodeColor(node.data?.label as string || '');
                      return colors.bg.replace('bg-', '').replace('-800', '').replace('-700', '');
                    }}
                    maskColor="rgba(0, 0, 0, 0.8)"
                  />
                </ReactFlow>
              </div>
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="px-4 py-2 border-t border-gray-700 flex items-center justify-between text-xs text-gray-500 shrink-0">
          <div className="flex items-center gap-4">
            <span>Click nodes to expand details</span>
            <span>•</span>
            <span>Scroll to zoom, drag to pan</span>
          </div>
          <div className="flex items-center gap-2">
            <span className="text-gray-600">Data flows left → right</span>
          </div>
        </div>
      </div>
    </div>
  );
}
