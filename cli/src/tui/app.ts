import * as blessed from 'blessed';
import * as contrib from 'blessed-contrib';
import { Environment, listEnvironments } from '../utils/config';
import { createClickHouseClient, executeQuery, closeClient, SystemQueries } from '../utils/clickhouse';

interface TUIState {
  currentEnv: Environment | null;
  currentView: string;
  data: Record<string, unknown>[];
}

export async function startTUI(env?: Environment): Promise<void> {
  const screen = blessed.screen({
    smartCSR: true,
    title: 'QueryDog CLI - TUI Mode',
  });

  const state: TUIState = {
    currentEnv: env || null,
    currentView: 'overview',
    data: [],
  };

  // Create grid layout
  const grid = new contrib.grid({ rows: 12, cols: 12, screen });

  // Header
  const header = grid.set(0, 0, 1, 12, blessed.box, {
    content: '{bold}{blue-fg}QueryDog CLI{/blue-fg}{/bold} - Press ? for help, q to quit',
    tags: true,
    style: { fg: 'white', bg: 'blue' },
  });

  // Environment info
  const envBox = grid.set(1, 0, 1, 12, blessed.box, {
    content: state.currentEnv ? `Environment: ${state.currentEnv.name} (${state.currentEnv.host}:${state.currentEnv.port})` : 'No environment selected - Press e to select',
    tags: true,
    style: { fg: 'yellow' },
  });

  // Menu
  const menu = grid.set(2, 0, 10, 2, blessed.list, {
    label: ' Views ',
    border: { type: 'line' },
    style: {
      fg: 'white',
      border: { fg: 'cyan' },
      selected: { bg: 'blue', fg: 'white' },
    },
    keys: true,
    vi: true,
    items: [
      'Overview',
      'Tables',
      'Views',
      'Partitions',
      'Parts',
      'Mutations',
      'Processes',
      'Merges',
      '---',
      'Queries - All',
      'Queries - Slow',
      'Queries - Memory',
      'Queries - Frequent',
      'Queries - Errors',
      '---',
      'Clusters',
      'Replicas',
      'Disks',
      'Metrics',
      'Users',
      'Settings',
    ],
  });

  // Main data table
  const dataTable = grid.set(2, 2, 8, 10, contrib.table, {
    keys: true,
    fg: 'white',
    label: ' Data ',
    border: { type: 'line', fg: 'cyan' },
    columnSpacing: 2,
    columnWidth: [20, 15, 15, 15, 40],
  });

  // Status bar
  const statusBar = grid.set(10, 2, 2, 10, blessed.box, {
    label: ' Status ',
    border: { type: 'line' },
    style: { fg: 'green', border: { fg: 'cyan' } },
    content: 'Ready',
  });

  // Help popup
  const helpPopup = blessed.box({
    parent: screen,
    top: 'center',
    left: 'center',
    width: '50%',
    height: '60%',
    border: { type: 'line' },
    style: { border: { fg: 'yellow' }, bg: 'black' },
    label: ' Help ',
    hidden: true,
    tags: true,
    content: `
{bold}QueryDog CLI - Keyboard Shortcuts{/bold}

{cyan-fg}Navigation:{/cyan-fg}
  Arrow keys / j/k  - Navigate menu
  Enter             - Select menu item
  Tab               - Switch focus

{cyan-fg}Views:{/cyan-fg}
  t - Tables        v - Views
  p - Partitions    m - Mutations
  P - Processes     M - Merges
  q - Queries       Q - Slow Queries

{cyan-fg}Actions:{/cyan-fg}
  e - Select environment
  r - Refresh current view
  / - Search
  f - Filter

{cyan-fg}General:{/cyan-fg}
  ? - Show this help
  q - Quit
  Esc - Close popup / Cancel
`,
  });

  // Environment selector popup
  const envPopup = blessed.list({
    parent: screen,
    top: 'center',
    left: 'center',
    width: '60%',
    height: '50%',
    border: { type: 'line' },
    style: {
      border: { fg: 'green' },
      selected: { bg: 'blue', fg: 'white' },
    },
    label: ' Select Environment ',
    hidden: true,
    keys: true,
    vi: true,
    items: listEnvironments().map(e => `${e.name} (${e.host}:${e.port})`),
  });

  // Loading indicator
  const loading = blessed.loading({
    parent: screen,
    top: 'center',
    left: 'center',
    height: 5,
    width: 30,
    border: { type: 'line' },
    style: { border: { fg: 'cyan' } },
  });

  // Helper to update status
  function setStatus(text: string, isError = false) {
    statusBar.setContent(isError ? `{red-fg}${text}{/red-fg}` : `{green-fg}${text}{/green-fg}`);
    screen.render();
  }

  // Helper to load data
  async function loadView(view: string) {
    if (!state.currentEnv) {
      setStatus('Please select an environment first (press e)', true);
      return;
    }

    state.currentView = view;
    loading.load('Loading...');
    screen.render();

    try {
      let query: string;
      let headers: string[] = [];

      switch (view) {
        case 'tables':
          query = SystemQueries.tables();
          headers = ['Name', 'Engine', 'Rows', 'Size', 'Partition Key'];
          break;
        case 'views':
          query = SystemQueries.views();
          headers = ['Database', 'Name', 'Engine', 'Modified'];
          break;
        case 'partitions':
          query = SystemQueries.partitionsSummary();
          headers = ['Database', 'Table', 'Partition', 'Parts', 'Rows', 'Size'];
          break;
        case 'parts':
          query = SystemQueries.parts();
          headers = ['Database', 'Table', 'Name', 'Rows', 'Size'];
          break;
        case 'mutations':
          query = SystemQueries.mutations();
          headers = ['Table', 'ID', 'Command', 'Created', 'Done'];
          break;
        case 'processes':
          query = SystemQueries.processes();
          headers = ['Query ID', 'User', 'Elapsed', 'Rows', 'Memory'];
          break;
        case 'merges':
          query = SystemQueries.merges();
          headers = ['Database', 'Table', 'Elapsed', 'Progress', 'Parts'];
          break;
        case 'queries-all':
          query = SystemQueries.queriesAll(24, 50);
          headers = ['Time', 'Duration', 'Rows', 'Memory', 'Query'];
          break;
        case 'queries-slow':
          query = SystemQueries.queriesSlowest(24, 50);
          headers = ['Time', 'Duration', 'Read', 'Written', 'Memory'];
          break;
        case 'queries-memory':
          query = SystemQueries.queriesHighestMemory(24, 50);
          headers = ['Time', 'Memory', 'Peak', 'Duration'];
          break;
        case 'queries-frequent':
          query = SystemQueries.queriesGrouped(24, 50);
          headers = ['Query', 'User', 'Count', 'Total', 'Avg'];
          break;
        case 'queries-errors':
          query = SystemQueries.queriesErrors(24, 50);
          headers = ['Time', 'Duration', 'User', 'Error'];
          break;
        case 'clusters':
          query = SystemQueries.clusters();
          headers = ['Cluster', 'Shard', 'Replica', 'Host', 'Port'];
          break;
        case 'replicas':
          query = SystemQueries.replicas();
          headers = ['Database', 'Table', 'Leader', 'Queue', 'Active'];
          break;
        case 'disks':
          query = SystemQueries.disks();
          headers = ['Name', 'Path', 'Free', 'Total', 'Used %'];
          break;
        case 'metrics':
          query = SystemQueries.metrics();
          headers = ['Metric', 'Value', 'Description'];
          break;
        case 'users':
          query = SystemQueries.users();
          headers = ['Name', 'Storage', 'Auth Type'];
          break;
        case 'settings':
          query = SystemQueries.settings();
          headers = ['Name', 'Value', 'Changed', 'Type'];
          break;
        default:
          // Overview - show database summary
          query = SystemQueries.databasesSummary();
          headers = ['Database', 'Engine', 'Tables', 'Rows', 'Size'];
      }

      const data = await executeQuery<Record<string, unknown>>(query);
      state.data = data;

      // Format data for table
      const tableData = data.slice(0, 100).map(row => {
        return Object.values(row).map(v => {
          const str = String(v ?? '');
          return str.length > 40 ? str.substring(0, 37) + '...' : str;
        });
      });

      dataTable.setData({
        headers,
        data: tableData,
      });

      loading.stop();
      setStatus(`Loaded ${data.length} rows`);
    } catch (err: unknown) {
      loading.stop();
      const message = err instanceof Error ? err.message : String(err);
      setStatus(`Error: ${message}`, true);
    }
  }

  // Select environment
  async function selectEnvironment(index: number) {
    const envs = listEnvironments();
    if (index >= 0 && index < envs.length) {
      state.currentEnv = envs[index];
      createClickHouseClient(state.currentEnv);
      envBox.setContent(`Environment: ${state.currentEnv.name} (${state.currentEnv.host}:${state.currentEnv.port})`);
      envPopup.hide();
      screen.render();
      await loadView('overview');
    }
  }

  // Key bindings
  screen.key(['q', 'C-c'], () => {
    closeClient().then(() => process.exit(0));
  });

  screen.key(['?'], () => {
    helpPopup.toggle();
    screen.render();
  });

  screen.key(['e'], () => {
    envPopup.show();
    envPopup.focus();
    screen.render();
  });

  screen.key(['escape'], () => {
    helpPopup.hide();
    envPopup.hide();
    screen.render();
  });

  screen.key(['r'], () => {
    loadView(state.currentView);
  });

  // Quick view shortcuts
  screen.key(['t'], () => loadView('tables'));
  screen.key(['v'], () => loadView('views'));
  screen.key(['p'], () => loadView('partitions'));
  screen.key(['m'], () => loadView('mutations'));
  screen.key(['S-p'], () => loadView('processes'));
  screen.key(['S-m'], () => loadView('merges'));

  // Menu selection
  menu.on('select', (item: { content: string }, index: number) => {
    const viewMap: Record<string, string> = {
      'Overview': 'overview',
      'Tables': 'tables',
      'Views': 'views',
      'Partitions': 'partitions',
      'Parts': 'parts',
      'Mutations': 'mutations',
      'Processes': 'processes',
      'Merges': 'merges',
      'Queries - All': 'queries-all',
      'Queries - Slow': 'queries-slow',
      'Queries - Memory': 'queries-memory',
      'Queries - Frequent': 'queries-frequent',
      'Queries - Errors': 'queries-errors',
      'Clusters': 'clusters',
      'Replicas': 'replicas',
      'Disks': 'disks',
      'Metrics': 'metrics',
      'Users': 'users',
      'Settings': 'settings',
    };
    const view = viewMap[item.content];
    if (view) {
      loadView(view);
    }
  });

  // Environment popup selection
  envPopup.on('select', (item: unknown, index: number) => {
    selectEnvironment(index);
  });

  // Initial focus
  menu.focus();

  // If env provided, connect and load overview
  if (state.currentEnv) {
    createClickHouseClient(state.currentEnv);
    await loadView('overview');
  }

  screen.render();
}
