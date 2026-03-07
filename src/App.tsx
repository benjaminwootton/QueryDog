import { useState, useEffect } from 'react';
import { Dog, RefreshCw, Database, HardDrive, Activity, BarChart2, Server, Info, X, FolderTree, Terminal, FileText, Layers, FileCode, Table, AlertTriangle, Users, Network } from 'lucide-react';
import { AutoRefreshToggle } from './components/AutoRefreshToggle';
import { QueriesPage } from './components/pages/QueriesPage';
import { PartsPage } from './components/pages/PartsPage';
import { PartLogPage } from './components/pages/PartLogPage';
import { ActivityPage } from './components/pages/ActivityPage';
import { MetricsPage } from './components/pages/MetricsPage';
import { InstancePage } from './components/pages/InstancePage';
import { UsersPage } from './components/pages/UsersPage';
import { ClusterPage } from './components/pages/ClusterPage';
import { TextLogPage } from './components/pages/TextLogPage';
import { MyQueriesPage } from './components/pages/MyQueriesPage';
import { DataExplorerPage } from './components/pages/DataExplorerPage';
import { ProfileEventsModal } from './components/ProfileEventsModal';
import { DatabaseBrowser } from './components/DatabaseBrowser';
import { QueryEditor } from './components/QueryEditor';
import { useQueryStore } from './stores/queryStore';
import { useQueryData } from './hooks/useQueryData';

type NavItem = 'queries' | 'textlog' | 'partlog' | 'parts' | 'activity' | 'metrics' | 'users' | 'cluster' | 'instance' | 'myqueries';
type RefreshInterval = 'off' | 10 | 30 | 60;

interface ConnectionInfo {
  host: string;
  port: string;
  secure: boolean;
  user: string;
}

// Helper to get a cookie value by name
function getCookie(name: string): string | null {
  const match = document.cookie.match(new RegExp('(^| )' + name + '=([^;]+)'));
  return match ? match[2] : null;
}

// Helper to set a cookie
function setCookie(name: string, value: string, days: number = 365) {
  const expires = new Date(Date.now() + days * 864e5).toUTCString();
  document.cookie = `${name}=${value}; expires=${expires}; path=/`;
}

function App() {
  const [navItem, setNavItem] = useState<NavItem>('queries');
  const [refreshInterval, setRefreshInterval] = useState<RefreshInterval>('off');
  // Show about modal on first visit only (check cookie)
  const [aboutOpen, setAboutOpen] = useState(() => {
    const hasVisited = getCookie('querydog_visited');
    if (!hasVisited) {
      setCookie('querydog_visited', 'true');
      return true;
    }
    return false;
  });
  const [browserOpen, setBrowserOpen] = useState(false);
  const [dataExplorerOpen, setDataExplorerOpen] = useState(false);
  const [queryEditorOpen, setQueryEditorOpen] = useState(false);
  const [queryEditorInitialQuery, setQueryEditorInitialQuery] = useState('');
  const [connectionInfo, setConnectionInfo] = useState<ConnectionInfo | null>(null);
  const [backendError, setBackendError] = useState<string | null>(null);
  const [hasQueriesFolder, setHasQueriesFolder] = useState(false);
  const { loading, error, hasPartLogAccess, hasPartsAccess, setActiveTab, setChartMetric, setTimeRange, setFieldFilter, clearAllFilters, setError } = useQueryStore();
  const { refresh } = useQueryData();

  // Fetch connection info on mount
  useEffect(() => {
    fetch('/api/connection-info')
      .then(async res => {
        const data = await res.json();
        if (!res.ok) {
          // Backend is running but can't connect to ClickHouse
          throw new Error(data.error || 'Failed to connect to ClickHouse');
        }
        return data;
      })
      .then(info => {
        setConnectionInfo(info);
        setBackendError(null);
      })
      .catch((err) => {
        const message = err.message === 'Failed to fetch'
          ? 'Failed to connect to backend server. Please check if the server is running.'
          : `Connection failed: ${err.message}`;
        setBackendError(message);
      });
  }, []);

  // Check if queries folder exists
  useEffect(() => {
    fetch('/api/my-queries/exists')
      .then(res => res.json())
      .then(data => setHasQueriesFolder(data.exists))
      .catch(() => setHasQueriesFolder(false));
  }, []);

  // Expose function to open query editor with a query (for use by ProfileEventsModal)
  useEffect(() => {
    (window as unknown as { openQueryEditor: (query: string) => void }).openQueryEditor = (query: string) => {
      // Strip off FORMAT JSON/JSONEachRow/etc from the end (added by ClickHouse for remote queries)
      const cleanedQuery = query.replace(/\s+FORMAT\s+\w+\s*$/i, '').trim();
      setQueryEditorInitialQuery(cleanedQuery);
      setQueryEditorOpen(true);
    };
  }, []);

  // Expose function to navigate to Queries page filtered by query_id
  useEffect(() => {
    (window as unknown as { navigateToQueryId: (queryId: string) => void }).navigateToQueryId = (queryId: string) => {
      // Clear existing filters and set query_id filter
      clearAllFilters();
      setFieldFilter('query_id', [queryId]);
      // Set time range to last 24 hours
      const end = new Date();
      const start = new Date(end.getTime() - 24 * 60 * 60 * 1000);
      setTimeRange({ start, end });
      // Navigate to queries page
      setNavItem('queries');
      setActiveTab('queries');
    };
  }, [clearAllFilters, setFieldFilter, setTimeRange, setActiveTab]);

  // Expose function to navigate to Queries page filtered by multiple query_ids
  useEffect(() => {
    (window as unknown as { navigateToQueryIds: (queryIds: string[]) => void }).navigateToQueryIds = (queryIds: string[]) => {
      if (queryIds.length === 0) return;
      // Clear existing filters and set query_id filter with all IDs
      clearAllFilters();
      setFieldFilter('query_id', queryIds);
      // Set time range to last 24 hours
      const end = new Date();
      const start = new Date(end.getTime() - 24 * 60 * 60 * 1000);
      setTimeRange({ start, end });
      // Navigate to queries page
      setNavItem('queries');
      setActiveTab('queries');
    };
  }, [clearAllFilters, setFieldFilter, setTimeRange, setActiveTab]);

  // Global auto-refresh
  useEffect(() => {
    if (refreshInterval === 'off') return;
    const interval = setInterval(refresh, refreshInterval * 1000);
    return () => clearInterval(interval);
  }, [refreshInterval, refresh]);

  // Close modals on Escape key
  useEffect(() => {
    const handleEscape = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        setAboutOpen(false);
        setBrowserOpen(false);
        setDataExplorerOpen(false);
        setQueryEditorOpen(false);
        setError(null);
      }
    };
    document.addEventListener('keydown', handleEscape);
    return () => document.removeEventListener('keydown', handleEscape);
  }, []);

  const navItems: { id: NavItem; label: string; icon: typeof Database }[] = [
    { id: 'queries', label: 'Queries', icon: Database },
    ...(hasPartsAccess ? [{ id: 'parts' as const, label: 'Objects', icon: HardDrive }] : []),
    ...(hasPartLogAccess ? [{ id: 'partlog' as const, label: 'Parts Log', icon: Layers }] : []),
    { id: 'activity', label: 'Activity', icon: Activity },
    { id: 'metrics', label: 'Metrics', icon: BarChart2 },
    { id: 'textlog', label: 'Text Log', icon: FileText },
    { id: 'users', label: 'Users', icon: Users },
    { id: 'cluster', label: 'Cluster', icon: Network },
    { id: 'instance', label: 'Instance', icon: Server },
    ...(hasQueriesFolder ? [{ id: 'myqueries' as const, label: 'My Queries', icon: FileCode }] : []),
  ];

  return (
    <div className="h-screen bg-gray-950 text-white flex flex-col overflow-hidden">
      {/* Header */}
      <header className="bg-gray-900 border-b border-gray-700 px-3 py-1.5 flex items-center justify-between shrink-0">
        <div className="flex items-center gap-6">
          <button
            onClick={() => {
              setNavItem('queries');
              setActiveTab('queries');
              setChartMetric('count');
              // Reset to last 1 hour
              const end = new Date();
              const start = new Date(end.getTime() - 60 * 60 * 1000);
              setTimeRange({ start, end });
            }}
            className="flex items-center gap-2 hover:opacity-80 transition-opacity"
          >
            <Dog className="w-5 h-5 text-blue-400" />
            <h1 className="text-base font-bold text-white">Query Dog</h1>
          </button>

          {/* Top Navigation */}
          <nav className="flex items-center gap-1">
            {navItems.map(({ id, label, icon: Icon }) => (
              <button
                key={id}
                onClick={() => setNavItem(id)}
                className={`flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium rounded transition-colors ${
                  navItem === id
                    ? 'bg-blue-600 text-white'
                    : 'text-gray-400 hover:text-white hover:bg-gray-800'
                }`}
              >
                <Icon className="w-3.5 h-3.5" />
                {label}
              </button>
            ))}
          </nav>
        </div>

        <div className="flex items-center gap-3">
          {connectionInfo && (
            <span className="text-xs text-gray-400 font-mono">
              {connectionInfo.user}@{connectionInfo.host}:{connectionInfo.port}
            </span>
          )}
          <button
            onClick={() => setBrowserOpen(true)}
            className="flex items-center gap-1 px-2 py-1 bg-gray-700 hover:bg-gray-600 rounded text-gray-300 text-xs"
            title="Database Browser"
          >
            <FolderTree className="w-3.5 h-3.5" />
            Browser
          </button>
          <button
            onClick={() => setDataExplorerOpen(true)}
            className="flex items-center gap-1 px-2 py-1 bg-gray-700 hover:bg-gray-600 rounded text-gray-300 text-xs"
            title="Data Explorer"
          >
            <Table className="w-3.5 h-3.5" />
            Data
          </button>
          <button
            onClick={() => {
              setQueryEditorInitialQuery('');
              setQueryEditorOpen(true);
            }}
            className="flex items-center gap-1 px-2 py-1 bg-gray-700 hover:bg-gray-600 rounded text-gray-300 text-xs"
            title="Query Editor"
          >
            <Terminal className="w-3.5 h-3.5" />
            Query
          </button>
          <AutoRefreshToggle
            interval={refreshInterval}
            onIntervalChange={setRefreshInterval}
          />
          <button
            onClick={refresh}
            disabled={loading}
            className="p-1 bg-gray-700 hover:bg-gray-600 rounded text-gray-300 disabled:opacity-50"
            title="Refresh"
          >
            <RefreshCw className={`w-3.5 h-3.5 ${loading ? 'animate-spin' : ''}`} />
          </button>
          <button
            onClick={() => setAboutOpen(true)}
            className="flex items-center gap-1 px-2 py-1 bg-gray-700 hover:bg-gray-600 rounded text-gray-300 text-xs"
            title="About"
          >
            <Info className="w-3.5 h-3.5" />
            About
          </button>
        </div>
      </header>

      {/* Backend Error Banner */}
      {backendError && (
        <div className="bg-red-900/50 border-b border-red-700 px-4 py-2 flex items-center justify-center gap-2">
          <span className="text-red-300 text-sm">{backendError}</span>
          <button
            onClick={() => {
              setBackendError(null);
              fetch('/api/connection-info')
                .then(async res => {
                  const data = await res.json();
                  if (!res.ok) {
                    throw new Error(data.error || 'Failed to connect to ClickHouse');
                  }
                  return data;
                })
                .then(setConnectionInfo)
                .catch((err) => {
                  const message = err.message === 'Failed to fetch'
                    ? 'Failed to connect to backend server. Please check if the server is running.'
                    : `Connection failed: ${err.message}`;
                  setBackendError(message);
                });
            }}
            className="px-2 py-0.5 bg-red-700 hover:bg-red-600 rounded text-white text-xs"
          >
            Retry
          </button>
        </div>
      )}

      {/* Main Content */}
      <main className="flex-1 overflow-hidden">
        {navItem === 'queries' && <QueriesPage />}
        {navItem === 'textlog' && <TextLogPage />}
        {navItem === 'partlog' && <PartLogPage />}
        {navItem === 'parts' && <PartsPage />}
        {navItem === 'activity' && <ActivityPage />}
        {navItem === 'metrics' && <MetricsPage />}
        {navItem === 'users' && <UsersPage />}
        {navItem === 'cluster' && <ClusterPage />}
        {navItem === 'instance' && <InstancePage />}
        {navItem === 'myqueries' && <MyQueriesPage />}
      </main>

      {/* Profile Events Modal */}
      <ProfileEventsModal />

      {/* Database Browser Modal */}
      {browserOpen && <DatabaseBrowser onClose={() => setBrowserOpen(false)} />}

      {/* Data Explorer Modal */}
      {dataExplorerOpen && (
        <div className="fixed inset-0 bg-black/70 flex items-center justify-center z-50" onClick={() => setDataExplorerOpen(false)}>
          <div
            className="bg-gray-900 border border-gray-700 rounded-lg w-[95vw] h-[90vh] overflow-hidden flex flex-col"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="flex items-center justify-between p-3 border-b border-gray-700 shrink-0">
              <h2 className="text-sm font-semibold text-white">Data Explorer</h2>
              <button onClick={() => setDataExplorerOpen(false)} className="text-gray-400 hover:text-white">
                <X className="w-4 h-4" />
              </button>
            </div>
            <div className="flex-1 overflow-hidden">
              <DataExplorerPage />
            </div>
          </div>
        </div>
      )}

      {/* Query Editor Modal */}
      {queryEditorOpen && (
        <QueryEditor
          initialQuery={queryEditorInitialQuery}
          onClose={() => {
            setQueryEditorOpen(false);
            setQueryEditorInitialQuery('');
          }}
        />
      )}

      {/* About Modal */}
      {aboutOpen && (
        <>
          <div className="fixed inset-0 bg-black/50 z-50" onClick={() => setAboutOpen(false)} />
          <div className="fixed top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 bg-gray-800 border border-gray-600 rounded-lg shadow-xl z-50 p-6 min-w-[300px]">
            <button
              onClick={() => setAboutOpen(false)}
              className="absolute top-3 right-3 text-gray-400 hover:text-white"
            >
              <X className="w-4 h-4" />
            </button>
            <div className="flex flex-col items-center gap-4">
              <Dog className="w-12 h-12 text-blue-400" />
              <h2 className="text-xl font-bold text-white">Query Dog</h2>
              <p className="text-sm text-gray-300 text-center">
                A tool for analysing the ClickHouse query log.
              </p>
              <p className="text-sm text-gray-400 text-center italic">
                🎵 Who let the logs out... 🎵
              </p>
              <a
                href="https://x.com/BenjaminWootton"
                target="_blank"
                rel="noopener noreferrer"
                className="text-sm text-blue-400 hover:text-blue-300"
              >
                By @BenjaminWootton
              </a>
            </div>
          </div>
        </>
      )}

      {/* Error Modal */}
      {error && (
        <>
          <div className="fixed inset-0 bg-black/60 z-50" onClick={() => setError(null)} />
          <div className="fixed top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 bg-red-950 border border-red-700 rounded-lg shadow-xl z-50 p-6 min-w-[300px] max-w-[80vw]">
            <button
              onClick={() => setError(null)}
              className="absolute top-3 right-3 text-red-200 hover:text-white"
            >
              <X className="w-4 h-4" />
            </button>
            <div className="flex flex-col items-center gap-3">
              <AlertTriangle className="w-10 h-10 text-red-300" />
              <h2 className="text-lg font-semibold text-red-100">Error</h2>
              <p className="text-sm text-red-100 text-center whitespace-pre-wrap break-words">
                {error}
              </p>
              <button
                onClick={() => setError(null)}
                className="px-3 py-1 bg-red-700 hover:bg-red-600 rounded text-white text-xs"
              >
                Dismiss
              </button>
            </div>
          </div>
        </>
      )}
    </div>
  );
}

export default App;
