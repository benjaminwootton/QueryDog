import { useState, useEffect } from 'react';
import { Dog, Database, HardDrive, Activity, Server, X, FolderTree, Terminal, FileText, Layers, FileCode, Table, AlertTriangle, Users, Network, Circle } from 'lucide-react';
import { AutoRefreshToggle } from './components/AutoRefreshToggle';
import { QueriesPage } from './components/pages/QueriesPage';
import { PartsPage } from './components/pages/PartsPage';
import { PartLogPage } from './components/pages/PartLogPage';
import { ActivityPage } from './components/pages/ActivityPage';
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
import { fetchEnvironments, switchEnvironment, type EnvironmentInfo } from './services/api';

type NavItem = 'queries' | 'textlog' | 'partlog' | 'parts' | 'activity' | 'users' | 'cluster' | 'instance' | 'myqueries';
type RefreshInterval = 'off' | 5 | 10 | 60 | 600;

interface ConnectionInfo {
  name: string;
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
  const [backendError, setBackendError] = useState<{ message: string; envName?: string; isBackendServer?: boolean } | null>(null);
  const [hasQueriesFolder, setHasQueriesFolder] = useState(false);
  const [environments, setEnvironments] = useState<EnvironmentInfo[]>([]);
  const [activeEnvIndex, setActiveEnvIndex] = useState(0);
  const [switching, setSwitching] = useState(false);
  const { error, hasPartLogAccess, hasPartsAccess, setActiveTab, setChartMetric, setTimeRange, setFieldFilter, clearAllFilters, setError } = useQueryStore();
  const { refresh } = useQueryData();

  // Fetch connection info and environments on mount
  useEffect(() => {
    // First fetch environments so we know what databases are available
    fetchEnvironments()
      .then(data => {
        setEnvironments(data.environments);
        setActiveEnvIndex(data.active);
        return data;
      })
      .then((envData) => {
        // Now check the connection to the active environment
        return fetch('/api/connection-info')
          .then(async res => {
            const data = await res.json();
            if (!res.ok) {
              const envName = envData.environments[envData.active]?.name || 'Unknown';
              throw { message: data.error || 'Failed to connect to ClickHouse', envName };
            }
            return data;
          })
          .then(info => {
            setConnectionInfo(info);
            setBackendError(null);
          })
          .catch((err) => {
            const envName = err.envName || envData.environments[envData.active]?.name || 'Unknown';
            const isBackendServer = err.message === 'Failed to fetch';
            const message = isBackendServer
              ? 'The QueryDog backend server is not running or cannot be reached.'
              : 'Unable to establish a connection to the ClickHouse database.';
            setBackendError({ message, envName, isBackendServer });
          });
      })
      .catch(() => {
        // Environments fetch failed - try connection info anyway
        fetch('/api/connection-info')
          .then(async res => {
            const data = await res.json();
            if (!res.ok) {
              throw new Error(data.error || 'Failed to connect to ClickHouse');
            }
            return data;
          })
          .then(info => {
            setConnectionInfo(info);
            setBackendError(null);
          })
          .catch((err) => {
            const isBackendServer = err.message === 'Failed to fetch';
            const message = isBackendServer
              ? 'The QueryDog backend server is not running or cannot be reached.'
              : 'Unable to establish a connection to the ClickHouse database.';
            setBackendError({ message, isBackendServer });
          });
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
    { id: 'instance', label: 'Instance', icon: Server },
    { id: 'textlog', label: 'Text Log', icon: FileText },
    { id: 'users', label: 'Users', icon: Users },
    { id: 'cluster', label: 'Cluster', icon: Network },
    ...(hasQueriesFolder ? [{ id: 'myqueries' as const, label: 'My Queries', icon: FileCode }] : []),
  ];

  return (
    <div className="h-screen bg-gray-950 text-white flex flex-col overflow-hidden">
      {/* Header */}
      <header className="bg-gray-900 border-b border-gray-700 px-1.5 py-1.5 flex items-center justify-between shrink-0">
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

        <div className="flex items-center gap-2">
          <button
            onClick={() => setBrowserOpen(true)}
            className="flex items-center gap-1 px-2 py-1 bg-gray-700 hover:bg-gray-600 rounded text-gray-300 text-xs"
            title="Database Browser"
          >
            <FolderTree className="w-3.5 h-3.5" />
            Browse
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
        </div>
      </header>


      {/* Main Content */}
      <main className="flex-1 overflow-hidden">
        {navItem === 'queries' && <QueriesPage />}
        {navItem === 'textlog' && <TextLogPage />}
        {navItem === 'partlog' && <PartLogPage />}
        {navItem === 'parts' && <PartsPage />}
        {navItem === 'activity' && <ActivityPage />}

        {navItem === 'users' && <UsersPage />}
        {navItem === 'cluster' && <ClusterPage />}
        {navItem === 'instance' && <InstancePage />}
        {navItem === 'myqueries' && <MyQueriesPage />}
      </main>

      {/* Footer */}
      <footer className="bg-gray-900 border-t border-gray-700 px-1.5 py-1.5 flex items-center justify-between shrink-0">
        <button
          onClick={() => setAboutOpen(true)}
          className="text-xs text-gray-400 hover:text-blue-400 hover:underline cursor-pointer"
        >
          About Query Dog V0.3
        </button>
        <div className="flex items-center gap-3">
          {/* Connection status - show red when error, green when connected */}
          {connectionInfo && !backendError && (
            <span className="flex items-center gap-1.5 text-xs text-gray-400 font-mono">
              <Circle className="w-2 h-2 fill-green-500 text-green-500" />
              {connectionInfo.user}@{connectionInfo.host}:{connectionInfo.port}
            </span>
          )}
          {backendError && (
            <span className="flex items-center gap-1.5 text-xs text-red-400 font-mono">
              <Circle className="w-2 h-2 fill-red-500 text-red-500" />
              {backendError.envName || 'Disconnected'}
            </span>
          )}
          {/* Always show database selector when multiple environments exist */}
          {environments.length > 1 && (
            <select
              value={activeEnvIndex}
              disabled={switching}
              onChange={async (e) => {
                const idx = parseInt(e.target.value);
                const selectedEnv = environments[idx];
                setSwitching(true);
                setBackendError(null);
                try {
                  const result = await switchEnvironment(idx);
                  setActiveEnvIndex(idx);
                  setConnectionInfo({
                    name: result.name,
                    host: result.host,
                    port: String(result.port),
                    secure: false,
                    user: selectedEnv.user,
                  });
                  if (!result.connected) {
                    setBackendError({
                      message: result.error || 'Connection failed',
                      envName: selectedEnv.name
                    });
                  } else {
                    // Connection successful - refresh data
                    refresh();
                  }
                } catch (err) {
                  setBackendError({
                    message: 'Failed to switch environment',
                    envName: selectedEnv.name
                  });
                } finally {
                  setSwitching(false);
                }
              }}
              className="bg-gray-700 border border-gray-600 rounded px-1.5 py-0.5 text-gray-300 text-xs"
            >
              {environments.map((env, i) => (
                <option key={i} value={i}>{env.name}</option>
              ))}
            </select>
          )}
        </div>
      </footer>

      {/* Profile Events Modal */}
      <ProfileEventsModal />

      {/* Database Browser Modal */}
      {browserOpen && <DatabaseBrowser onClose={() => setBrowserOpen(false)} />}

      {/* Data Explorer Modal */}
      {dataExplorerOpen && <DataExplorerPage onClose={() => setDataExplorerOpen(false)} />}

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
              <h2 className="text-xl font-bold text-white">Query Dog V0.3</h2>
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

      {/* Connection Error Modal */}
      {backendError && (
        <>
          <div className="fixed inset-0 bg-black/60 z-50" onClick={() => setBackendError(null)} />
          <div className="fixed top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 bg-red-950 border border-red-700 rounded-lg shadow-xl z-50 p-6 min-w-[400px] max-w-[500px]">
            <button
              onClick={() => setBackendError(null)}
              className="absolute top-3 right-3 text-red-200 hover:text-white"
            >
              <X className="w-4 h-4" />
            </button>
            <div className="flex flex-col items-center gap-4">
              <AlertTriangle className="w-12 h-12 text-red-400" />
              <h2 className="text-lg font-semibold text-red-100">
                {backendError.isBackendServer ? 'Backend Server Unavailable' : 'Database Connection Failed'}
              </h2>
              {!backendError.isBackendServer && backendError.envName && (
                <div className="bg-red-900/50 border border-red-700 rounded px-4 py-2 w-full text-center">
                  <span className="text-red-300 text-sm font-mono">{backendError.envName}</span>
                </div>
              )}
              <p className="text-sm text-red-200 text-center">
                {backendError.message}
              </p>

              {!backendError.isBackendServer && environments.length > 1 && (
                <div className="w-full border-t border-red-800 pt-4 mt-2">
                  <p className="text-sm text-red-300 text-center mb-3">Select a different database:</p>
                  <select
                    value={activeEnvIndex}
                    disabled={switching}
                    onChange={async (e) => {
                      const idx = parseInt(e.target.value);
                      const selectedEnv = environments[idx];
                      setSwitching(true);
                      try {
                        const result = await switchEnvironment(idx);
                        setActiveEnvIndex(idx);
                        setConnectionInfo({
                          name: result.name,
                          host: result.host,
                          port: String(result.port),
                          secure: false,
                          user: selectedEnv.user,
                        });
                        if (!result.connected) {
                          setBackendError({
                            message: 'Unable to establish a connection to the ClickHouse database.',
                            envName: selectedEnv.name,
                            isBackendServer: false
                          });
                        } else {
                          setBackendError(null);
                          refresh();
                        }
                      } catch (err) {
                        setBackendError({
                          message: 'Unable to establish a connection to the ClickHouse database.',
                          envName: selectedEnv.name,
                          isBackendServer: false
                        });
                      } finally {
                        setSwitching(false);
                      }
                    }}
                    className="w-full bg-gray-800 border border-gray-600 rounded px-3 py-2 text-gray-200 text-sm"
                  >
                    {environments.map((env, i) => (
                      <option key={i} value={i}>{env.name}</option>
                    ))}
                  </select>
                </div>
              )}

              <div className="flex gap-3 mt-2">
                <button
                  onClick={() => {
                    const currentEnv = environments[activeEnvIndex];
                    setSwitching(true);
                    fetch('/api/connection-info')
                      .then(async res => {
                        const data = await res.json();
                        if (!res.ok) {
                          throw new Error(data.error || 'Failed to connect');
                        }
                        return data;
                      })
                      .then(info => {
                        setConnectionInfo(info);
                        setBackendError(null);
                        refresh();
                      })
                      .catch((err) => {
                        const isBackendServer = err.message === 'Failed to fetch';
                        const message = isBackendServer
                          ? 'The QueryDog backend server is not running or cannot be reached.'
                          : 'Unable to establish a connection to the ClickHouse database.';
                        setBackendError({ message, envName: currentEnv?.name, isBackendServer });
                      })
                      .finally(() => setSwitching(false));
                  }}
                  disabled={switching}
                  className="px-4 py-2 bg-red-700 hover:bg-red-600 disabled:bg-red-800 disabled:cursor-not-allowed rounded text-white text-sm font-medium"
                >
                  {switching ? 'Connecting...' : 'Retry Connection'}
                </button>
                <button
                  onClick={() => setBackendError(null)}
                  className="px-4 py-2 bg-gray-700 hover:bg-gray-600 rounded text-white text-sm font-medium"
                >
                  Dismiss
                </button>
              </div>
            </div>
          </div>
        </>
      )}
    </div>
  );
}

export default App;
