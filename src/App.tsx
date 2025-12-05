import { useState, useEffect } from 'react';
import { Dog, RefreshCw, Database, HardDrive, Activity, BarChart2, Server, Info, X, FolderTree, Terminal, FileText } from 'lucide-react';
import { AutoRefreshToggle } from './components/AutoRefreshToggle';
import { QueriesPage } from './components/pages/QueriesPage';
import { PartsPage } from './components/pages/PartsPage';
import { ActivityPage } from './components/pages/ActivityPage';
import { MetricsPage } from './components/pages/MetricsPage';
import { InstancePage } from './components/pages/InstancePage';
import { TextLogPage } from './components/pages/TextLogPage';
import { ProfileEventsModal } from './components/ProfileEventsModal';
import { DatabaseBrowser } from './components/DatabaseBrowser';
import { QueryEditor } from './components/QueryEditor';
import { useQueryStore } from './stores/queryStore';
import { useQueryData } from './hooks/useQueryData';

type NavItem = 'queries' | 'textlog' | 'parts' | 'activity' | 'metrics' | 'instance';
type RefreshInterval = 'off' | 10 | 30 | 60;

function App() {
  const [navItem, setNavItem] = useState<NavItem>('queries');
  const [refreshInterval, setRefreshInterval] = useState<RefreshInterval>('off');
  const [aboutOpen, setAboutOpen] = useState(false);
  const [browserOpen, setBrowserOpen] = useState(false);
  const [queryEditorOpen, setQueryEditorOpen] = useState(false);
  const [queryEditorInitialQuery, setQueryEditorInitialQuery] = useState('');
  const { loading, error } = useQueryStore();
  const { refresh } = useQueryData();

  // Expose function to open query editor with a query (for use by ProfileEventsModal)
  useEffect(() => {
    (window as unknown as { openQueryEditor: (query: string) => void }).openQueryEditor = (query: string) => {
      setQueryEditorInitialQuery(query);
      setQueryEditorOpen(true);
    };
  }, []);

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
        setQueryEditorOpen(false);
      }
    };
    document.addEventListener('keydown', handleEscape);
    return () => document.removeEventListener('keydown', handleEscape);
  }, []);

  const navItems: { id: NavItem; label: string; icon: typeof Database }[] = [
    { id: 'queries', label: 'Queries', icon: Database },
    { id: 'textlog', label: 'Text Log', icon: FileText },
    { id: 'parts', label: 'Part Log', icon: HardDrive },
    { id: 'activity', label: 'Activity', icon: Activity },
    { id: 'metrics', label: 'Metrics', icon: BarChart2 },
    { id: 'instance', label: 'Instance', icon: Server },
  ];

  return (
    <div className="h-screen bg-gray-950 text-white flex flex-col overflow-hidden">
      {/* Header */}
      <header className="bg-gray-900 border-b border-gray-700 px-3 py-1.5 flex items-center justify-between shrink-0">
        <div className="flex items-center gap-6">
          <div className="flex items-center gap-2">
            <Dog className="w-5 h-5 text-blue-400" />
            <h1 className="text-base font-bold text-white">Query Dog</h1>
          </div>

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
          {error && <span className="text-xs text-red-400">{error}</span>}
          <button
            onClick={() => setBrowserOpen(true)}
            className="flex items-center gap-1 px-2 py-1 bg-gray-700 hover:bg-gray-600 rounded text-gray-300 text-xs"
            title="Database Browser"
          >
            <FolderTree className="w-3.5 h-3.5" />
            Browser
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

      {/* Main Content */}
      <main className="flex-1 overflow-hidden">
        {navItem === 'queries' && <QueriesPage />}
        {navItem === 'textlog' && <TextLogPage />}
        {navItem === 'parts' && <PartsPage />}
        {navItem === 'activity' && <ActivityPage />}
        {navItem === 'metrics' && <MetricsPage />}
        {navItem === 'instance' && <InstancePage />}
      </main>

      {/* Profile Events Modal */}
      <ProfileEventsModal />

      {/* Database Browser Modal */}
      {browserOpen && <DatabaseBrowser onClose={() => setBrowserOpen(false)} />}

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
    </div>
  );
}

export default App;
