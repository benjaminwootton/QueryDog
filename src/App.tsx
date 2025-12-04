import { useState } from 'react';
import { Dog, RefreshCw, Database, HardDrive, Activity, BarChart2, Server } from 'lucide-react';
import { QueriesPage } from './components/pages/QueriesPage';
import { PartsPage } from './components/pages/PartsPage';
import { ActivityPage } from './components/pages/ActivityPage';
import { MetricsPage } from './components/pages/MetricsPage';
import { InstancePage } from './components/pages/InstancePage';
import { ProfileEventsModal } from './components/ProfileEventsModal';
import { useQueryStore } from './stores/queryStore';
import { useQueryData } from './hooks/useQueryData';

type NavItem = 'queries' | 'parts' | 'activity' | 'metrics' | 'instance';

function App() {
  const [navItem, setNavItem] = useState<NavItem>('queries');
  const { loading, error } = useQueryStore();
  const { refresh } = useQueryData();

  const navItems: { id: NavItem; label: string; icon: typeof Database }[] = [
    { id: 'queries', label: 'Queries', icon: Database },
    { id: 'parts', label: 'Parts', icon: HardDrive },
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
            <h1 className="text-base font-bold text-white">QueryDog</h1>
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
            onClick={refresh}
            disabled={loading}
            className="p-1.5 bg-gray-700 hover:bg-gray-600 rounded text-gray-300 disabled:opacity-50"
            title="Refresh"
          >
            <RefreshCw className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} />
          </button>
        </div>
      </header>

      {/* Main Content */}
      <main className="flex-1 overflow-hidden">
        {navItem === 'queries' && <QueriesPage />}
        {navItem === 'parts' && <PartsPage />}
        {navItem === 'activity' && <ActivityPage />}
        {navItem === 'metrics' && <MetricsPage />}
        {navItem === 'instance' && <InstancePage />}
      </main>

      {/* Modal */}
      <ProfileEventsModal />
    </div>
  );
}

export default App;
