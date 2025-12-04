import { useState, useEffect } from 'react';
import { Search, X } from 'lucide-react';
import { useQueryStore } from '../stores/queryStore';

export function SearchBar() {
  const { search, setSearch } = useQueryStore();
  const [localSearch, setLocalSearch] = useState(search);

  useEffect(() => {
    const timer = setTimeout(() => {
      setSearch(localSearch);
    }, 300);
    return () => clearTimeout(timer);
  }, [localSearch, setSearch]);

  return (
    <div className="relative flex items-center">
      <Search className="absolute left-2 w-3 h-3 text-gray-400" />
      <input
        type="text"
        value={localSearch}
        onChange={(e) => setLocalSearch(e.target.value)}
        placeholder="Search query or query_id..."
        className="bg-gray-800 border border-gray-600 rounded pl-6 pr-6 py-0.5 text-white text-xs w-64"
      />
      {localSearch && (
        <button
          onClick={() => setLocalSearch('')}
          className="absolute right-2 text-gray-400 hover:text-white"
        >
          <X className="w-3 h-3" />
        </button>
      )}
    </div>
  );
}
