import { useState } from 'react';
import { Settings, X } from 'lucide-react';
import { useQueryStore } from '../stores/queryStore';

export function ColumnSelector() {
  const [isOpen, setIsOpen] = useState(false);
  const { columns, toggleColumnVisibility } = useQueryStore();

  return (
    <div className="relative">
      <button
        onClick={() => setIsOpen(!isOpen)}
        className="p-1 bg-gray-700 hover:bg-gray-600 rounded text-gray-300"
        title="Configure columns"
      >
        <Settings className="w-3.5 h-3.5" />
      </button>

      {isOpen && (
        <>
          <div className="fixed inset-0 z-40" onClick={() => setIsOpen(false)} />
          <div className="absolute right-0 top-full mt-1 bg-gray-800 border border-gray-600 rounded shadow-lg z-50 min-w-[200px]">
            <div className="flex items-center justify-between p-2 border-b border-gray-700">
              <span className="text-xs font-semibold text-gray-300">Columns</span>
              <button onClick={() => setIsOpen(false)} className="text-gray-400 hover:text-white">
                <X className="w-3 h-3" />
              </button>
            </div>
            <div className="max-h-64 overflow-y-auto p-1">
              {columns.map((col) => (
                <label
                  key={col.field}
                  className="flex items-center gap-2 p-1.5 hover:bg-gray-700 rounded cursor-pointer"
                >
                  <input
                    type="checkbox"
                    checked={col.visible}
                    onChange={() => toggleColumnVisibility(col.field)}
                    className="w-3 h-3 rounded border-gray-500 bg-gray-700 text-blue-500"
                  />
                  <span className="text-xs text-gray-300">{col.headerName}</span>
                </label>
              ))}
            </div>
          </div>
        </>
      )}
    </div>
  );
}
