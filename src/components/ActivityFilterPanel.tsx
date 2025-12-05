import { useState, useEffect } from 'react';
import { Filter, X, ChevronDown, ChevronRight } from 'lucide-react';

interface FilterConfig {
  field: string;
  label: string;
}

interface ActivityFilterPanelProps {
  filters: Record<string, string[]>;
  onFilterChange: (field: string, values: string[]) => void;
  onClearFilter: (field: string) => void;
  onClearAll: () => void;
  fetchDistinct: (field: string) => Promise<string[]>;
  filterableFields: FilterConfig[];
}

export function ActivityFilterPanel({
  filters,
  onFilterChange,
  onClearFilter,
  onClearAll,
  fetchDistinct,
  filterableFields,
}: ActivityFilterPanelProps) {
  const [isOpen, setIsOpen] = useState(false);
  const [expandedField, setExpandedField] = useState<string | null>(null);
  const [fieldValues, setFieldValues] = useState<Record<string, string[]>>({});
  const [loadingField, setLoadingField] = useState<string | null>(null);

  const activeFilterCount = Object.values(filters).filter((v) => v.length > 0).length;

  const loadFieldValues = async (field: string) => {
    if (fieldValues[field]) return;

    setLoadingField(field);
    try {
      const values = await fetchDistinct(field);
      setFieldValues((prev) => ({ ...prev, [field]: values }));
    } catch (error) {
      console.error('Failed to load field values:', error);
    } finally {
      setLoadingField(null);
    }
  };

  const toggleField = (field: string) => {
    if (expandedField === field) {
      setExpandedField(null);
    } else {
      setExpandedField(field);
      loadFieldValues(field);
    }
  };

  const toggleValue = (field: string, value: string) => {
    const current = filters[field] || [];
    if (current.includes(value)) {
      onFilterChange(field, current.filter((v) => v !== value));
    } else {
      onFilterChange(field, [...current, value]);
    }
  };

  // Reset field values when filters change significantly
  useEffect(() => {
    setFieldValues({});
  }, []);

  return (
    <div className="relative">
      <button
        onClick={() => setIsOpen(!isOpen)}
        className={`flex items-center gap-1 px-2 py-0.5 rounded text-xs ${
          activeFilterCount > 0 ? 'bg-blue-600 text-white' : 'bg-gray-700 hover:bg-gray-600 text-gray-300'
        }`}
      >
        <Filter className="w-3 h-3" />
        <span>Filters</span>
        {activeFilterCount > 0 && (
          <span className="bg-white/20 px-1 rounded text-xs">{activeFilterCount}</span>
        )}
      </button>

      {isOpen && (
        <>
          <div className="fixed inset-0 z-40" onClick={() => setIsOpen(false)} />
          <div className="absolute left-0 top-full mt-1 bg-gray-800 border border-gray-600 rounded shadow-lg z-50 min-w-[280px]">
            <div className="flex items-center justify-between p-2 border-b border-gray-700">
              <span className="text-xs font-semibold text-gray-300">Filters</span>
              <div className="flex items-center gap-2">
                {activeFilterCount > 0 && (
                  <button
                    onClick={onClearAll}
                    className="text-xs text-red-400 hover:text-red-300"
                  >
                    Clear all
                  </button>
                )}
                <button onClick={() => setIsOpen(false)} className="text-gray-400 hover:text-white">
                  <X className="w-3 h-3" />
                </button>
              </div>
            </div>
            <div className="max-h-96 overflow-y-auto">
              {filterableFields.map(({ field, label }) => {
                const selectedValues = filters[field] || [];
                const isExpanded = expandedField === field;
                const values = fieldValues[field] || [];

                return (
                  <div key={field} className="border-b border-gray-700/50 last:border-0">
                    <button
                      onClick={() => toggleField(field)}
                      className="w-full flex items-center justify-between p-2 hover:bg-gray-700/50"
                    >
                      <div className="flex items-center gap-2">
                        {isExpanded ? (
                          <ChevronDown className="w-3 h-3 text-gray-400" />
                        ) : (
                          <ChevronRight className="w-3 h-3 text-gray-400" />
                        )}
                        <span className="text-xs text-gray-300">{label}</span>
                      </div>
                      {selectedValues.length > 0 && (
                        <span className="bg-blue-600 px-1.5 py-0.5 rounded text-xs text-white">
                          {selectedValues.length}
                        </span>
                      )}
                    </button>

                    {isExpanded && (
                      <div className="px-2 pb-2">
                        {loadingField === field ? (
                          <div className="text-xs text-gray-400 py-2 text-center">Loading...</div>
                        ) : values.length === 0 ? (
                          <div className="text-xs text-gray-400 py-2 text-center">No values</div>
                        ) : (
                          <div className="max-h-40 overflow-y-auto space-y-1">
                            {values.map((value) => (
                              <label
                                key={value}
                                className="flex items-center gap-2 p-1 hover:bg-gray-700 rounded cursor-pointer"
                              >
                                <input
                                  type="checkbox"
                                  checked={selectedValues.includes(value)}
                                  onChange={() => toggleValue(field, value)}
                                  className="w-3 h-3 rounded border-gray-500 bg-gray-700 text-blue-500"
                                />
                                <span className="text-xs text-gray-300 truncate" title={value}>
                                  {value || '(empty)'}
                                </span>
                              </label>
                            ))}
                          </div>
                        )}
                        {selectedValues.length > 0 && (
                          <button
                            onClick={() => onClearFilter(field)}
                            className="mt-1 text-xs text-red-400 hover:text-red-300"
                          >
                            Clear
                          </button>
                        )}
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          </div>
        </>
      )}
    </div>
  );
}
