import { Timer } from 'lucide-react';

type RefreshInterval = 'off' | 10 | 30 | 60;

interface AutoRefreshToggleProps {
  interval: RefreshInterval;
  onIntervalChange: (interval: RefreshInterval) => void;
}

export function AutoRefreshToggle({ interval, onIntervalChange }: AutoRefreshToggleProps) {
  const options: { value: RefreshInterval; label: string }[] = [
    { value: 'off', label: 'Off' },
    { value: 10, label: '10s' },
    { value: 30, label: '30s' },
    { value: 60, label: '60s' },
  ];

  return (
    <div className="flex items-center gap-1.5 text-xs">
      <Timer className="w-3 h-3 text-gray-400" />
      <select
        value={interval}
        onChange={(e) => {
          const val = e.target.value;
          onIntervalChange(val === 'off' ? 'off' : parseInt(val) as RefreshInterval);
        }}
        className="bg-gray-700 border border-gray-600 rounded px-1.5 py-0.5 text-gray-300 text-xs"
      >
        {options.map((opt) => (
          <option key={opt.value} value={opt.value}>
            {opt.label}
          </option>
        ))}
      </select>
    </div>
  );
}
