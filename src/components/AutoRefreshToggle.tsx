type RefreshInterval = 'off' | 5 | 10 | 60 | 600;

interface AutoRefreshToggleProps {
  interval: RefreshInterval;
  onIntervalChange: (interval: RefreshInterval) => void;
}

export function AutoRefreshToggle({ interval, onIntervalChange }: AutoRefreshToggleProps) {
  const options: { value: RefreshInterval; label: string }[] = [
    { value: 'off', label: 'Auto Refresh Off' },
    { value: 5, label: 'Refresh 5s' },
    { value: 10, label: 'Refresh 10s' },
    { value: 60, label: 'Refresh 1m' },
    { value: 600, label: 'Refresh 10m' },
  ];

  return (
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
  );
}
