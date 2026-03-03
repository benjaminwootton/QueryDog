import { SystemTable } from '../SystemTable';
import { fetchSettings, fetchSettingsColumns } from '../../services/api';

export function InstancePage() {
  return (
    <div className="h-full flex flex-col">
      <div className="border-b border-gray-700 px-4 flex items-center gap-1 shrink-0">
        <span className="flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium border-b-2 -mb-px border-blue-500 text-blue-400">
          Settings
        </span>
      </div>

      <div className="flex-1 overflow-hidden px-4 pt-3 pb-4">
        <SystemTable
          fetchData={fetchSettings}
          fetchColumns={fetchSettingsColumns}
          getRowId={(data) => String(data.name)}
          hideHeader
          columnWidthOverrides={{ name: 400 }}
        />
      </div>
    </div>
  );
}
