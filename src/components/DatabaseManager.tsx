import { useState, useEffect, useCallback } from 'react';
import { X, Database, Plus, Pencil, Trash2, Play, Check, AlertCircle, Loader2, Shield, ShieldOff } from 'lucide-react';
import {
  fetchFullEnvironments,
  addEnvironment,
  updateEnvironment,
  deleteEnvironment,
  testEnvironmentConnection,
  switchEnvironment,
  type FullEnvironmentInfo,
} from '../services/api';

type Mode = 'list' | 'add' | 'edit';

interface DatabaseManagerProps {
  isOpen: boolean;
  onClose: () => void;
  onConnect: (result: { name: string; host: string; port: number; connected: boolean; error?: string }, envIndex: number) => void;
  connectedEnvIndex: number | null;
}

const defaultEnv: Omit<FullEnvironmentInfo, 'index'> = {
  name: '',
  host: '',
  port: 8443,
  user: 'default',
  password: '',
  database: 'default',
  secure: true,
  tls_reject_unauthorized: true,
  cluster: '',
  queries_folder: 'queries',
};

export function DatabaseManager({ isOpen, onClose, onConnect, connectedEnvIndex }: DatabaseManagerProps) {
  const [mode, setMode] = useState<Mode>('list');
  const [environments, setEnvironments] = useState<FullEnvironmentInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [editIndex, setEditIndex] = useState<number | null>(null);
  const [formData, setFormData] = useState<Omit<FullEnvironmentInfo, 'index'>>(defaultEnv);
  const [testing, setTesting] = useState(false);
  const [testResult, setTestResult] = useState<{ success: boolean; message?: string; error?: string } | null>(null);
  const [connecting, setConnecting] = useState<number | null>(null);
  const [saving, setSaving] = useState(false);
  const [deleting, setDeleting] = useState<number | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [deleteConfirm, setDeleteConfirm] = useState<number | null>(null);

  const loadEnvironments = useCallback(async () => {
    try {
      setLoading(true);
      setError(null);
      const data = await fetchFullEnvironments();
      setEnvironments(data.environments);
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    if (isOpen) {
      loadEnvironments();
      setMode('list');
      setTestResult(null);
      setDeleteConfirm(null);
    }
  }, [isOpen, loadEnvironments]);

  useEffect(() => {
    const handleEscape = (e: KeyboardEvent) => {
      if (e.key === 'Escape' && isOpen) {
        if (mode !== 'list') {
          setMode('list');
          setTestResult(null);
        } else {
          onClose();
        }
      }
    };
    document.addEventListener('keydown', handleEscape);
    return () => document.removeEventListener('keydown', handleEscape);
  }, [isOpen, mode, onClose]);

  if (!isOpen) return null;

  const handleAdd = () => {
    setFormData(defaultEnv);
    setEditIndex(null);
    setTestResult(null);
    setMode('add');
  };

  const handleEdit = (env: FullEnvironmentInfo) => {
    setFormData({
      name: env.name,
      host: env.host,
      port: env.port,
      user: env.user,
      password: env.password,
      database: env.database,
      secure: env.secure,
      tls_reject_unauthorized: env.tls_reject_unauthorized,
      cluster: env.cluster,
      queries_folder: env.queries_folder,
    });
    setEditIndex(env.index);
    setTestResult(null);
    setMode('edit');
  };

  const handleDelete = async (index: number) => {
    if (deleteConfirm !== index) {
      setDeleteConfirm(index);
      return;
    }

    try {
      setDeleting(index);
      const result = await deleteEnvironment(index);
      if (result.success) {
        await loadEnvironments();
        setDeleteConfirm(null);
      } else {
        setError(result.error || 'Failed to delete environment');
      }
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setDeleting(null);
    }
  };

  const handleTest = async () => {
    setTesting(true);
    setTestResult(null);
    try {
      const result = await testEnvironmentConnection(formData);
      setTestResult(result);
    } catch (err) {
      setTestResult({ success: false, error: (err as Error).message });
    } finally {
      setTesting(false);
    }
  };

  const handleSave = async () => {
    if (!formData.name || !formData.host) {
      setError('Name and host are required');
      return;
    }

    try {
      setSaving(true);
      setError(null);

      let result;
      if (mode === 'add') {
        result = await addEnvironment(formData);
      } else if (editIndex !== null) {
        result = await updateEnvironment(editIndex, formData);
      }

      if (result?.success) {
        await loadEnvironments();
        setMode('list');
        setTestResult(null);
      } else {
        setError(result?.error || 'Failed to save environment');
      }
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setSaving(false);
    }
  };

  const handleConnect = async (index: number) => {
    try {
      setConnecting(index);
      const result = await switchEnvironment(index);
      onConnect(result, index);
      if (result.connected) {
        onClose();
      }
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setConnecting(null);
    }
  };

  const renderForm = () => (
    <div className="space-y-4">
      <div className="grid grid-cols-2 gap-4">
        <div className="col-span-2">
          <label className="block text-xs text-gray-400 mb-1">Connection Name *</label>
          <input
            type="text"
            value={formData.name}
            onChange={(e) => setFormData({ ...formData, name: e.target.value })}
            className="w-full bg-gray-800 border border-gray-700 rounded px-3 py-2 text-sm text-gray-200 focus:border-blue-500 focus:outline-none"
            placeholder="My Database"
          />
        </div>
        <div className="col-span-2 sm:col-span-1">
          <label className="block text-xs text-gray-400 mb-1">Host *</label>
          <input
            type="text"
            value={formData.host}
            onChange={(e) => setFormData({ ...formData, host: e.target.value })}
            className="w-full bg-gray-800 border border-gray-700 rounded px-3 py-2 text-sm text-gray-200 focus:border-blue-500 focus:outline-none"
            placeholder="localhost or clickhouse.example.com"
          />
        </div>
        <div className="col-span-2 sm:col-span-1">
          <label className="block text-xs text-gray-400 mb-1">Port</label>
          <input
            type="number"
            value={formData.port}
            onChange={(e) => setFormData({ ...formData, port: parseInt(e.target.value) || 8443 })}
            className="w-full bg-gray-800 border border-gray-700 rounded px-3 py-2 text-sm text-gray-200 focus:border-blue-500 focus:outline-none"
            placeholder="8443"
          />
        </div>
        <div>
          <label className="block text-xs text-gray-400 mb-1">Username</label>
          <input
            type="text"
            value={formData.user}
            onChange={(e) => setFormData({ ...formData, user: e.target.value })}
            className="w-full bg-gray-800 border border-gray-700 rounded px-3 py-2 text-sm text-gray-200 focus:border-blue-500 focus:outline-none"
            placeholder="default"
          />
        </div>
        <div>
          <label className="block text-xs text-gray-400 mb-1">Password</label>
          <input
            type="password"
            value={formData.password}
            onChange={(e) => setFormData({ ...formData, password: e.target.value })}
            className="w-full bg-gray-800 border border-gray-700 rounded px-3 py-2 text-sm text-gray-200 focus:border-blue-500 focus:outline-none"
            placeholder="••••••••"
          />
        </div>
        <div>
          <label className="block text-xs text-gray-400 mb-1">Database</label>
          <input
            type="text"
            value={formData.database}
            onChange={(e) => setFormData({ ...formData, database: e.target.value })}
            className="w-full bg-gray-800 border border-gray-700 rounded px-3 py-2 text-sm text-gray-200 focus:border-blue-500 focus:outline-none"
            placeholder="default"
          />
        </div>
        <div>
          <label className="block text-xs text-gray-400 mb-1">Cluster (optional)</label>
          <input
            type="text"
            value={formData.cluster}
            onChange={(e) => setFormData({ ...formData, cluster: e.target.value })}
            className="w-full bg-gray-800 border border-gray-700 rounded px-3 py-2 text-sm text-gray-200 focus:border-blue-500 focus:outline-none"
            placeholder="my_cluster"
          />
        </div>
      </div>

      <div className="flex items-center gap-6">
        <label className="flex items-center gap-2 cursor-pointer">
          <input
            type="checkbox"
            checked={formData.secure}
            onChange={(e) => setFormData({
              ...formData,
              secure: e.target.checked,
              port: e.target.checked ? 8443 : 8123,
            })}
            className="w-4 h-4 rounded border-gray-600 bg-gray-800 text-blue-500 focus:ring-blue-500"
          />
          <span className="text-sm text-gray-300 flex items-center gap-1">
            {formData.secure ? <Shield className="w-4 h-4 text-green-400" /> : <ShieldOff className="w-4 h-4 text-yellow-400" />}
            Use HTTPS
          </span>
        </label>
        {formData.secure && (
          <label className="flex items-center gap-2 cursor-pointer">
            <input
              type="checkbox"
              checked={!formData.tls_reject_unauthorized}
              onChange={(e) => setFormData({ ...formData, tls_reject_unauthorized: !e.target.checked })}
              className="w-4 h-4 rounded border-gray-600 bg-gray-800 text-blue-500 focus:ring-blue-500"
            />
            <span className="text-sm text-gray-300">Allow self-signed certs</span>
          </label>
        )}
      </div>

      {testResult && (
        <div className={`p-3 rounded text-sm flex items-center gap-2 ${testResult.success ? 'bg-green-900/50 text-green-300 border border-green-700' : 'bg-red-900/50 text-red-300 border border-red-700'}`}>
          {testResult.success ? <Check className="w-4 h-4" /> : <AlertCircle className="w-4 h-4" />}
          {testResult.success ? 'Connection successful!' : testResult.error || 'Connection failed'}
        </div>
      )}

      <div className="flex justify-between pt-2">
        <button
          onClick={handleTest}
          disabled={testing || !formData.host}
          className="px-4 py-2 bg-gray-700 hover:bg-gray-600 disabled:bg-gray-800 disabled:text-gray-500 rounded text-sm font-medium flex items-center gap-2"
        >
          {testing ? <Loader2 className="w-4 h-4 animate-spin" /> : <Play className="w-4 h-4" />}
          Test Connection
        </button>
        <div className="flex gap-2">
          <button
            onClick={() => { setMode('list'); setTestResult(null); }}
            className="px-4 py-2 bg-gray-700 hover:bg-gray-600 rounded text-sm font-medium"
          >
            Cancel
          </button>
          <button
            onClick={handleSave}
            disabled={saving || !formData.name || !formData.host}
            className="px-4 py-2 bg-blue-600 hover:bg-blue-500 disabled:bg-blue-800 disabled:text-gray-400 rounded text-sm font-medium flex items-center gap-2"
          >
            {saving ? <Loader2 className="w-4 h-4 animate-spin" /> : <Check className="w-4 h-4" />}
            {mode === 'add' ? 'Add Connection' : 'Save Changes'}
          </button>
        </div>
      </div>
    </div>
  );

  const renderList = () => (
    <div className="space-y-3">
      {loading ? (
        <div className="flex items-center justify-center py-8">
          <Loader2 className="w-6 h-6 animate-spin text-blue-400" />
        </div>
      ) : environments.length === 0 ? (
        <div className="text-center py-8 text-gray-400">
          No connections configured
        </div>
      ) : (
        <div className="bg-gray-800 rounded max-h-[30rem] overflow-y-auto">
          <table className="w-full text-xs">
            <thead className="sticky top-0 bg-gray-800">
              <tr className="border-b border-gray-700">
                <th className="text-left p-2 text-gray-400">Name</th>
                <th className="text-left p-2 text-gray-400">Host</th>
                <th className="text-left p-2 text-gray-400">User</th>
                <th className="text-center p-2 text-gray-400 w-16">TLS</th>
                <th className="text-right p-2 text-gray-400 w-32">Actions</th>
              </tr>
            </thead>
            <tbody>
              {environments.map((env) => (
                <tr
                  key={env.index}
                  className={`border-b border-gray-700/50 hover:bg-gray-700/30 ${connectedEnvIndex === env.index ? 'bg-green-900/20' : ''}`}
                >
                  <td className="p-2">
                    <div className="flex items-center gap-2">
                      <span className="text-blue-300 font-medium">{env.name}</span>
                      {connectedEnvIndex === env.index && (
                        <span className="text-[10px] bg-green-700 text-green-100 px-1.5 py-0.5 rounded">Connected</span>
                      )}
                    </div>
                  </td>
                  <td className="p-2 text-gray-300 font-mono text-[11px]">{env.host}:{env.port}</td>
                  <td className="p-2 text-gray-400">{env.user}</td>
                  <td className="p-2 text-center">
                    {env.secure ? (
                      <Shield className="w-4 h-4 text-green-400 mx-auto" />
                    ) : (
                      <ShieldOff className="w-4 h-4 text-yellow-400 mx-auto" />
                    )}
                  </td>
                  <td className="p-2">
                    <div className="flex items-center justify-end gap-1">
                      <button
                        onClick={() => handleConnect(env.index)}
                        disabled={connecting !== null}
                        className="p-1.5 hover:bg-green-700/50 rounded text-green-400 hover:text-green-300 disabled:opacity-50"
                        title="Connect"
                      >
                        {connecting === env.index ? (
                          <Loader2 className="w-4 h-4 animate-spin" />
                        ) : (
                          <Play className="w-4 h-4" />
                        )}
                      </button>
                      <button
                        onClick={() => handleEdit(env)}
                        className="p-1.5 hover:bg-blue-700/50 rounded text-blue-400 hover:text-blue-300"
                        title="Edit"
                      >
                        <Pencil className="w-4 h-4" />
                      </button>
                      <button
                        onClick={() => handleDelete(env.index)}
                        disabled={deleting !== null || environments.length <= 1}
                        className={`p-1.5 rounded disabled:opacity-30 ${deleteConfirm === env.index ? 'bg-red-700 text-white' : 'hover:bg-red-700/50 text-red-400 hover:text-red-300'}`}
                        title={deleteConfirm === env.index ? 'Click again to confirm' : 'Delete'}
                      >
                        {deleting === env.index ? (
                          <Loader2 className="w-4 h-4 animate-spin" />
                        ) : (
                          <Trash2 className="w-4 h-4" />
                        )}
                      </button>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      <div className="flex justify-between pt-2">
        <button
          onClick={handleAdd}
          className="px-4 py-2 bg-green-700 hover:bg-green-600 rounded text-sm font-medium flex items-center gap-2"
        >
          <Plus className="w-4 h-4" />
          Add Connection
        </button>
        <button
          onClick={onClose}
          className="px-4 py-2 bg-gray-700 hover:bg-gray-600 rounded text-sm font-medium"
        >
          Close
        </button>
      </div>
    </div>
  );

  return (
    <>
      <div className="fixed inset-0 bg-black/60 z-50" onClick={() => mode === 'list' ? onClose() : setMode('list')} />
      <div className="fixed top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 bg-gray-900 border border-gray-700 rounded-lg shadow-xl z-50 p-6 w-[960px] max-w-[95vw] max-h-[90vh] overflow-y-auto">
        <button
          onClick={() => mode === 'list' ? onClose() : setMode('list')}
          className="absolute top-3 right-3 text-gray-400 hover:text-white"
        >
          <X className="w-4 h-4" />
        </button>

        <div className="flex items-center gap-3 mb-4">
          <Database className="w-8 h-8 text-blue-400" />
          <div>
            <h2 className="text-lg font-semibold text-gray-100">
              {mode === 'list' ? 'Database Connections' : mode === 'add' ? 'Add Connection' : 'Edit Connection'}
            </h2>
            <p className="text-xs text-gray-400">
              {mode === 'list'
                ? 'Manage your ClickHouse database connections'
                : mode === 'add'
                  ? 'Configure a new database connection'
                  : `Editing: ${formData.name}`}
            </p>
          </div>
        </div>

        {error && (
          <div className="mb-4 p-3 bg-red-900/50 border border-red-700 rounded text-sm text-red-300 flex items-center gap-2">
            <AlertCircle className="w-4 h-4 flex-shrink-0" />
            {error}
            <button onClick={() => setError(null)} className="ml-auto text-red-400 hover:text-red-200">
              <X className="w-4 h-4" />
            </button>
          </div>
        )}

        {mode === 'list' ? renderList() : renderForm()}
      </div>
    </>
  );
}
