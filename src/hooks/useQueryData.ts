import { useEffect, useCallback, useRef } from 'react';
import { useQueryStore } from '../stores/queryStore';
import {
  fetchQueryLog,
  fetchTimeSeries,
  fetchTotalCount,
  fetchColumnMetadata,
  fetchPartLog,
  fetchPartLogCount,
  fetchPartLogColumnMetadata,
} from '../services/api';
import { createColumnsFromMetadata } from '../types/queryLog';

export function useQueryData() {
  const {
    timeRange,
    bucketSize,
    search,
    fieldFilters,
    rangeFilters,
    sortField,
    sortOrder,
    partLogSortField,
    partLogSortOrder,
    setEntries,
    setTimeSeries,
    setTotalCount,
    setLoading,
    setError,
    setColumns,
    setPartLogEntries,
    setPartLogTotalCount,
    setPartLogLoading,
    setPartLogColumns,
  } = useQueryStore();

  const columnsLoadedRef = useRef(false);
  const partLogColumnsLoadedRef = useRef(false);

  // Load column metadata once on startup
  useEffect(() => {
    if (columnsLoadedRef.current) return;
    columnsLoadedRef.current = true;

    fetchColumnMetadata()
      .then((metadata) => {
        console.log('Column metadata loaded:', metadata.length, 'columns');
        const columns = createColumnsFromMetadata(metadata, 'query_log');
        console.log('Columns created:', columns.length);
        setColumns(columns);
      })
      .catch((err) => {
        console.error('Failed to load column metadata:', err);
      });
  }, [setColumns]);

  // Load part_log column metadata once on startup
  useEffect(() => {
    if (partLogColumnsLoadedRef.current) return;
    partLogColumnsLoadedRef.current = true;

    fetchPartLogColumnMetadata()
      .then((metadata) => {
        console.log('Part log column metadata loaded:', metadata.length, 'columns');
        const columns = createColumnsFromMetadata(metadata, 'part_log');
        console.log('Part log columns created:', columns.length);
        setPartLogColumns(columns);
      })
      .catch((err) => {
        console.error('Failed to load part_log column metadata:', err);
      });
  }, [setPartLogColumns]);

  const loadData = useCallback(async () => {
    setLoading(true);
    setError(null);

    try {
      const [entries, timeSeries, total] = await Promise.all([
        fetchQueryLog(timeRange, search, sortField, sortOrder, fieldFilters, rangeFilters),
        fetchTimeSeries(timeRange, bucketSize, search, fieldFilters, rangeFilters),
        fetchTotalCount(timeRange, search, fieldFilters, rangeFilters),
      ]);

      setEntries(entries);
      setTimeSeries(timeSeries);
      setTotalCount(total);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load data');
    } finally {
      setLoading(false);
    }
  }, [
    timeRange,
    bucketSize,
    search,
    fieldFilters,
    rangeFilters,
    sortField,
    sortOrder,
    setEntries,
    setTimeSeries,
    setTotalCount,
    setLoading,
    setError,
  ]);

  const loadPartLogData = useCallback(async () => {
    setPartLogLoading(true);

    try {
      const [entries, total] = await Promise.all([
        fetchPartLog(timeRange, partLogSortField, partLogSortOrder),
        fetchPartLogCount(timeRange),
      ]);

      setPartLogEntries(entries);
      setPartLogTotalCount(total);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load part log data');
    } finally {
      setPartLogLoading(false);
    }
  }, [
    timeRange,
    partLogSortField,
    partLogSortOrder,
    setPartLogEntries,
    setPartLogTotalCount,
    setPartLogLoading,
    setError,
  ]);

  useEffect(() => {
    loadData();
  }, [loadData]);

  // Load part log data on startup
  useEffect(() => {
    loadPartLogData();
  }, [loadPartLogData]);

  const refresh = useCallback(() => {
    loadData();
    loadPartLogData();
  }, [loadData, loadPartLogData]);

  return { refresh };
}
