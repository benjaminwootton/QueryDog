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
  fetchPartLogTimeSeries,
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
    pageSize,
    currentPage,
    partLogSortField,
    partLogSortOrder,
    partLogFieldFilters,
    partLogPageSize,
    partLogCurrentPage,
    setEntries,
    setTimeSeries,
    setTotalCount,
    setLoading,
    setError,
    setColumns,
    setPartLogEntries,
    setPartLogTimeSeries,
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

    const offset = currentPage * pageSize;

    try {
      const [entries, timeSeries, total] = await Promise.all([
        fetchQueryLog(timeRange, search, sortField, sortOrder, fieldFilters, rangeFilters, pageSize, offset),
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
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [
    timeRange.start.getTime(),
    timeRange.end.getTime(),
    bucketSize,
    search,
    fieldFilters,
    rangeFilters,
    sortField,
    sortOrder,
    pageSize,
    currentPage,
    setEntries,
    setTimeSeries,
    setTotalCount,
    setLoading,
    setError,
  ]);

  const loadPartLogData = useCallback(async () => {
    setPartLogLoading(true);

    const offset = partLogCurrentPage * partLogPageSize;

    try {
      const [entries, timeSeries, total] = await Promise.all([
        fetchPartLog(timeRange, partLogSortField, partLogSortOrder, partLogFieldFilters, partLogPageSize, offset),
        fetchPartLogTimeSeries(timeRange, bucketSize, partLogFieldFilters),
        fetchPartLogCount(timeRange, partLogFieldFilters),
      ]);

      setPartLogEntries(entries);
      setPartLogTimeSeries(timeSeries);
      setPartLogTotalCount(total);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load part log data');
    } finally {
      setPartLogLoading(false);
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [
    timeRange.start.getTime(),
    timeRange.end.getTime(),
    bucketSize,
    partLogSortField,
    partLogSortOrder,
    partLogFieldFilters,
    partLogPageSize,
    partLogCurrentPage,
    setPartLogEntries,
    setPartLogTimeSeries,
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
