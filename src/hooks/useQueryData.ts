import { useEffect, useCallback, useRef } from 'react';
import { useQueryStore } from '../stores/queryStore';
import {
  fetchQueryLog,
  fetchTimeSeries,
  fetchStackedTimeSeries,
  fetchTotalCount,
  fetchColumnMetadata,
  fetchPartLog,
  fetchPartLogCount,
  fetchPartLogColumnMetadata,
  fetchPartLogTimeSeries,
  fetchPartLogStackedTimeSeries,
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
    chartType,
    partLogSortField,
    partLogSortOrder,
    partLogFieldFilters,
    partLogPageSize,
    partLogCurrentPage,
    setEntries,
    setTimeSeries,
    setStackedTimeSeries,
    setTotalCount,
    setLoading,
    setError,
    setColumns,
    setPartLogEntries,
    setPartLogTimeSeries,
    setPartLogStackedTimeSeries,
    setPartLogTotalCount,
    setPartLogLoading,
    setHasPartLogAccess,
    setPartLogColumns,
  } = useQueryStore();

  const columnsLoadedRef = useRef(false);
  const partLogColumnsLoadedRef = useRef(false);
  const queryRequestIdRef = useRef(0);
  const partLogRequestIdRef = useRef(0);

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
    const requestId = ++queryRequestIdRef.current;
    setLoading(true);
    setError(null);

    // Get latest state from store to ensure we have the most recent time range
    const state = useQueryStore.getState();
    const currentTimeRange = state.timeRange;

    const offset = currentPage * pageSize;

    try {
      const results = await Promise.allSettled([
        fetchQueryLog(currentTimeRange, search, sortField, sortOrder, fieldFilters, rangeFilters, pageSize, offset, bucketSize),
        fetchTimeSeries(currentTimeRange, bucketSize, search, fieldFilters, rangeFilters),
        fetchStackedTimeSeries(currentTimeRange, bucketSize, search, fieldFilters, rangeFilters),
        fetchTotalCount(currentTimeRange, search, fieldFilters, rangeFilters, bucketSize),
      ]);

      // Process each result individually - set data if successful, show error if failed
      let hasAnyData = false;
      let errors: string[] = [];

      if (requestId !== queryRequestIdRef.current) return;

      if (results[0].status === 'fulfilled') {
        setEntries(results[0].value);
        hasAnyData = true;
      } else {
        console.error('Failed to load query log entries:', results[0].reason);
        errors.push(`Entries: ${results[0].reason.message}`);
        setEntries([]);
      }

      if (results[1].status === 'fulfilled') {
        setTimeSeries(results[1].value);
        hasAnyData = true;
      } else {
        console.error('Failed to load time series:', results[1].reason);
        errors.push(`Time series: ${results[1].reason.message}`);
        setTimeSeries([]);
      }

      if (results[2].status === 'fulfilled') {
        setStackedTimeSeries(results[2].value);
        hasAnyData = true;
      } else {
        console.error('Failed to load stacked time series:', results[2].reason);
        errors.push(`Stacked series: ${results[2].reason.message}`);
        setStackedTimeSeries([]);
      }

      if (results[3].status === 'fulfilled') {
        setTotalCount(results[3].value);
        hasAnyData = true;
      } else {
        console.error('Failed to load count:', results[3].reason);
        errors.push(`Count: ${results[3].reason.message}`);
        setTotalCount(0);
      }

      // Check for errors and categorize them
      if (errors.length > 0) {
        // Find the first error with a specific type (authentication, permission, not_found)
        const rejectedResult = results.find(r =>
          r.status === 'rejected' &&
          ['authentication', 'permission', 'not_found'].includes((r as PromiseRejectedResult).reason?.type)
        ) as PromiseRejectedResult | undefined;
        const criticalError: any = rejectedResult?.reason;

        if (criticalError?.type === 'authentication') {
          setError('⚠️ Authentication failed: Invalid username or password for ClickHouse');
        } else if (criticalError?.type === 'permission') {
          setError('⚠️ Access denied: You do not have permission to access system.query_log table');
        } else if (criticalError?.type === 'not_found') {
          setError('⚠️ Table system.query_log does not exist or is not accessible on this ClickHouse instance');
        } else if (!hasAnyData) {
          // All requests failed with generic errors
          setError(`Failed to load data: ${errors[0]}`);
        } else {
          // Some data loaded, but some requests failed - show a warning
          console.warn('Some query log data failed to load:', errors.join('; '));
        }
      }
    } catch (err) {
      if (requestId !== queryRequestIdRef.current) return;
      setError(err instanceof Error ? err.message : 'Failed to load data');
    } finally {
      if (requestId === queryRequestIdRef.current) {
        setLoading(false);
      }
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
    chartType,
    setEntries,
    setTimeSeries,
    setStackedTimeSeries,
    setTotalCount,
    setLoading,
    setError,
  ]);

  const loadPartLogData = useCallback(async () => {
    const requestId = ++partLogRequestIdRef.current;
    setPartLogLoading(true);

    // Get latest state from store to ensure we have the most recent time range
    const state = useQueryStore.getState();
    const currentTimeRange = state.timeRange;

    const offset = partLogCurrentPage * partLogPageSize;

    try {
      const results = await Promise.allSettled([
        fetchPartLog(currentTimeRange, partLogSortField, partLogSortOrder, partLogFieldFilters, partLogPageSize, offset),
        fetchPartLogTimeSeries(currentTimeRange, bucketSize, partLogFieldFilters),
        fetchPartLogStackedTimeSeries(currentTimeRange, bucketSize, partLogFieldFilters),
        fetchPartLogCount(currentTimeRange, partLogFieldFilters),
      ]);

      // Process each result individually - set data if successful, show error if failed
      let hasAnyData = false;
      let errors: string[] = [];

      if (requestId !== partLogRequestIdRef.current) return;

      if (results[0].status === 'fulfilled') {
        setPartLogEntries(results[0].value);
        hasAnyData = true;
      } else {
        console.error('Failed to load part log entries:', results[0].reason);
        errors.push(`Entries: ${results[0].reason.message}`);
        setPartLogEntries([]);
      }

      if (results[1].status === 'fulfilled') {
        setPartLogTimeSeries(results[1].value);
        hasAnyData = true;
      } else {
        console.error('Failed to load part log time series:', results[1].reason);
        errors.push(`Time series: ${results[1].reason.message}`);
        setPartLogTimeSeries([]);
      }

      if (results[2].status === 'fulfilled') {
        setPartLogStackedTimeSeries(results[2].value);
        hasAnyData = true;
      } else {
        console.error('Failed to load part log stacked time series:', results[2].reason);
        errors.push(`Stacked series: ${results[2].reason.message}`);
        setPartLogStackedTimeSeries([]);
      }

      if (results[3].status === 'fulfilled') {
        setPartLogTotalCount(results[3].value);
        hasAnyData = true;
      } else {
        console.error('Failed to load part log count:', results[3].reason);
        errors.push(`Count: ${results[3].reason.message}`);
        setPartLogTotalCount(0);
      }

      // Check for errors and categorize them
      if (errors.length > 0) {
        // Find the first error with a specific type (authentication, permission, not_found)
        const rejectedResult = results.find(r =>
          r.status === 'rejected' &&
          ['authentication', 'permission', 'not_found'].includes((r as PromiseRejectedResult).reason?.type)
        ) as PromiseRejectedResult | undefined;
        const criticalError: any = rejectedResult?.reason;

        if (criticalError?.type === 'permission') {
          // Permission errors are expected - just mark as no access and don't show error
          console.info('Part log access denied - hiding part log features');
          setHasPartLogAccess(false);
        } else if (criticalError?.type === 'authentication') {
          // Authentication errors should be shown
          setError('⚠️ Authentication failed: Invalid username or password for ClickHouse');
          setHasPartLogAccess(false);
        } else if (criticalError?.type === 'not_found') {
          // Table doesn't exist - hide but don't show error
          console.info('Part log table not found - hiding part log features');
          setHasPartLogAccess(false);
        } else if (!hasAnyData) {
          // All requests failed with generic errors
          setError(`Failed to load part log data: ${errors[0]}`);
          setHasPartLogAccess(false);
        } else {
          // Some data loaded, but some requests failed - show a warning
          console.warn('Some part log data failed to load:', errors.join('; '));
        }
      }
    } catch (err) {
      if (requestId !== partLogRequestIdRef.current) return;
      setError(err instanceof Error ? err.message : 'Failed to load part log data');
    } finally {
      if (requestId === partLogRequestIdRef.current) {
        setPartLogLoading(false);
      }
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
    setPartLogStackedTimeSeries,
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

  const { triggerGlobalRefresh, refreshTimeRange } = useQueryStore();

  const refresh = useCallback(() => {
    // Update time range to current time if using a relative range (e.g., "last 15 minutes")
    refreshTimeRange();
    loadData();
    loadPartLogData();
    triggerGlobalRefresh();
  }, [loadData, loadPartLogData, triggerGlobalRefresh, refreshTimeRange]);

  return { refresh };
}
