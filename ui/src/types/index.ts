export interface QueryResult {
  success: boolean;
  rows: string[][];
  columns: string[];
  row_count: number;
  execution_time_ms: number;
  error?: string;
}

export interface TableSchema {
  name: string;
  columns: ColumnInfo[];
}

export interface ColumnInfo {
  name: string;
  data_type: string;
}

export interface SavedQuery {
  id: string;
  title: string;
  sql: string;
  folder?: string;
  created_at?: string;
  updated_at?: string;
}

export interface QueryHistoryItem {
  id: string;
  sql: string;
  timestamp: number;
  execution_time_ms?: number;
  row_count?: number;
  success?: boolean;
}

export interface Tab {
  id: string;
  title: string;
  sql: string;
  result?: QueryResult;
  isExecuting?: boolean;
  error?: string;
}

