import axios from 'axios';
import type { QueryResult, TableSchema, SavedQuery } from '../types';

const API_BASE = '/api';

const api = axios.create({
  baseURL: API_BASE,
  timeout: 120000, // 2 minutes for long queries
});

export async function runQuery(sql: string): Promise<QueryResult> {
  try {
    const response = await api.post<QueryResult>('/execute', { sql });
    return response.data;
  } catch (error: any) {
    if (error.response) {
      // Server responded with error
      const errorData = error.response.data;
      return {
        success: false,
        rows: [],
        columns: [],
        row_count: 0,
        execution_time_ms: 0,
        error: errorData?.error || errorData?.message || 'Query execution failed',
      };
    } else if (error.request) {
      // Request made but no response
      return {
        success: false,
        rows: [],
        columns: [],
        row_count: 0,
        execution_time_ms: 0,
        error: 'Network error: Could not connect to server',
      };
    } else {
      // Error setting up request
      return {
        success: false,
        rows: [],
        columns: [],
        row_count: 0,
        execution_time_ms: 0,
        error: error.message || 'Unknown error occurred',
      };
    }
  }
}

export async function getSchema(): Promise<TableSchema[]> {
  try {
    const response = await api.get<{ tables: string[] }>('/tables');
    const tableNames = response.data.tables || [];
    
    const schemas: TableSchema[] = [];
    for (const tableName of tableNames) {
      try {
        const tableResponse = await api.get(`/table/${tableName}`);
        if (tableResponse.data && tableResponse.data.name) {
          schemas.push({
            name: tableResponse.data.name,
            columns: (tableResponse.data.columns || []).map((col: any) => ({
              name: col.name || col,
              data_type: col.data_type || col.type || 'VARCHAR',
            })),
          });
        }
      } catch (e) {
        console.error(`Failed to load schema for ${tableName}:`, e);
      }
    }
    
    return schemas;
  } catch (error: any) {
    console.error('Failed to load schema:', error);
    return [];
  }
}

export async function getSavedQueries(): Promise<SavedQuery[]> {
  try {
    const response = await api.get<SavedQuery[]>('/saved');
    return response.data || [];
  } catch (error) {
    console.error('Failed to load saved queries:', error);
    return [];
  }
}

export async function saveQuery(query: Omit<SavedQuery, 'id'>): Promise<SavedQuery> {
  const response = await api.post<SavedQuery>('/saved', query);
  return response.data;
}

export async function updateQuery(id: string, query: Partial<SavedQuery>): Promise<SavedQuery> {
  const response = await api.put<SavedQuery>(`/saved/${id}`, query);
  return response.data;
}

export async function deleteQuery(id: string): Promise<void> {
  await api.delete(`/saved/${id}`);
}

export async function getHealth(): Promise<{ status: string }> {
  const response = await api.get<{ status: string }>('/health');
  return response.data;
}

