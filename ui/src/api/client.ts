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

export interface AskLLMRequest {
  intent: string;
  user_id?: string | null;
  ollama_url?: string;
  model?: string;
}

export interface AskLLMResponse {
  status: 'success' | 'validation_failed' | 'execution_failed' | 'schema_out_of_sync';
  entry_id?: string;
  structured_plan?: any;
  sql?: string;
  result?: {
    row_count: number;
    execution_time_ms: number;
  };
  reason?: string;
  suggestions?: string[];
  error?: string;
}

export async function askLLM(request: AskLLMRequest): Promise<AskLLMResponse> {
  try {
    const response = await api.post<AskLLMResponse>('/ask', {
      intent: request.intent,
      user_id: request.user_id || null,
      ollama_url: request.ollama_url || 'http://localhost:11434',
      model: request.model || 'llama3.2', // Default to llama3.2
    });
    // If response has schema_out_of_sync status, return it as-is
    if (response.data.status === 'schema_out_of_sync') {
      return {
        ...response.data,
        reason: response.data.reason || 'Schema is out of sync',
        suggestions: response.data.suggestions || [],
      };
    }
    return response.data;
  } catch (error: any) {
    if (error.response) {
      // Check if it's a schema_out_of_sync error
      if (error.response.data?.status === 'schema_out_of_sync') {
        return {
          status: 'schema_out_of_sync',
          reason: error.response.data?.reason || 'Schema is out of sync',
          suggestions: error.response.data?.suggestions || [],
        };
      }
      return {
        status: 'execution_failed',
        error: error.response.data?.error || error.response.data?.message || 'LLM query failed',
      };
    } else if (error.request) {
      return {
        status: 'execution_failed',
        error: 'Network error: Could not connect to server',
      };
    } else {
      return {
        status: 'execution_failed',
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

export interface ClearAllResponse {
  success: boolean;
  message: string;
  tables_dropped: string[];
  cleared_items: string[];
  error?: string;
}

export async function clearAllData(): Promise<ClearAllResponse> {
  try {
    const response = await api.post<ClearAllResponse>('/clear_all');
    return response.data;
  } catch (error: any) {
    return {
      success: false,
      message: error.response?.data?.message || error.message || 'Failed to clear all data',
      tables_dropped: [],
      cleared_items: [],
      error: error.response?.data?.error || error.message,
    };
  }
}
