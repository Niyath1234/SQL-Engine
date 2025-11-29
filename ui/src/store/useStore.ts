import { create } from 'zustand';
import { persist } from 'zustand/middleware';
import type { Tab, QueryHistoryItem, SavedQuery } from '../types';

interface AppState {
  // Tabs
  tabs: Tab[];
  activeTabId: string | null;
  addTab: (tab?: Partial<Tab>) => string;
  closeTab: (id: string) => void;
  updateTab: (id: string, updates: Partial<Tab>) => void;
  setActiveTab: (id: string) => void;
  
  // Query History
  history: QueryHistoryItem[];
  addToHistory: (item: Omit<QueryHistoryItem, 'id'>) => void;
  clearHistory: () => void;
  
  // Saved Queries
  savedQueries: SavedQuery[];
  setSavedQueries: (queries: SavedQuery[]) => void;
  
  // Schema
  schema: any[];
  setSchema: (schema: any[]) => void;
  
  // Theme
  theme: 'light' | 'dark';
  toggleTheme: () => void;
  
  // Layout
  sidebarWidth: number;
  setSidebarWidth: (width: number) => void;
  editorHeight: number;
  setEditorHeight: (height: number) => void;
}

export const useStore = create<AppState>()(
  persist(
    (set) => ({
      // Tabs
      tabs: [{ id: '1', title: 'Query 1', sql: '' }],
      activeTabId: '1',
      addTab: (tab) => {
        const id = `tab-${Date.now()}`;
        set((state) => ({
          tabs: [...state.tabs, { id, title: tab?.title || `Query ${state.tabs.length + 1}`, sql: tab?.sql || '' }],
          activeTabId: id,
        }));
        return id;
      },
      closeTab: (id) => {
        set((state) => {
          const newTabs = state.tabs.filter((t) => t.id !== id);
          if (newTabs.length === 0) {
            return {
              tabs: [{ id: '1', title: 'Query 1', sql: '' }],
              activeTabId: '1',
            };
          }
          const newActiveId = state.activeTabId === id 
            ? (newTabs[0]?.id || null)
            : state.activeTabId;
          return {
            tabs: newTabs,
            activeTabId: newActiveId,
          };
        });
      },
      updateTab: (id, updates) => {
        set((state) => ({
          tabs: state.tabs.map((t) => (t.id === id ? { ...t, ...updates } : t)),
        }));
      },
      setActiveTab: (id) => {
        set({ activeTabId: id });
      },
      
      // Query History
      history: [],
      addToHistory: (item) => {
        const newItem: QueryHistoryItem = {
          ...item,
          id: `hist-${Date.now()}-${Math.random()}`,
        };
        set((state) => ({
          history: [newItem, ...state.history].slice(0, 100), // Keep last 100
        }));
      },
      clearHistory: () => {
        set({ history: [] });
      },
      
      // Saved Queries
      savedQueries: [],
      setSavedQueries: (queries) => {
        set({ savedQueries: queries });
      },
      
      // Schema
      schema: [],
      setSchema: (schema) => {
        set({ schema });
      },
      
      // Theme
      theme: 'light',
      toggleTheme: () => {
        set((state) => ({ theme: state.theme === 'light' ? 'dark' : 'light' }));
      },
      
      // Layout
      sidebarWidth: 250,
      setSidebarWidth: (width) => {
        set({ sidebarWidth: width });
      },
      editorHeight: 50,
      setEditorHeight: (height) => {
        set({ editorHeight: height });
      },
    }),
    {
      name: 'sql-engine-storage',
      partialize: (state) => ({
        theme: state.theme,
        sidebarWidth: state.sidebarWidth,
        editorHeight: state.editorHeight,
        savedQueries: state.savedQueries,
        history: state.history.slice(0, 50), // Only persist last 50
      }),
    }
  )
);

