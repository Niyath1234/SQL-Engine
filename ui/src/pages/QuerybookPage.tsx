import { useState, useEffect, useRef } from 'react';
import { Box, Tabs, Tab, IconButton, Tooltip, Button, Divider, Typography } from '@mui/material';
import {
  PlayArrow,
  Add,
  Close,
  DarkMode,
  LightMode,
  Save,
  ContentCopy,
} from '@mui/icons-material';
import { useStore } from '../store/useStore';
import { SqlEditor } from '../components/SqlEditor';
import { ResultGrid } from '../components/ResultGrid';
import { SchemaSidebar } from '../components/SchemaSidebar';
import { QueryHistory } from '../components/QueryHistory';
import { SavedQueries } from '../components/SavedQueries';
import { runQuery, getSchema, getSavedQueries, saveQuery, updateQuery, deleteQuery } from '../api/client';
import { ThemeProvider, createTheme, CssBaseline } from '@mui/material';

export function QuerybookPage() {
  const {
    tabs,
    activeTabId,
    addTab,
    closeTab,
    updateTab,
    setActiveTab,
    history,
    addToHistory,
    savedQueries,
    setSavedQueries,
    schema,
    setSchema,
    theme,
    toggleTheme,
    sidebarWidth,
    setSidebarWidth,
    editorHeight,
    setEditorHeight,
  } = useStore();

  const [leftPanel, setLeftPanel] = useState<'schema' | 'history' | 'saved'>('schema');
  const [isResizing, setIsResizing] = useState(false);
  const [isResizingVertical, setIsResizingVertical] = useState(false);
  const resizeRef = useRef<HTMLDivElement>(null);
  const verticalResizeRef = useRef<HTMLDivElement>(null);

  const activeTab = tabs.find((t) => t.id === activeTabId) || tabs[0];

  const muiTheme = createTheme({
    palette: {
      mode: theme,
      primary: { main: '#1976d2' },
    },
  });

  useEffect(() => {
    loadSchema();
    loadSavedQueries();
  }, []);

  const loadSchema = async () => {
    const s = await getSchema();
    setSchema(s);
  };

  const loadSavedQueries = async () => {
    const queries = await getSavedQueries();
    setSavedQueries(queries);
  };

  const handleRunQuery = async (sql?: string) => {
    if (!activeTab) return;

    // Use provided SQL (from selection) or fall back to full tab SQL
    const querySql = sql || activeTab.sql;
    if (!querySql.trim()) return;

    updateTab(activeTab.id, { isExecuting: true, error: undefined });
    const result = await runQuery(querySql);

    updateTab(activeTab.id, {
      result,
      isExecuting: false,
      error: result.error,
    });

    addToHistory({
      sql: querySql,
      timestamp: Date.now(),
      execution_time_ms: result.execution_time_ms,
      row_count: result.row_count,
      success: result.success,
    });
  };

  const handleTableClick = (tableName: string) => {
    if (!activeTab) return;
    const newSql = `SELECT * FROM ${tableName} LIMIT 100;`;
    updateTab(activeTab.id, { sql: newSql });
  };

  const handleSaveQuery = async (query: Omit<import('../types').SavedQuery, 'id'>) => {
    await saveQuery(query);
    await loadSavedQueries();
  };

  const handleUpdateQuery = async (id: string, updates: Partial<import('../types').SavedQuery>) => {
    await updateQuery(id, updates);
    await loadSavedQueries();
  };

  const handleDeleteQuery = async (id: string) => {
    await deleteQuery(id);
    await loadSavedQueries();
  };

  const handleSaveCurrentQuery = async () => {
    if (!activeTab || !activeTab.sql.trim()) return;
    const title = prompt('Enter query name:');
    if (!title) return;
    await handleSaveQuery({ title, sql: activeTab.sql, folder: undefined });
  };

  const handleDuplicateTab = () => {
    if (!activeTab) return;
    addTab({ sql: activeTab.sql, title: `${activeTab.title} (Copy)` });
  };

  useEffect(() => {
    const handleMouseMove = (e: MouseEvent) => {
      if (isResizing && resizeRef.current) {
        const newWidth = e.clientX;
        if (newWidth >= 200 && newWidth <= 600) {
          setSidebarWidth(newWidth);
        }
      }
      if (isResizingVertical && verticalResizeRef.current) {
        const newHeight = ((window.innerHeight - e.clientY) / window.innerHeight) * 100;
        if (newHeight >= 20 && newHeight <= 80) {
          setEditorHeight(newHeight);
        }
      }
    };

    const handleMouseUp = () => {
      setIsResizing(false);
      setIsResizingVertical(false);
    };

    if (isResizing || isResizingVertical) {
      document.addEventListener('mousemove', handleMouseMove);
      document.addEventListener('mouseup', handleMouseUp);
      return () => {
        document.removeEventListener('mousemove', handleMouseMove);
        document.removeEventListener('mouseup', handleMouseUp);
      };
    }
  }, [isResizing, isResizingVertical, setSidebarWidth, setEditorHeight]);

  return (
    <ThemeProvider theme={muiTheme}>
      <CssBaseline />
      <Box sx={{ height: '100vh', display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
        {/* Header */}
        <Box
          sx={{
            height: 48,
            borderBottom: 1,
            borderColor: 'divider',
            display: 'flex',
            alignItems: 'center',
            px: 2,
            gap: 1,
            bgcolor: 'background.paper',
          }}
        >
          <Typography variant="h6" sx={{ fontSize: '1.1rem', fontWeight: 600, mr: 2 }}>
            SQL Engine
          </Typography>
          <Divider orientation="vertical" flexItem />
          <Button
            variant="contained"
            startIcon={<PlayArrow />}
            onClick={() => handleRunQuery()}
            disabled={!activeTab || activeTab.isExecuting}
            size="small"
            title="Run Query (Ctrl/Cmd+Enter or Shift+Enter)"
          >
            Run Query
          </Button>
          <Tooltip title="Save Query (Ctrl+S)">
            <IconButton size="small" onClick={handleSaveCurrentQuery} disabled={!activeTab?.sql.trim()}>
              <Save />
            </IconButton>
          </Tooltip>
          <Tooltip title="Duplicate Tab (Ctrl+D)">
            <IconButton size="small" onClick={handleDuplicateTab}>
              <ContentCopy fontSize="small" />
            </IconButton>
          </Tooltip>
          <Box sx={{ flex: 1 }} />
          <Tooltip title={`Switch to ${theme === 'light' ? 'dark' : 'light'} mode`}>
            <IconButton size="small" onClick={toggleTheme}>
              {theme === 'light' ? <DarkMode /> : <LightMode />}
            </IconButton>
          </Tooltip>
        </Box>

        {/* Main Content */}
        <Box sx={{ flex: 1, display: 'flex', overflow: 'hidden' }}>
          {/* Left Sidebar */}
          <Box sx={{ width: sidebarWidth, display: 'flex', flexDirection: 'column', borderRight: 1, borderColor: 'divider' }}>
            <Tabs value={leftPanel} onChange={(_, v) => setLeftPanel(v)} variant="fullWidth" sx={{ borderBottom: 1, borderColor: 'divider' }}>
              <Tab label="Schema" value="schema" />
              <Tab label="History" value="history" />
              <Tab label="Saved" value="saved" />
            </Tabs>
            <Box sx={{ flex: 1, overflow: 'hidden' }}>
              {leftPanel === 'schema' && (
                <SchemaSidebar schema={schema} onTableClick={handleTableClick} onRefresh={loadSchema} theme={theme} />
              )}
              {leftPanel === 'history' && (
                <QueryHistory
                  history={history}
                  onSelect={(sql) => {
                    if (activeTab) updateTab(activeTab.id, { sql });
                  }}
                  onClear={() => {}}
                  theme={theme}
                />
              )}
              {leftPanel === 'saved' && (
                <SavedQueries
                  queries={savedQueries}
                  onSelect={(sql) => {
                    if (activeTab) updateTab(activeTab.id, { sql });
                  }}
                  onSave={async (query) => {
                    await handleSaveQuery(query);
                    await loadSavedQueries();
                  }}
                  onUpdate={async (id, updates) => {
                    await handleUpdateQuery(id, updates);
                    await loadSavedQueries();
                  }}
                  onDelete={async (id) => {
                    await handleDeleteQuery(id);
                    await loadSavedQueries();
                  }}
                  theme={theme}
                />
              )}
            </Box>
          </Box>

          {/* Resize Handle */}
          <Box
            ref={resizeRef}
            onMouseDown={() => setIsResizing(true)}
            sx={{
              width: 4,
              cursor: 'col-resize',
              bgcolor: 'divider',
              '&:hover': { bgcolor: 'primary.main' },
            }}
          />

          {/* Editor + Results Area */}
          <Box sx={{ flex: 1, display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
            {/* Tabs */}
            <Box sx={{ borderBottom: 1, borderColor: 'divider', display: 'flex', alignItems: 'center', bgcolor: 'background.paper' }}>
              <Box sx={{ flex: 1, display: 'flex', overflow: 'auto' }}>
                {tabs.map((tab) => (
                  <Box
                    key={tab.id}
                    onClick={() => setActiveTab(tab.id)}
                    sx={{
                      display: 'flex',
                      alignItems: 'center',
                      px: 2,
                      py: 1,
                      cursor: 'pointer',
                      borderBottom: activeTabId === tab.id ? 2 : 0,
                      borderColor: 'primary.main',
                      bgcolor: activeTabId === tab.id ? 'action.selected' : 'transparent',
                      '&:hover': { bgcolor: 'action.hover' },
                      minWidth: 150,
                    }}
                  >
                    <Typography variant="body2" sx={{ flex: 1, fontSize: '0.875rem' }}>
                      {tab.title}
                    </Typography>
                    <IconButton
                      size="small"
                      onClick={(e) => {
                        e.stopPropagation();
                        closeTab(tab.id);
                      }}
                      sx={{ ml: 1, '&:hover': { bgcolor: 'error.light', color: 'error.main' } }}
                    >
                      <Close fontSize="small" />
                    </IconButton>
                  </Box>
                ))}
              </Box>
              <IconButton size="small" onClick={() => addTab()} sx={{ mx: 1 }}>
                <Add />
              </IconButton>
            </Box>

            {/* Editor */}
            <Box
              sx={{
                height: `${editorHeight}%`,
                borderBottom: 1,
                borderColor: 'divider',
                position: 'relative',
              }}
            >
              {activeTab && (
                <SqlEditor
                  value={activeTab.sql}
                  onChange={(sql) => updateTab(activeTab.id, { sql })}
                  onRun={handleRunQuery}
                  schema={schema}
                  theme={theme}
                />
              )}
            </Box>

            {/* Vertical Resize Handle */}
            <Box
              ref={verticalResizeRef}
              onMouseDown={() => setIsResizingVertical(true)}
              sx={{
                height: 4,
                cursor: 'row-resize',
                bgcolor: 'divider',
                '&:hover': { bgcolor: 'primary.main' },
              }}
            />

            {/* Results */}
            <Box sx={{ flex: 1, overflow: 'hidden', bgcolor: 'background.paper' }}>
              {activeTab?.isExecuting ? (
                <Box sx={{ p: 3, textAlign: 'center' }}>
                  <Typography>Executing query...</Typography>
                </Box>
              ) : activeTab?.result ? (
                <ResultGrid result={activeTab.result} theme={theme} />
              ) : activeTab?.error ? (
                <Box sx={{ p: 2, bgcolor: 'error.light', color: 'error.contrastText' }}>
                  <Typography variant="h6">Error</Typography>
                  <Typography>{activeTab.error}</Typography>
                </Box>
              ) : (
                <Box sx={{ p: 3, textAlign: 'center', color: 'text.secondary' }}>
                  <Typography>Run a query to see results here</Typography>
                </Box>
              )}
            </Box>
          </Box>
        </Box>
      </Box>
    </ThemeProvider>
  );
}

