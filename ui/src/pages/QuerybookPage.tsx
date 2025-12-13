import { useState, useEffect, useRef } from 'react';
import { Box, Tabs, Tab, IconButton, Tooltip, Button, Divider, Typography, ToggleButton, ToggleButtonGroup } from '@mui/material';
import {
  PlayArrow,
  Add,
  Close,
  Save,
  Chat,
  Code,
  Menu,
  Storage,
  TableChart,
} from '@mui/icons-material';
import { useStore } from '../store/useStore';
import { SqlEditor } from '../components/SqlEditor';
import { ResultGrid } from '../components/ResultGrid';
import { SchemaSidebar } from '../components/SchemaSidebar';
import { QueryHistory } from '../components/QueryHistory';
import { SavedQueries } from '../components/SavedQueries';
import { ChatInterface } from '../components/ChatInterface';
import { DataIngestionPanel } from '../components/DataIngestionPanel';
import { runQuery, getSchema, getSavedQueries, saveQuery, updateQuery, deleteQuery } from '../api/client';
import { ThemeProvider, createTheme, CssBaseline } from '@mui/material';
import type { QueryResult } from '../types';

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
    sidebarWidth,
    setSidebarWidth,
    editorHeight,
    setEditorHeight,
  } = useStore();

  const [viewMode, setViewMode] = useState<'sql' | 'chat'>('chat');
  const [leftPanel, setLeftPanel] = useState<'schema' | 'history' | 'saved' | 'ingestion'>('schema');
  const [leftSidebarOpen, setLeftSidebarOpen] = useState(true);
  const [isResizing, setIsResizing] = useState(false);
  const [isResizingVertical, setIsResizingVertical] = useState(false);
  const resizeRef = useRef<HTMLDivElement>(null);
  const verticalResizeRef = useRef<HTMLDivElement>(null);

  const activeTab = tabs.find((t) => t.id === activeTabId) || tabs[0];

  // VSCode/Cursor-like dark theme
  const muiTheme = createTheme({
    palette: {
      mode: 'dark', // Force dark theme like VSCode/Cursor
      primary: { main: '#007ACC' }, // VSCode blue
      background: {
        default: '#1e1e1e', // VSCode dark background
        paper: '#252526', // VSCode panel background
      },
      text: {
        primary: '#cccccc', // VSCode text color
        secondary: '#858585',
      },
      divider: '#3e3e42', // VSCode divider
    },
    components: {
      MuiPaper: {
        styleOverrides: {
          root: {
            backgroundColor: '#252526',
          },
        },
      },
      MuiButton: {
        styleOverrides: {
          root: {
            textTransform: 'none',
            borderRadius: '2px',
          },
        },
      },
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

  const handleChatResult = (result: QueryResult) => {
    if (activeTab) {
      updateTab(activeTab.id, {
        result,
        isExecuting: false,
      });
    }
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


  const handleIngestionComplete = () => {
    loadSchema();
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
      <Box sx={{ height: '100vh', display: 'flex', flexDirection: 'column', overflow: 'hidden', bgcolor: '#1e1e1e' }}>
        {/* Top Bar (VSCode style) */}
        <Box
          sx={{
            height: 35,
            borderBottom: 1,
            borderColor: '#3e3e42',
            display: 'flex',
            alignItems: 'center',
            px: 1,
            gap: 1,
            bgcolor: '#2d2d30',
          }}
        >
          <IconButton
            size="small"
            onClick={() => setLeftSidebarOpen(!leftSidebarOpen)}
            sx={{ color: '#cccccc', '&:hover': { bgcolor: '#3e3e42' } }}
          >
            <Menu fontSize="small" />
          </IconButton>
          <Divider orientation="vertical" flexItem sx={{ bgcolor: '#3e3e42' }} />
          
          <Typography variant="body2" sx={{ fontSize: '0.85rem', fontWeight: 500, color: '#cccccc', mr: 2 }}>
            DA_Cursor
          </Typography>
          
          {/* View Mode Toggle */}
          <ToggleButtonGroup
            value={viewMode}
            exclusive
            onChange={(_, newMode) => newMode && setViewMode(newMode)}
            size="small"
            sx={{
              '& .MuiToggleButton-root': {
                px: 1.5,
                py: 0.5,
                fontSize: '0.75rem',
                color: '#cccccc',
                borderColor: '#3e3e42',
                '&.Mui-selected': {
                  bgcolor: '#007ACC',
                  color: 'white',
                  '&:hover': {
                    bgcolor: '#005a9e',
                  },
                },
              },
            }}
          >
            <ToggleButton value="chat">
              <Chat fontSize="small" sx={{ mr: 0.5, fontSize: '0.875rem' }} />
              Chat
            </ToggleButton>
            <ToggleButton value="sql">
              <Code fontSize="small" sx={{ mr: 0.5, fontSize: '0.875rem' }} />
              SQL
            </ToggleButton>
          </ToggleButtonGroup>

          {viewMode === 'sql' && (
            <>
              <Divider orientation="vertical" flexItem sx={{ bgcolor: '#3e3e42', mx: 1 }} />
              <Button
                variant="text"
                startIcon={<PlayArrow fontSize="small" />}
                onClick={() => handleRunQuery()}
                disabled={!activeTab || activeTab.isExecuting}
                size="small"
                sx={{
                  color: '#cccccc',
                  fontSize: '0.75rem',
                  '&:hover': { bgcolor: '#3e3e42' },
                  '&:disabled': { color: '#858585' },
                }}
              >
                Run
              </Button>
              <Tooltip title="Save Query">
                <IconButton
                  size="small"
                  onClick={handleSaveCurrentQuery}
                  disabled={!activeTab?.sql.trim()}
                  sx={{ color: '#cccccc', '&:hover': { bgcolor: '#3e3e42' } }}
                >
                  <Save fontSize="small" />
                </IconButton>
              </Tooltip>
            </>
          )}
          
          <Box sx={{ flex: 1 }} />
        </Box>

        {/* Main Content */}
        <Box sx={{ flex: 1, display: 'flex', overflow: 'hidden' }}>
          {/* Left Sidebar (VSCode style) */}
          {leftSidebarOpen && (
            <Box
              sx={{
                width: sidebarWidth,
                display: 'flex',
                flexDirection: 'column',
                borderRight: 1,
                borderColor: '#3e3e42',
                bgcolor: '#252526',
              }}
            >
              {/* Sidebar Tabs */}
              <Box sx={{ borderBottom: 1, borderColor: '#3e3e42' }}>
                <Tabs
                  value={leftPanel}
                  onChange={(_, v) => setLeftPanel(v)}
                  variant="fullWidth"
                  sx={{
                    minHeight: 35,
                    '& .MuiTab-root': {
                      minHeight: 35,
                      fontSize: '0.75rem',
                      color: '#858585',
                      '&.Mui-selected': {
                        color: '#cccccc',
                      },
                    },
                    '& .MuiTabs-indicator': {
                      backgroundColor: '#007ACC',
                    },
                  }}
                >
                  <Tab icon={<Storage fontSize="small" />} iconPosition="start" label="Schema" value="schema" />
                  <Tab icon={<TableChart fontSize="small" />} iconPosition="start" label="Ingest" value="ingestion" />
                  <Tab label="History" value="history" />
                  <Tab label="Saved" value="saved" />
                </Tabs>
              </Box>

              {/* Sidebar Content */}
              <Box sx={{ flex: 1, overflow: 'hidden' }}>
                {leftPanel === 'schema' && (
                  <SchemaSidebar schema={schema} onTableClick={handleTableClick} onRefresh={loadSchema} theme="dark" />
                )}
                {leftPanel === 'ingestion' && (
                  <DataIngestionPanel theme="dark" onIngestionComplete={handleIngestionComplete} />
                )}
                {leftPanel === 'history' && (
                  <QueryHistory
                    history={history}
                    onSelect={(sql) => {
                      if (activeTab) updateTab(activeTab.id, { sql });
                    }}
                    onClear={() => {}}
                    theme="dark"
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
                    theme="dark"
                  />
                )}
              </Box>
            </Box>
          )}

          {/* Resize Handle */}
          {leftSidebarOpen && (
            <Box
              ref={resizeRef}
              onMouseDown={() => setIsResizing(true)}
              sx={{
                width: 4,
                cursor: 'col-resize',
                bgcolor: '#1e1e1e',
                '&:hover': { bgcolor: '#007ACC' },
              }}
            />
          )}

          {/* Editor + Results Area */}
          <Box sx={{ flex: 1, display: 'flex', flexDirection: 'column', overflow: 'hidden', bgcolor: '#1e1e1e' }}>
            {viewMode === 'chat' ? (
              // Chat Interface (Full Screen)
              <ChatInterface onResult={handleChatResult} theme="dark" />
            ) : (
              // SQL Editor Mode
              <>
                {/* Tabs (VSCode style) */}
                <Box
                  sx={{
                    borderBottom: 1,
                    borderColor: '#3e3e42',
                    display: 'flex',
                    alignItems: 'center',
                    bgcolor: '#252526',
                    minHeight: 35,
                  }}
                >
                  <Box sx={{ flex: 1, display: 'flex', overflow: 'auto' }}>
                    {tabs.map((tab) => (
                      <Box
                        key={tab.id}
                        onClick={() => setActiveTab(tab.id)}
                        sx={{
                          display: 'flex',
                          alignItems: 'center',
                          px: 2,
                          py: 0.5,
                          cursor: 'pointer',
                          borderBottom: activeTabId === tab.id ? 2 : 0,
                          borderColor: '#007ACC',
                          bgcolor: activeTabId === tab.id ? '#1e1e1e' : 'transparent',
                          '&:hover': { bgcolor: '#2a2d2e' },
                          minWidth: 150,
                          fontSize: '0.8rem',
                        }}
                      >
                        <Typography
                          variant="body2"
                          sx={{
                            flex: 1,
                            fontSize: '0.8rem',
                            color: activeTabId === tab.id ? '#cccccc' : '#858585',
                          }}
                        >
                          {tab.title}
                        </Typography>
                        <IconButton
                          size="small"
                          onClick={(e) => {
                            e.stopPropagation();
                            closeTab(tab.id);
                          }}
                          sx={{
                            ml: 1,
                            p: 0.5,
                            color: '#858585',
                            '&:hover': { bgcolor: '#3e3e42', color: '#cccccc' },
                          }}
                        >
                          <Close fontSize="small" sx={{ fontSize: '0.875rem' }} />
                        </IconButton>
                      </Box>
                    ))}
                  </Box>
                  <IconButton
                    size="small"
                    onClick={() => addTab()}
                    sx={{ mx: 0.5, color: '#cccccc', '&:hover': { bgcolor: '#3e3e42' } }}
                  >
                    <Add fontSize="small" />
                  </IconButton>
                </Box>

                {/* Editor */}
                <Box
                  sx={{
                    height: `${editorHeight}%`,
                    borderBottom: 1,
                    borderColor: '#3e3e42',
                    position: 'relative',
                    bgcolor: '#1e1e1e',
                  }}
                >
                  {activeTab && (
                    <SqlEditor
                      value={activeTab.sql}
                      onChange={(sql) => updateTab(activeTab.id, { sql })}
                      onRun={handleRunQuery}
                      schema={schema}
                      theme="dark"
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
                    bgcolor: '#1e1e1e',
                    '&:hover': { bgcolor: '#007ACC' },
                  }}
                />

                {/* Results */}
                <Box sx={{ flex: 1, overflow: 'hidden', bgcolor: '#1e1e1e' }}>
                  {activeTab?.isExecuting ? (
                    <Box sx={{ p: 3, textAlign: 'center' }}>
                      <Typography sx={{ color: '#cccccc' }}>Executing query...</Typography>
                    </Box>
                  ) : activeTab?.result ? (
                    <ResultGrid result={activeTab.result} theme="dark" />
                  ) : activeTab?.error ? (
                    <Box sx={{ p: 2, bgcolor: '#3a1f1f', color: '#f48771' }}>
                      <Typography variant="h6">Error</Typography>
                      <Typography>{activeTab.error}</Typography>
                    </Box>
                  ) : (
                    <Box sx={{ p: 3, textAlign: 'center', color: '#858585' }}>
                      <Typography>Run a query to see results here</Typography>
                    </Box>
                  )}
                </Box>
              </>
            )}
          </Box>
        </Box>
      </Box>
    </ThemeProvider>
  );
}
