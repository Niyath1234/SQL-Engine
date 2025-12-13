import { useState, useRef, useEffect } from 'react';
import { Box, TextField, IconButton, Typography, Paper, CircularProgress, Chip, Collapse, Table, TableBody, TableCell, TableContainer, TableHead, TableRow } from '@mui/material';
import { Send, Code, CheckCircle, Error as ErrorIcon, ContentCopy, Visibility, VisibilityOff } from '@mui/icons-material';
import { askLLM, AskLLMResponse } from '../api/client';
import { QueryResult } from '../types';
import axios from 'axios';

interface Message {
  id: string;
  role: 'user' | 'assistant';
  content: string;
  sql?: string;
  result?: QueryResult;
  error?: string;
  status?: 'thinking' | 'success' | 'error';
  execution_time_ms?: number;
  row_count?: number;
  showSql?: boolean; // Whether to show SQL (default false for naive users)
}

interface ChatInterfaceProps {
  onResult?: (result: QueryResult) => void;
  theme?: 'light' | 'dark';
}

export function ChatInterface({ onResult, theme = 'light' }: ChatInterfaceProps) {
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  useEffect(() => {
    // Focus input on mount
    inputRef.current?.focus();
  }, []);

  const handleSend = async () => {
    if (!input.trim() || isLoading) return;

    const userMessage: Message = {
      id: Date.now().toString(),
      role: 'user',
      content: input.trim(),
    };

    setMessages((prev) => [...prev, userMessage]);
    setInput('');
    setIsLoading(true);

    // Add thinking message
    const thinkingMessage: Message = {
      id: (Date.now() + 1).toString(),
      role: 'assistant',
      content: 'Thinking...',
      status: 'thinking',
    };
    setMessages((prev) => [...prev, thinkingMessage]);

    try {
      const response: AskLLMResponse = await askLLM({
        intent: userMessage.content,
        model: 'llama3.2', // Hardcoded to llama3.2
        ollama_url: 'http://localhost:11434',
      });

      // Remove thinking message
      setMessages((prev) => prev.filter((m) => m.id !== thinkingMessage.id));

      if (response.status === 'success' && response.sql && response.result) {
        // Execute the SQL to get full result rows
        let fullResult: QueryResult;
        try {
          const { runQuery } = await import('../api/client');
          fullResult = await runQuery(response.sql);
        } catch (e) {
          // Fallback if execution fails
          fullResult = {
            success: true,
            rows: [],
            columns: [],
            row_count: response.result.row_count,
            execution_time_ms: response.result.execution_time_ms,
          };
        }

        const assistantMessage: Message = {
          id: (Date.now() + 2).toString(),
          role: 'assistant',
          content: `Found ${fullResult.row_count} result${fullResult.row_count !== 1 ? 's' : ''}`,
          sql: response.sql,
          result: fullResult,
          status: 'success',
          execution_time_ms: fullResult.execution_time_ms,
          row_count: fullResult.row_count,
          showSql: false, // Hide SQL by default for naive users
        };

        setMessages((prev) => [...prev, assistantMessage]);
        onResult?.(fullResult);
      } else if (response.status === 'validation_failed') {
        const assistantMessage: Message = {
          id: (Date.now() + 2).toString(),
          role: 'assistant',
          content: `I couldn't generate a valid query. ${response.reason || 'Validation failed'}.${response.suggestions ? `\n\nSuggestions:\n${response.suggestions.map((s) => `- ${s}`).join('\n')}` : ''}`,
          error: response.reason,
          status: 'error',
        };
        setMessages((prev) => [...prev, assistantMessage]);
      } else if (response.status === 'schema_out_of_sync') {
        const assistantMessage: Message = {
          id: (Date.now() + 2).toString(),
          role: 'assistant',
          content: `Schema sync required: ${response.reason || 'Schema is out of sync'}.${response.suggestions ? `\n\nSuggestions:\n${response.suggestions.map((s) => `- ${s}`).join('\n')}` : ''}\n\nI'll try to sync the schema automatically and retry your query...`,
          error: response.reason,
          status: 'error',
        };
        setMessages((prev) => [...prev, assistantMessage]);
        
        // Auto-sync schema and retry query
        try {
          const syncResponse = await axios.post('/api/schema/sync', {}, { headers: { 'Content-Type': 'application/json' } });
          if (syncResponse.data?.status === 'success') {
            const syncingMessage: Message = {
              id: (Date.now() + 3).toString(),
              role: 'assistant',
              content: '✅ Schema synced successfully! Retrying your query...',
              status: 'thinking',
            };
            setMessages((prev) => [...prev, syncingMessage]);
            
            // Retry the original query
            const retryResponse: AskLLMResponse = await askLLM({
              intent: userMessage.content,
              model: 'llama3.2',
              ollama_url: 'http://localhost:11434',
            });
            
            // Remove syncing message
            setMessages((prev) => prev.filter((m) => m.id !== syncingMessage.id));
            
            // Handle retry response
            if (retryResponse.status === 'success' && retryResponse.sql && retryResponse.result) {
              // Execute SQL to get full result rows
              let fullResult: QueryResult;
              try {
                const { runQuery } = await import('../api/client');
                fullResult = await runQuery(retryResponse.sql!);
              } catch (e) {
                fullResult = {
                  success: true,
                  rows: [],
                  columns: [],
                  row_count: retryResponse.result.row_count,
                  execution_time_ms: retryResponse.result.execution_time_ms,
                };
              }

              const retrySuccessMessage: Message = {
                id: (Date.now() + 4).toString(),
                role: 'assistant',
                content: `Found ${fullResult.row_count} result${fullResult.row_count !== 1 ? 's' : ''}`,
                sql: retryResponse.sql,
                result: fullResult,
                status: 'success',
                execution_time_ms: fullResult.execution_time_ms,
                row_count: fullResult.row_count,
                showSql: false,
              };

              setMessages((prev) => [...prev, retrySuccessMessage]);
              onResult?.(fullResult);
            } else {
              const retryErrorMessage: Message = {
                id: (Date.now() + 4).toString(),
                role: 'assistant',
                content: `Schema synced, but query still failed: ${retryResponse.error || retryResponse.reason || 'Unknown error'}`,
                error: retryResponse.error || retryResponse.reason,
                status: 'error',
              };
              setMessages((prev) => [...prev, retryErrorMessage]);
            }
          } else {
            throw new Error('Schema sync returned non-success status');
          }
        } catch (syncError: any) {
          console.error('Schema sync failed:', syncError);
          const errorMessage: Message = {
            id: (Date.now() + 3).toString(),
            role: 'assistant',
            content: `Failed to sync schema: ${syncError.response?.data?.error || syncError.message || 'Unknown error'}. Please try syncing manually via the API.`,
            error: syncError.message,
            status: 'error',
          };
          setMessages((prev) => [...prev, errorMessage]);
        }
      } else {
        const assistantMessage: Message = {
          id: (Date.now() + 2).toString(),
          role: 'assistant',
          content: `Sorry, I encountered an error: ${response.error || response.reason || 'Unknown error'}`,
          error: response.error || response.reason,
          status: 'error',
        };
        setMessages((prev) => [...prev, assistantMessage]);
      }
    } catch (error: any) {
      // Remove thinking message
      setMessages((prev) => prev.filter((m) => m.id !== thinkingMessage.id));

      const errorMessage: Message = {
        id: (Date.now() + 2).toString(),
        role: 'assistant',
        content: `Sorry, I encountered an error: ${error.message || 'Unknown error'}`,
        error: error.message,
        status: 'error',
      };
      setMessages((prev) => [...prev, errorMessage]);
    } finally {
      setIsLoading(false);
    }
  };

  const handleKeyPress = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  };

  const copyToClipboard = (text: string) => {
    navigator.clipboard.writeText(text);
  };

  const toggleSql = (messageId: string) => {
    setMessages((prev) =>
      prev.map((msg) =>
        msg.id === messageId ? { ...msg, showSql: !msg.showSql } : msg
      )
    );
  };

  const renderMessage = (message: Message) => {
    const isUser = message.role === 'user';
    const bgColor = theme === 'dark' 
      ? (isUser ? 'rgba(0, 122, 204, 0.15)' : 'rgba(37, 37, 38, 0.8)')
      : (isUser ? 'rgba(59, 130, 246, 0.1)' : 'rgba(250, 250, 250, 0.8)');

    return (
      <Box
        key={message.id}
        sx={{
          display: 'flex',
          flexDirection: 'column',
          mb: 2,
          alignItems: isUser ? 'flex-end' : 'flex-start',
        }}
      >
        <Paper
          elevation={0}
          sx={{
            p: 2,
            maxWidth: '80%',
            bgcolor: bgColor,
            borderRadius: 2,
            border: `1px solid ${theme === 'dark' ? 'rgba(62, 62, 66, 0.8)' : 'rgba(0, 0, 0, 0.1)'}`,
          }}
        >
          {message.status === 'thinking' ? (
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
              <CircularProgress size={16} />
              <Typography variant="body2" sx={{ fontStyle: 'italic', color: 'text.secondary' }}>
                Thinking...
              </Typography>
            </Box>
          ) : (
            <>
              <Typography
                variant="body2"
                sx={{
                  whiteSpace: 'pre-wrap',
                  wordBreak: 'break-word',
                  fontFamily: isUser ? 'inherit' : 'inherit',
                  fontSize: '0.9rem',
                  lineHeight: 1.6,
                  mb: message.result && message.result.rows.length > 0 ? 2 : 0,
                }}
              >
                {message.content}
              </Typography>

              {/* Results Table - Show by default for naive users */}
              {message.status === 'success' && message.result && message.result.rows.length > 0 && (
                <Box sx={{ mt: 2 }}>
                  <TableContainer 
                    component={Paper} 
                    elevation={0}
                    sx={{
                      maxHeight: 400,
                      overflow: 'auto',
                      bgcolor: theme === 'dark' ? '#252526' : 'rgba(0, 0, 0, 0.02)',
                      border: `1px solid ${theme === 'dark' ? '#3e3e42' : 'rgba(0, 0, 0, 0.1)'}`,
                    }}
                  >
                    <Table size="small" stickyHeader>
                      <TableHead>
                        <TableRow>
                          {message.result.columns.map((col, idx) => (
                            <TableCell
                              key={idx}
                              sx={{
                                bgcolor: theme === 'dark' ? '#2d2d30' : 'rgba(0, 0, 0, 0.05)',
                                fontWeight: 600,
                                fontSize: '0.85rem',
                                color: theme === 'dark' ? '#cccccc' : 'inherit',
                              }}
                            >
                              {col}
                            </TableCell>
                          ))}
                        </TableRow>
                      </TableHead>
                      <TableBody>
                        {message.result.rows.slice(0, 100).map((row, rowIdx) => (
                          <TableRow key={rowIdx} hover>
                            {row.map((cell, cellIdx) => (
                              <TableCell
                                key={cellIdx}
                                sx={{
                                  fontSize: '0.85rem',
                                  color: theme === 'dark' ? '#cccccc' : 'inherit',
                                }}
                              >
                                {String(cell || '')}
                              </TableCell>
                            ))}
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </TableContainer>
                  {message.result.rows.length > 100 && (
                    <Typography variant="caption" sx={{ mt: 1, display: 'block', color: 'text.secondary' }}>
                      Showing first 100 of {message.result.rows.length} rows
                    </Typography>
                  )}
                </Box>
              )}

              {/* SQL Query - Hidden by default, show with icon */}
              {message.sql && (
                <Box sx={{ mt: 2 }}>
                  <Box
                    sx={{
                      display: 'flex',
                      alignItems: 'center',
                      gap: 1,
                      cursor: 'pointer',
                      '&:hover': { opacity: 0.8 },
                    }}
                    onClick={() => toggleSql(message.id)}
                  >
                    <IconButton size="small" sx={{ p: 0.5 }}>
                      {message.showSql ? <VisibilityOff fontSize="small" /> : <Visibility fontSize="small" />}
                    </IconButton>
                    <Chip
                      icon={<Code />}
                      label="View SQL Query"
                      size="small"
                      sx={{ height: 24, fontSize: '0.75rem' }}
                    />
                    <IconButton
                      size="small"
                      onClick={(e) => {
                        e.stopPropagation();
                        copyToClipboard(message.sql!);
                      }}
                      sx={{ p: 0.5, ml: 'auto' }}
                    >
                      <ContentCopy fontSize="small" />
                    </IconButton>
                  </Box>
                  <Collapse in={message.showSql || false}>
                    <Paper
                      elevation={0}
                      sx={{
                        p: 1.5,
                        mt: 1,
                        bgcolor: theme === 'dark' ? '#252526' : 'rgba(0, 0, 0, 0.05)',
                        borderRadius: 1,
                        border: `1px solid ${theme === 'dark' ? '#3e3e42' : 'rgba(0, 0, 0, 0.1)'}`,
                      }}
                    >
                      <Typography
                        variant="body2"
                        component="pre"
                        sx={{
                          fontFamily: 'monospace',
                          fontSize: '0.8rem',
                          margin: 0,
                          whiteSpace: 'pre-wrap',
                          wordBreak: 'break-word',
                        }}
                      >
                        {message.sql}
                      </Typography>
                    </Paper>
                  </Collapse>
                </Box>
              )}

              {/* Execution stats - subtle */}
              {message.status === 'success' && message.row_count !== undefined && (
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mt: 1.5 }}>
                  <CheckCircle sx={{ fontSize: 14, color: 'success.main', opacity: 0.7 }} />
                  <Typography variant="caption" sx={{ color: 'text.secondary', fontSize: '0.75rem' }}>
                    {message.row_count} result{message.row_count !== 1 ? 's' : ''} • {message.execution_time_ms?.toFixed(2)}ms
                  </Typography>
                </Box>
              )}

              {message.status === 'error' && (
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mt: 1.5 }}>
                  <ErrorIcon sx={{ fontSize: 16, color: 'error.main' }} />
                  <Typography variant="caption" sx={{ color: 'error.main' }}>
                    {message.error}
                  </Typography>
                </Box>
              )}
            </>
          )}
        </Paper>
      </Box>
    );
  };

  return (
      <Box
        sx={{
          height: '100%',
          display: 'flex',
          flexDirection: 'column',
          bgcolor: theme === 'dark' ? '#1e1e1e' : 'background.default',
        }}
      >
      {/* Messages Area */}
      <Box
        sx={{
          flex: 1,
          overflow: 'auto',
          p: 2,
          '&::-webkit-scrollbar': {
            width: '8px',
          },
          '&::-webkit-scrollbar-track': {
            background: theme === 'dark' ? 'rgba(255, 255, 255, 0.05)' : 'rgba(0, 0, 0, 0.05)',
          },
          '&::-webkit-scrollbar-thumb': {
            background: theme === 'dark' ? 'rgba(255, 255, 255, 0.2)' : 'rgba(0, 0, 0, 0.2)',
            borderRadius: '4px',
          },
        }}
      >
        {messages.length === 0 ? (
          <Box
            sx={{
              display: 'flex',
              flexDirection: 'column',
              alignItems: 'center',
              justifyContent: 'center',
              height: '100%',
              color: 'text.secondary',
              textAlign: 'center',
            }}
          >
            <Typography variant="h6" sx={{ mb: 1, fontWeight: 500 }}>
              DA_Cursor
            </Typography>
            <Typography variant="body2" sx={{ mb: 3, opacity: 0.7 }}>
              Ask me anything about your data in natural language
            </Typography>
            <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1, maxWidth: 400 }}>
              <Chip
                label="Example: Show me all products with their categories"
                size="small"
                sx={{ justifyContent: 'flex-start', height: 'auto', py: 1 }}
              />
              <Chip
                label="Example: What are the total sales by customer?"
                size="small"
                sx={{ justifyContent: 'flex-start', height: 'auto', py: 1 }}
              />
              <Chip
                label="Example: List orders from the last 30 days"
                size="small"
                sx={{ justifyContent: 'flex-start', height: 'auto', py: 1 }}
              />
            </Box>
            <Typography variant="caption" sx={{ mt: 3, opacity: 0.5 }}>
              Powered by Llama 3.2
            </Typography>
          </Box>
        ) : (
          <>
            {messages.map(renderMessage)}
            <div ref={messagesEndRef} />
          </>
        )}
      </Box>

      {/* Input Area */}
      <Box
        sx={{
          borderTop: `1px solid ${theme === 'dark' ? '#3e3e42' : 'rgba(0, 0, 0, 0.1)'}`,
          p: 2,
          bgcolor: theme === 'dark' ? '#252526' : 'background.paper',
        }}
      >
        <Box sx={{ display: 'flex', gap: 1, alignItems: 'flex-end' }}>
          <TextField
            inputRef={inputRef}
            fullWidth
            multiline
            maxRows={4}
            placeholder="Ask a question about your data..."
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyPress={handleKeyPress}
            disabled={isLoading}
            variant="outlined"
            size="small"
              sx={{
                '& .MuiOutlinedInput-root': {
                  borderRadius: 2,
                  bgcolor: theme === 'dark' ? '#3c3c3c' : 'rgba(0, 0, 0, 0.02)',
                  color: theme === 'dark' ? '#cccccc' : 'inherit',
                  '& fieldset': {
                    borderColor: theme === 'dark' ? '#3e3e42' : 'rgba(0, 0, 0, 0.1)',
                  },
                  '&:hover fieldset': {
                    borderColor: theme === 'dark' ? '#007ACC' : 'rgba(0, 0, 0, 0.2)',
                  },
                  '&.Mui-focused fieldset': {
                    borderColor: '#007ACC',
                  },
                },
              }}
          />
          <IconButton
            color="primary"
            onClick={handleSend}
            disabled={!input.trim() || isLoading}
            sx={{
              bgcolor: 'primary.main',
              color: 'white',
              '&:hover': {
                bgcolor: 'primary.dark',
              },
              '&:disabled': {
                bgcolor: theme === 'dark' ? 'rgba(255, 255, 255, 0.1)' : 'rgba(0, 0, 0, 0.1)',
                color: theme === 'dark' ? 'rgba(255, 255, 255, 0.3)' : 'rgba(0, 0, 0, 0.3)',
              },
            }}
          >
            {isLoading ? <CircularProgress size={20} color="inherit" /> : <Send />}
          </IconButton>
        </Box>
        <Typography variant="caption" sx={{ mt: 1, display: 'block', textAlign: 'center', opacity: 0.5 }}>
          Press Enter to send, Shift+Enter for new line
        </Typography>
      </Box>
    </Box>
  );
}

