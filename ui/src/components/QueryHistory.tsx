import {
  Box,
  List,
  ListItem,
  ListItemButton,
  Typography,
  IconButton,
  Divider,
  Tooltip,
} from '@mui/material';
import {
  History,
  ClearAll,
} from '@mui/icons-material';
import type { QueryHistoryItem } from '../types';

interface QueryHistoryProps {
  history: QueryHistoryItem[];
  onSelect: (sql: string) => void;
  onClear: () => void;
  theme?: 'light' | 'dark';
}

export function QueryHistory({ history, onSelect, onClear, theme = 'light' }: QueryHistoryProps) {

  const formatTime = (timestamp: number) => {
    const date = new Date(timestamp);
    const now = Date.now();
    const diff = now - timestamp;
    
    if (diff < 60000) return 'Just now';
    if (diff < 3600000) return `${Math.floor(diff / 60000)}m ago`;
    if (diff < 86400000) return `${Math.floor(diff / 3600000)}h ago`;
    return date.toLocaleDateString();
  };

  const truncate = (text: string, maxLength: number) => {
    if (text.length <= maxLength) return text;
    return text.substring(0, maxLength) + '...';
  };

  return (
    <Box
      sx={{
        height: '100%',
        display: 'flex',
        flexDirection: 'column',
        bgcolor: theme === 'dark' ? 'grey.900' : 'grey.50',
        borderRight: 1,
        borderColor: 'divider',
      }}
    >
      <Box sx={{ p: 2, borderBottom: 1, borderColor: 'divider', display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          <History fontSize="small" />
          <Typography variant="h6" sx={{ fontSize: '1rem', fontWeight: 600 }}>
            Query History
          </Typography>
        </Box>
        {history.length > 0 && (
          <Tooltip title="Clear history">
            <IconButton size="small" onClick={onClear}>
              <ClearAll fontSize="small" />
            </IconButton>
          </Tooltip>
        )}
      </Box>

      <Box sx={{ flex: 1, overflow: 'auto' }}>
        {history.length === 0 ? (
          <Box sx={{ p: 2, textAlign: 'center', color: 'text.secondary' }}>
            <Typography variant="body2">No query history</Typography>
          </Box>
        ) : (
          <List dense>
            {history.map((item) => (
              <Box key={item.id}>
                <ListItem disablePadding>
                  <ListItemButton
                    onClick={() => onSelect(item.sql)}
                    sx={{ flexDirection: 'column', alignItems: 'flex-start', py: 1 }}
                  >
                    <Box sx={{ width: '100%', display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 0.5 }}>
                      <Typography variant="caption" color="text.secondary">
                        {formatTime(item.timestamp)}
                      </Typography>
                      {item.execution_time_ms && (
                        <Typography variant="caption" color="text.secondary">
                          {item.execution_time_ms.toFixed(0)}ms
                        </Typography>
                      )}
                    </Box>
                    <Typography
                      variant="body2"
                      sx={{
                        fontFamily: 'monospace',
                        fontSize: '0.75rem',
                        wordBreak: 'break-word',
                        color: item.success === false ? 'error.main' : 'text.primary',
                      }}
                    >
                      {truncate(item.sql, 120)}
                    </Typography>
                    {item.row_count !== undefined && (
                      <Typography variant="caption" color="text.secondary" sx={{ mt: 0.5 }}>
                        {item.row_count.toLocaleString()} rows
                      </Typography>
                    )}
                  </ListItemButton>
                </ListItem>
                <Divider />
              </Box>
            ))}
          </List>
        )}
      </Box>
    </Box>
  );
}

