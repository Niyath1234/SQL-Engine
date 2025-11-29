import { useState } from 'react';
import {
  Box,
  List,
  ListItem,
  ListItemButton,
  ListItemText,
  Collapse,
  Typography,
  Divider,
  IconButton,
  TextField,
  InputAdornment,
} from '@mui/material';
import {
  ExpandLess,
  ExpandMore,
  TableChart,
  Search,
  Refresh,
} from '@mui/icons-material';
import type { TableSchema } from '../types';

interface SchemaSidebarProps {
  schema: TableSchema[];
  onTableClick: (tableName: string) => void;
  onRefresh: () => void;
  theme?: 'light' | 'dark';
}

export function SchemaSidebar({ schema, onTableClick, onRefresh, theme = 'light' }: SchemaSidebarProps) {
  const [expandedTables, setExpandedTables] = useState<Set<string>>(new Set());
  const [searchQuery, setSearchQuery] = useState('');

  const toggleTable = (tableName: string) => {
    const newExpanded = new Set(expandedTables);
    if (newExpanded.has(tableName)) {
      newExpanded.delete(tableName);
    } else {
      newExpanded.add(tableName);
    }
    setExpandedTables(newExpanded);
  };

  const filteredSchema = schema.filter(
    (table) =>
      table.name.toLowerCase().includes(searchQuery.toLowerCase()) ||
      table.columns.some((col) => col.name.toLowerCase().includes(searchQuery.toLowerCase()))
  );

  return (
    <Box
      sx={{
        width: '100%',
        height: '100%',
        display: 'flex',
        flexDirection: 'column',
        bgcolor: theme === 'dark' ? 'grey.900' : 'grey.50',
        borderRight: 1,
        borderColor: 'divider',
      }}
    >
      <Box sx={{ p: 2, borderBottom: 1, borderColor: 'divider' }}>
        <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 1 }}>
          <Typography variant="h6" sx={{ fontSize: '1rem', fontWeight: 600 }}>
            Schema Explorer
          </Typography>
          <IconButton size="small" onClick={onRefresh} title="Refresh schema">
            <Refresh fontSize="small" />
          </IconButton>
        </Box>
        <TextField
          fullWidth
          size="small"
          placeholder="Search tables..."
          value={searchQuery}
          onChange={(e) => setSearchQuery(e.target.value)}
          InputProps={{
            startAdornment: (
              <InputAdornment position="start">
                <Search fontSize="small" />
              </InputAdornment>
            ),
          }}
          sx={{ mt: 1 }}
        />
      </Box>

      <Box sx={{ flex: 1, overflow: 'auto' }}>
        {filteredSchema.length === 0 ? (
          <Box sx={{ p: 2, textAlign: 'center', color: 'text.secondary' }}>
            <Typography variant="body2">
              {searchQuery ? 'No tables found' : 'No tables available'}
            </Typography>
          </Box>
        ) : (
          <List dense>
            {filteredSchema.map((table) => (
              <Box key={table.name}>
                <ListItem disablePadding>
                  <ListItemButton
                    onClick={() => toggleTable(table.name)}
                    sx={{ py: 0.5 }}
                  >
                    <TableChart sx={{ mr: 1, fontSize: 18, color: 'primary.main' }} />
                    <ListItemText
                      primary={table.name}
                      secondary={`${table.columns.length} columns`}
                      primaryTypographyProps={{ variant: 'body2', fontWeight: 500 }}
                      secondaryTypographyProps={{ variant: 'caption' }}
                    />
                    {expandedTables.has(table.name) ? <ExpandLess /> : <ExpandMore />}
                  </ListItemButton>
                </ListItem>
                <Collapse in={expandedTables.has(table.name)} timeout="auto" unmountOnExit>
                  <List component="div" disablePadding dense>
                    {table.columns.map((column) => (
                      <ListItemButton
                        key={column.name}
                        sx={{ pl: 4, py: 0.25 }}
                        onClick={() => onTableClick(table.name)}
                      >
                        <ListItemText
                          primary={column.name}
                          secondary={column.data_type}
                          primaryTypographyProps={{ variant: 'caption' }}
                          secondaryTypographyProps={{ variant: 'caption', sx: { fontSize: '0.7rem' } }}
                        />
                      </ListItemButton>
                    ))}
                  </List>
                </Collapse>
                <Divider />
              </Box>
            ))}
          </List>
        )}
      </Box>
    </Box>
  );
}

