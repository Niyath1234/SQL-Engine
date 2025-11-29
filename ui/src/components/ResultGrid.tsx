import { useMemo, useState } from 'react';
import {
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  Box,
  Typography,
  IconButton,
  Tooltip,
} from '@mui/material';
import ContentCopyIcon from '@mui/icons-material/ContentCopy';
import type { QueryResult } from '../types';

interface ResultGridProps {
  result: QueryResult;
  theme?: 'light' | 'dark';
}

export function ResultGrid({ result, theme = 'light' }: ResultGridProps) {
  const [sortColumn, setSortColumn] = useState<string | null>(null);
  const [sortDirection, setSortDirection] = useState<'asc' | 'desc'>('asc');

  const sortedRows = useMemo(() => {
    if (!sortColumn || !result.rows.length) return result.rows;

    const colIndex = result.columns.indexOf(sortColumn);
    if (colIndex === -1) return result.rows;

    return [...result.rows].sort((a, b) => {
      const aVal = a[colIndex] || '';
      const bVal = b[colIndex] || '';
      
      // Try numeric comparison
      const aNum = parseFloat(aVal);
      const bNum = parseFloat(bVal);
      if (!isNaN(aNum) && !isNaN(bNum)) {
        return sortDirection === 'asc' ? aNum - bNum : bNum - aNum;
      }
      
      // String comparison
      const comparison = aVal.localeCompare(bVal);
      return sortDirection === 'asc' ? comparison : -comparison;
    });
  }, [result.rows, result.columns, sortColumn, sortDirection]);

  const handleSort = (column: string) => {
    if (sortColumn === column) {
      setSortDirection(sortDirection === 'asc' ? 'desc' : 'asc');
    } else {
      setSortColumn(column);
      setSortDirection('asc');
    }
  };

  const copyToClipboard = (text: string) => {
    navigator.clipboard.writeText(text);
  };

  if (!result.success) {
    return (
      <Box sx={{ p: 2, bgcolor: 'error.light', color: 'error.contrastText', borderRadius: 1 }}>
        <Typography variant="h6">Error</Typography>
        <Typography>{result.error || 'Query execution failed'}</Typography>
      </Box>
    );
  }

  if (result.rows.length === 0) {
    return (
      <Box sx={{ p: 3, textAlign: 'center', color: 'text.secondary' }}>
        <Typography>Query executed successfully. No rows returned.</Typography>
      </Box>
    );
  }

  return (
    <Box sx={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
      <Box sx={{ p: 1, borderBottom: 1, borderColor: 'divider', display: 'flex', gap: 2, alignItems: 'center' }}>
        <Typography variant="body2" color="text.secondary">
          {result.row_count.toLocaleString()} rows • {result.execution_time_ms.toFixed(2)} ms
        </Typography>
      </Box>
      <TableContainer component={Paper} sx={{ flex: 1, overflow: 'auto' }}>
        <Table stickyHeader size="small">
          <TableHead>
            <TableRow>
              {result.columns.map((col) => (
                <TableCell
                  key={col}
                  onClick={() => handleSort(col)}
                  sx={{
                    cursor: 'pointer',
                    userSelect: 'none',
                    bgcolor: theme === 'dark' ? 'grey.800' : 'grey.100',
                    '&:hover': { bgcolor: theme === 'dark' ? 'grey.700' : 'grey.200' },
                    fontWeight: 'bold',
                  }}
                >
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                    {col}
                    {sortColumn === col && (
                      <Typography variant="caption">
                        {sortDirection === 'asc' ? '↑' : '↓'}
                      </Typography>
                    )}
                  </Box>
                </TableCell>
              ))}
            </TableRow>
          </TableHead>
          <TableBody>
            {sortedRows.map((row, idx) => (
              <TableRow key={idx} hover>
                {row.map((cell, cellIdx) => (
                  <TableCell key={cellIdx}>
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                      <Typography variant="body2" sx={{ flex: 1 }}>
                        {cell !== null && cell !== undefined ? String(cell) : 'NULL'}
                      </Typography>
                      <Tooltip title="Copy">
                        <IconButton
                          size="small"
                          onClick={() => copyToClipboard(String(cell || ''))}
                          sx={{ opacity: 0.5, '&:hover': { opacity: 1 } }}
                        >
                          <ContentCopyIcon fontSize="small" />
                        </IconButton>
                      </Tooltip>
                    </Box>
                  </TableCell>
                ))}
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>
    </Box>
  );
}

