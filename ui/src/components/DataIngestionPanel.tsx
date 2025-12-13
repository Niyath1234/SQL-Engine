import { useState } from 'react';
import { Box, TextField, Button, Typography, Paper, Alert, Tabs, Tab, Select, MenuItem, FormControl, InputLabel, Dialog, DialogTitle, DialogContent, DialogActions, DialogContentText } from '@mui/material';
import { PlayArrow, Clear, CheckCircle, Error as ErrorIcon, CloudDownload, DeleteForever, Http } from '@mui/icons-material';
import axios from 'axios';
import { clearAllData } from '../api/client';

interface DataIngestionPanelProps {
  theme?: 'light' | 'dark';
  onIngestionComplete?: () => void;
}

interface IngestionResult {
  success: boolean;
  records_ingested?: number;
  tables_affected?: string[];
  error?: string;
}

export function DataIngestionPanel({ theme = 'dark', onIngestionComplete }: DataIngestionPanelProps) {
  const [jsonInput, setJsonInput] = useState('');
  const [sourceId, setSourceId] = useState('');
  const [isIngesting, setIsIngesting] = useState(false);
  const [isLoadingSample, setIsLoadingSample] = useState(false);
  const [result, setResult] = useState<IngestionResult | null>(null);
  const [activeTab, setActiveTab] = useState(0);
  
  // Postman-style API call fields
  const [mode, setMode] = useState<'json' | 'api'>('api');
  const [apiUrl, setApiUrl] = useState('');
  const [apiMethod, setApiMethod] = useState('GET');
  const [apiHeaders, setApiHeaders] = useState('{"Content-Type": "application/json"}');
  const [apiBody, setApiBody] = useState('');
  const [isFetchingApi, setIsFetchingApi] = useState(false);
  
  // Clear All dialog
  const [clearDialogOpen, setClearDialogOpen] = useState(false);
  const [isClearing, setIsClearing] = useState(false);

  const exampleJson = {
    products: `[
  {
    "id": 1,
    "name": "Product A",
    "price": 29.99,
    "category_id": 1,
    "created_at": "2024-01-15"
  },
  {
    "id": 2,
    "name": "Product B",
    "price": 49.99,
    "category_id": 2,
    "created_at": "2024-01-16"
  }
]`,
    orders: `[
  {
    "id": 1,
    "customer_id": 1,
    "order_date": "2024-01-20",
    "status": "completed",
    "total_amount": 79.98
  },
  {
    "id": 2,
    "customer_id": 2,
    "order_date": "2024-01-21",
    "status": "pending",
    "total_amount": 49.99
  }
]`,
    nested: `[
  {
    "id": 1,
    "name": "Order 1",
    "items": [
      {"product_id": 1, "quantity": 2, "price": 29.99},
      {"product_id": 2, "quantity": 1, "price": 49.99}
    ],
    "customer": {
      "id": 1,
      "name": "John Doe",
      "email": "john@example.com"
    }
  }
]`,
  };

  const handleIngest = async () => {
    if (!jsonInput.trim()) {
      setResult({ success: false, error: 'Please provide JSON data' });
      return;
    }

    if (!sourceId.trim()) {
      setResult({ success: false, error: 'Please provide a source ID' });
      return;
    }

    setIsIngesting(true);
    setResult(null);

    try {
      // Parse JSON to validate
      const parsed = JSON.parse(jsonInput);
      
      // Simulate API ingestion
      const response = await axios.post('/api/ingest/simulate', {
        source_id: sourceId,
        payloads: Array.isArray(parsed) ? parsed : [parsed],
      });

      const ingestionResult: IngestionResult = {
        success: true,
        records_ingested: response.data.records_ingested || 0,
        tables_affected: response.data.tables_affected || [],
      };

      setResult(ingestionResult);
      onIngestionComplete?.();
    } catch (error: any) {
      if (error.response) {
        setResult({
          success: false,
          error: error.response.data?.error || error.response.data?.message || 'Ingestion failed',
        });
      } else if (error instanceof SyntaxError) {
        setResult({
          success: false,
          error: `Invalid JSON: ${error.message}`,
        });
      } else {
        setResult({
          success: false,
          error: error.message || 'Unknown error occurred',
        });
      }
    } finally {
      setIsIngesting(false);
    }
  };

  const handleLoadExample = (example: string) => {
    setJsonInput(example);
    setResult(null);
  };

  const handleClear = () => {
    setJsonInput('');
    setResult(null);
  };

  const handleLoadSampleData = async () => {
    setIsLoadingSample(true);
    setResult(null);
    
    try {
      const response = await axios.post('/api/ingest/load_sample_data');
      
      if (response.data.status === 'success') {
        setResult({
          success: true,
          records_ingested: response.data.tables_loaded?.reduce((sum: number, t: any) => sum + (t.records || 0), 0) || 0,
          tables_affected: response.data.tables_loaded?.map((t: any) => t.table) || [],
        });
        onIngestionComplete?.();
      } else {
        setResult({
          success: false,
          error: 'Failed to load sample data',
        });
      }
    } catch (error: any) {
      setResult({
        success: false,
        error: error.response?.data?.error || error.message || 'Failed to load sample data',
      });
    } finally {
      setIsLoadingSample(false);
    }
  };

  const handleFetchApi = async () => {
    if (!apiUrl.trim()) {
      setResult({ success: false, error: 'Please provide an API URL' });
      return;
    }

    setIsFetchingApi(true);
    setResult(null);

    try {
      let headers: Record<string, string> = {};
      try {
        headers = JSON.parse(apiHeaders || '{}');
      } catch (e) {
        setResult({ success: false, error: 'Invalid headers JSON' });
        setIsFetchingApi(false);
        return;
      }

      let response;
      const config = { headers };

      if (apiMethod === 'GET') {
        response = await axios.get(apiUrl, config);
      } else if (apiMethod === 'POST') {
        let body = apiBody;
        try {
          body = apiBody ? JSON.parse(apiBody) : {};
        } catch (e) {
          setResult({ success: false, error: 'Invalid body JSON' });
          setIsFetchingApi(false);
          return;
        }
        response = await axios.post(apiUrl, body, config);
      } else {
        setResult({ success: false, error: 'Only GET and POST methods are supported' });
        setIsFetchingApi(false);
        return;
      }

      // Extract data from response
      const data = response.data;
      const jsonData = Array.isArray(data) ? data : (data.data || data.items || [data]);

      // Auto-generate source ID from URL
      const urlParts = new URL(apiUrl);
      const autoSourceId = urlParts.pathname.split('/').filter(Boolean).join('_') || 'api_data';

      // Ingest the fetched data
      const ingestResponse = await axios.post('/api/ingest/simulate', {
        source_id: autoSourceId,
        payloads: Array.isArray(jsonData) ? jsonData : [jsonData],
      });

      setResult({
        success: true,
        records_ingested: ingestResponse.data.records_ingested || 0,
        tables_affected: ingestResponse.data.tables_affected || [],
      });
      
      setSourceId(autoSourceId);
      setJsonInput(JSON.stringify(jsonData, null, 2));
      onIngestionComplete?.();
    } catch (error: any) {
      setResult({
        success: false,
        error: error.response?.data?.error || error.message || 'Failed to fetch API data',
      });
    } finally {
      setIsFetchingApi(false);
    }
  };

  const handleClearAll = async () => {
    setIsClearing(true);
    try {
      const response = await clearAllData();
      if (response.success) {
        setResult({
          success: true,
          records_ingested: 0,
          tables_affected: response.tables_dropped,
        });
        onIngestionComplete?.();
        setClearDialogOpen(false);
      } else {
        setResult({
          success: false,
          error: response.error || 'Failed to clear all data',
        });
      }
    } catch (error: any) {
      setResult({
        success: false,
        error: error.message || 'Failed to clear all data',
      });
    } finally {
      setIsClearing(false);
    }
  };

  const bgColor = theme === 'dark' ? 'rgba(30, 30, 30, 0.8)' : 'rgba(250, 250, 250, 0.8)';
  const borderColor = theme === 'dark' ? 'rgba(255, 255, 255, 0.1)' : 'rgba(0, 0, 0, 0.1)';

  return (
    <Box sx={{ height: '100%', display: 'flex', flexDirection: 'column', p: 2 }}>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
        <Typography variant="h6" sx={{ fontWeight: 600 }}>
          Data Ingestion
        </Typography>
        <Box sx={{ display: 'flex', gap: 1 }}>
          <Button
            variant="outlined"
            startIcon={<DeleteForever />}
            onClick={() => setClearDialogOpen(true)}
            size="small"
            color="error"
            sx={{ borderColor: '#f48771', color: '#f48771', '&:hover': { borderColor: '#f48771', bgcolor: 'rgba(244, 135, 113, 0.1)' } }}
          >
            Clear All
          </Button>
          <Button
            variant="contained"
            startIcon={<CloudDownload />}
            onClick={handleLoadSampleData}
            disabled={isLoadingSample}
            size="small"
            sx={{ bgcolor: '#007ACC', '&:hover': { bgcolor: '#005a9e' } }}
          >
            {isLoadingSample ? 'Loading...' : 'Load Sample'}
          </Button>
        </Box>
      </Box>
      
      {/* Mode Toggle */}
      <Tabs
        value={mode}
        onChange={(_, v) => setMode(v)}
        sx={{ mb: 2, borderBottom: 1, borderColor: 'divider' }}
      >
        <Tab icon={<Http />} iconPosition="start" label="API Call (Postman)" value="api" />
        <Tab label="JSON Paste" value="json" />
      </Tabs>
      <Typography variant="body2" sx={{ mb: 2, color: 'text.secondary' }}>
        Load sample data (5 tables) or paste JSON data from APIs to simulate ingestion
      </Typography>

      {mode === 'api' ? (
        <>
          {/* API Call Interface (Postman-style) */}
          <Box sx={{ display: 'flex', gap: 1, mb: 2, alignItems: 'center' }}>
            <FormControl size="small" sx={{ minWidth: 100 }}>
              <InputLabel>Method</InputLabel>
              <Select
                value={apiMethod}
                onChange={(e) => setApiMethod(e.target.value)}
                label="Method"
                sx={{ bgcolor: bgColor }}
              >
                <MenuItem value="GET">GET</MenuItem>
                <MenuItem value="POST">POST</MenuItem>
              </Select>
            </FormControl>
            <TextField
              label="API URL"
              placeholder="https://api.example.com/products"
              value={apiUrl}
              onChange={(e) => setApiUrl(e.target.value)}
              size="small"
              fullWidth
              InputProps={{
                sx: {
                  bgcolor: bgColor,
                  '& fieldset': { borderColor: borderColor },
                },
              }}
            />
            <Button
              variant="contained"
              startIcon={<PlayArrow />}
              onClick={handleFetchApi}
              disabled={isFetchingApi || !apiUrl.trim()}
              sx={{ bgcolor: '#007ACC', '&:hover': { bgcolor: '#005a9e' } }}
            >
              {isFetchingApi ? 'Fetching...' : 'Fetch'}
            </Button>
          </Box>

          <TextField
            label="Headers (JSON)"
            placeholder='{"Authorization": "Bearer token", "Content-Type": "application/json"}'
            value={apiHeaders}
            onChange={(e) => setApiHeaders(e.target.value)}
            size="small"
            fullWidth
            multiline
            rows={3}
            sx={{ mb: 2 }}
            InputProps={{
              sx: {
                fontFamily: 'monospace',
                fontSize: '0.85rem',
                bgcolor: bgColor,
                '& fieldset': { borderColor: borderColor },
              },
            }}
          />

          {apiMethod === 'POST' && (
            <TextField
              label="Request Body (JSON)"
              placeholder='{"key": "value"}'
              value={apiBody}
              onChange={(e) => setApiBody(e.target.value)}
              size="small"
              fullWidth
              multiline
              rows={4}
              sx={{ mb: 2 }}
              InputProps={{
                sx: {
                  fontFamily: 'monospace',
                  fontSize: '0.85rem',
                  bgcolor: bgColor,
                  '& fieldset': { borderColor: borderColor },
                },
              }}
            />
          )}

          {/* Source ID Input */}
          <TextField
            label="Source ID (auto-generated from URL)"
            placeholder="e.g., products_api"
            value={sourceId}
            onChange={(e) => setSourceId(e.target.value)}
            size="small"
            fullWidth
            sx={{ mb: 2 }}
            InputProps={{
              sx: {
                bgcolor: bgColor,
                '& fieldset': { borderColor: borderColor },
              },
            }}
          />
        </>
      ) : (
        <>
          {/* Source ID Input */}
          <TextField
            label="Source ID"
            placeholder="e.g., products_api, orders_api"
            value={sourceId}
            onChange={(e) => setSourceId(e.target.value)}
            size="small"
            fullWidth
            sx={{ mb: 2 }}
            InputProps={{
              sx: {
                bgcolor: bgColor,
                '& fieldset': { borderColor: borderColor },
              },
            }}
          />

          {/* Example Tabs */}
          <Tabs
            value={activeTab}
            onChange={(_, v) => setActiveTab(v)}
            sx={{ mb: 2, borderBottom: 1, borderColor: 'divider' }}
          >
            <Tab label="Products" onClick={() => handleLoadExample(exampleJson.products)} />
            <Tab label="Orders" onClick={() => handleLoadExample(exampleJson.orders)} />
            <Tab label="Nested" onClick={() => handleLoadExample(exampleJson.nested)} />
          </Tabs>

          {/* JSON Input */}
          <TextField
            label="JSON Data"
            placeholder="Paste your JSON data here..."
            value={jsonInput}
            onChange={(e) => setJsonInput(e.target.value)}
            multiline
            rows={12}
            fullWidth
            sx={{ mb: 2, flex: 1 }}
            InputProps={{
              sx: {
                fontFamily: 'monospace',
                fontSize: '0.85rem',
                bgcolor: bgColor,
                '& fieldset': { borderColor: borderColor },
              },
            }}
          />
        </>
      )}

      {/* Action Buttons */}
      {mode === 'json' && (
        <Box sx={{ display: 'flex', gap: 1, mb: 2 }}>
          <Button
            variant="contained"
            startIcon={<PlayArrow />}
            onClick={handleIngest}
            disabled={isIngesting || !jsonInput.trim() || !sourceId.trim()}
            fullWidth
          >
            {isIngesting ? 'Ingesting...' : 'Ingest Data'}
          </Button>
          <Button
            variant="outlined"
            startIcon={<Clear />}
            onClick={handleClear}
            disabled={isIngesting}
          >
            Clear
          </Button>
        </Box>
      )}

      {/* Result */}
      {result && (
        <Alert
          severity={result.success ? 'success' : 'error'}
          icon={result.success ? <CheckCircle /> : <ErrorIcon />}
          sx={{ mb: 2 }}
        >
          {result.success ? (
            <Box>
              <Typography variant="body2" sx={{ fontWeight: 600, mb: 0.5 }}>
                Ingestion Successful!
              </Typography>
              <Typography variant="caption" sx={{ display: 'block' }}>
                Records ingested: {result.records_ingested || 0}
              </Typography>
              {result.tables_affected && result.tables_affected.length > 0 && (
                <Typography variant="caption" sx={{ display: 'block' }}>
                  Tables affected: {result.tables_affected.join(', ')}
                </Typography>
              )}
            </Box>
          ) : (
            <Typography variant="body2">{result.error}</Typography>
          )}
        </Alert>
      )}

      {/* Info */}
      <Paper
        elevation={0}
        sx={{
          p: 1.5,
          bgcolor: bgColor,
          border: `1px solid ${borderColor}`,
          borderRadius: 1,
        }}
      >
        <Typography variant="caption" sx={{ color: 'text.secondary', display: 'block', mb: 0.5 }}>
          <strong>Tips:</strong>
        </Typography>
        <Typography variant="caption" sx={{ color: 'text.secondary', display: 'block' }}>
          • JSON arrays will be ingested as table rows
        </Typography>
        <Typography variant="caption" sx={{ color: 'text.secondary', display: 'block' }}>
          • Nested objects will be flattened automatically
        </Typography>
        <Typography variant="caption" sx={{ color: 'text.secondary', display: 'block' }}>
          • Schema will be inferred automatically
        </Typography>
      </Paper>

      {/* Clear All Dialog */}
      <Dialog open={clearDialogOpen} onClose={() => setClearDialogOpen(false)}>
        <DialogTitle>Clear All Data</DialogTitle>
        <DialogContent>
          <DialogContentText>
            This will permanently delete all tables, data, cache, and reset the hypergraph spine.
            This action cannot be undone. Are you sure?
          </DialogContentText>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setClearDialogOpen(false)} color="inherit">
            Cancel
          </Button>
          <Button onClick={handleClearAll} color="error" disabled={isClearing} startIcon={<DeleteForever />}>
            {isClearing ? 'Clearing...' : 'Clear All'}
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
}

