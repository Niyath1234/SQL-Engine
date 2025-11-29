import { useState } from 'react';
import {
  Box,
  List,
  ListItem,
  ListItemButton,
  ListItemText,
  Typography,
  IconButton,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  TextField,
  Button,
  Menu,
  MenuItem,
  Divider,
} from '@mui/material';
import {
  Bookmark,
  Add,
  Edit,
  Delete,
  Folder,
} from '@mui/icons-material';
import type { SavedQuery } from '../types';

interface SavedQueriesProps {
  queries: SavedQuery[];
  onSelect: (sql: string) => void;
  onSave: (query: Omit<SavedQuery, 'id'>) => Promise<void>;
  onUpdate: (id: string, query: Partial<SavedQuery>) => Promise<void>;
  onDelete: (id: string) => Promise<void>;
  theme?: 'light' | 'dark';
}

export function SavedQueries({
  queries,
  onSelect,
  onSave,
  onUpdate,
  onDelete,
  theme = 'light',
}: SavedQueriesProps) {
  const [dialogOpen, setDialogOpen] = useState(false);
  const [editingQuery, setEditingQuery] = useState<SavedQuery | null>(null);
  const [title, setTitle] = useState('');
  const [sql, setSql] = useState('');
  const [folder, setFolder] = useState('');
  const [menuAnchor, setMenuAnchor] = useState<{ el: HTMLElement; query: SavedQuery } | null>(null);

  // Group queries by folder

  const handleOpenDialog = (query?: SavedQuery) => {
    if (query) {
      setEditingQuery(query);
      setTitle(query.title);
      setSql(query.sql);
      setFolder(query.folder || '');
    } else {
      setEditingQuery(null);
      setTitle('');
      setSql('');
      setFolder('');
    }
    setDialogOpen(true);
  };

  const handleSave = async () => {
    if (!title.trim() || !sql.trim()) return;

    if (editingQuery) {
      await onUpdate(editingQuery.id, { title, sql, folder: folder || undefined });
    } else {
      await onSave({ title, sql, folder: folder || undefined });
    }

    setDialogOpen(false);
    setEditingQuery(null);
    setTitle('');
    setSql('');
    setFolder('');
  };

  const groupedQueries = queries.reduce((acc, query) => {
    const folderName = query.folder || 'Uncategorized';
    if (!acc[folderName]) acc[folderName] = [];
    acc[folderName].push(query);
    return acc;
  }, {} as Record<string, SavedQuery[]>);

  return (
    <>
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
            <Bookmark fontSize="small" />
            <Typography variant="h6" sx={{ fontSize: '1rem', fontWeight: 600 }}>
              Saved Queries
            </Typography>
          </Box>
          <IconButton size="small" onClick={() => handleOpenDialog()} title="Save current query">
            <Add fontSize="small" />
          </IconButton>
        </Box>

        <Box sx={{ flex: 1, overflow: 'auto' }}>
          {queries.length === 0 ? (
            <Box sx={{ p: 2, textAlign: 'center', color: 'text.secondary' }}>
              <Typography variant="body2">No saved queries</Typography>
            </Box>
          ) : (
            <List dense>
              {Object.entries(groupedQueries).map(([folderName, folderQueries]) => (
                <Box key={folderName}>
                  <ListItem disablePadding>
                    <ListItemButton sx={{ py: 0.5 }}>
                      <Folder fontSize="small" sx={{ mr: 1, fontSize: 16 }} />
                      <ListItemText
                        primary={folderName}
                        primaryTypographyProps={{ variant: 'body2', fontWeight: 500 }}
                      />
                    </ListItemButton>
                  </ListItem>
                  {folderQueries.map((query) => (
                    <ListItem key={query.id} disablePadding>
                      <ListItemButton
                        onClick={() => onSelect(query.sql)}
                        onContextMenu={(e) => {
                          e.preventDefault();
                          setMenuAnchor({ el: e.currentTarget, query });
                        }}
                        sx={{ pl: 4, py: 0.5 }}
                      >
                        <ListItemText
                          primary={query.title}
                          secondary={query.sql.substring(0, 60) + '...'}
                          primaryTypographyProps={{ variant: 'body2' }}
                          secondaryTypographyProps={{ variant: 'caption' }}
                        />
                      </ListItemButton>
                    </ListItem>
                  ))}
                  <Divider />
                </Box>
              ))}
            </List>
          )}
        </Box>
      </Box>

      <Menu
        open={!!menuAnchor}
        onClose={() => setMenuAnchor(null)}
        anchorEl={menuAnchor?.el}
      >
        <MenuItem
          onClick={() => {
            if (menuAnchor) {
              handleOpenDialog(menuAnchor.query);
            }
            setMenuAnchor(null);
          }}
        >
          <Edit fontSize="small" sx={{ mr: 1 }} />
          Edit
        </MenuItem>
        <MenuItem
          onClick={async () => {
            if (menuAnchor) {
              await onDelete(menuAnchor.query.id);
            }
            setMenuAnchor(null);
          }}
        >
          <Delete fontSize="small" sx={{ mr: 1 }} />
          Delete
        </MenuItem>
      </Menu>

      <Dialog open={dialogOpen} onClose={() => setDialogOpen(false)} maxWidth="md" fullWidth>
        <DialogTitle>{editingQuery ? 'Edit Query' : 'Save Query'}</DialogTitle>
        <DialogContent>
          <TextField
            fullWidth
            label="Title"
            value={title}
            onChange={(e) => setTitle(e.target.value)}
            margin="normal"
            required
          />
          <TextField
            fullWidth
            label="SQL"
            value={sql}
            onChange={(e) => setSql(e.target.value)}
            margin="normal"
            multiline
            rows={8}
            required
            sx={{ fontFamily: 'monospace' }}
          />
          <TextField
            fullWidth
            label="Folder (optional)"
            value={folder}
            onChange={(e) => setFolder(e.target.value)}
            margin="normal"
            placeholder="e.g., Analytics, Reports"
          />
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setDialogOpen(false)}>Cancel</Button>
          <Button onClick={handleSave} variant="contained" disabled={!title.trim() || !sql.trim()}>
            Save
          </Button>
        </DialogActions>
      </Dialog>
    </>
  );
}

