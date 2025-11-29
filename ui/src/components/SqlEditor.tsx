import { useEffect, useRef } from 'react';
import Editor from '@monaco-editor/react';
import type { editor } from 'monaco-editor';
import type { TableSchema } from '../types';

interface SqlEditorProps {
  value: string;
  onChange: (value: string) => void;
  onRun: (sql?: string) => void;
  schema?: TableSchema[];
  theme?: 'light' | 'dark';
  readOnly?: boolean;
}

export function SqlEditor({ value, onChange, onRun, schema = [], theme = 'light', readOnly = false }: SqlEditorProps) {
  const editorRef = useRef<editor.IStandaloneCodeEditor | null>(null);

  useEffect(() => {
    if (!editorRef.current) return;

    // Setup autocomplete - Monaco is loaded by @monaco-editor/react
    import('monaco-editor').then((monaco) => {
      if (!editorRef.current) return;

      // Register SQL language if not already registered
      const languages = monaco.languages.getLanguages();
      if (!languages.find(l => l.id === 'sql')) {
        monaco.languages.register({ id: 'sql' });
      }

      // Setup autocomplete provider
      const tables = schema.map((t) => t.name);
      const columns = schema.flatMap((t) => t.columns.map((c) => `${t.name}.${c.name}`));
      const allColumns = schema.flatMap((t) => t.columns.map((c) => c.name));

      monaco.languages.registerCompletionItemProvider('sql', {
        provideCompletionItems: (model, position) => {
          const word = model.getWordUntilPosition(position);
          const range = {
            startLineNumber: position.lineNumber,
            endLineNumber: position.lineNumber,
            startColumn: word.startColumn,
            endColumn: word.endColumn,
          };

          const suggestions = [
            // SQL Keywords
            ...['SELECT', 'FROM', 'WHERE', 'JOIN', 'INNER', 'LEFT', 'RIGHT', 'FULL', 'ON', 'GROUP BY', 'ORDER BY', 'HAVING', 'LIMIT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER', 'TABLE', 'AS', 'AND', 'OR', 'NOT', 'IN', 'LIKE', 'IS', 'NULL', 'COUNT', 'SUM', 'AVG', 'MAX', 'MIN', 'DISTINCT'].map((keyword) => ({
              label: keyword,
              kind: monaco.languages.CompletionItemKind.Keyword,
              insertText: keyword,
              range,
            })),
            // Tables
            ...tables.map((table) => ({
              label: table,
              kind: monaco.languages.CompletionItemKind.Class,
              insertText: table,
              range,
              detail: 'Table',
            })),
            // Columns (qualified)
            ...columns.map((col) => ({
              label: col,
              kind: monaco.languages.CompletionItemKind.Field,
              insertText: col,
              range,
              detail: 'Column',
            })),
            // Columns (unqualified)
            ...allColumns.map((col) => ({
              label: col,
              kind: monaco.languages.CompletionItemKind.Field,
              insertText: col,
              range,
              detail: 'Column',
            })),
          ];

          return { suggestions };
        },
      });

      // Keyboard shortcuts
      if (editorRef.current) {
        const runQuery = () => {
          const selection = editorRef.current?.getSelection();
          if (selection && !selection.isEmpty()) {
            // Get selected text
            const selectedText = editorRef.current?.getModel()?.getValueInRange(selection);
            if (selectedText && selectedText.trim()) {
              onRun(selectedText.trim());
              return;
            }
          }
          // No selection, run full query
          onRun();
        };

        // Ctrl/Cmd + Enter: Run selection or full query
        editorRef.current.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter, runQuery);
        
        // Shift + Enter: Run selection or full query
        editorRef.current.addCommand(monaco.KeyMod.Shift | monaco.KeyCode.Enter, runQuery);
      }
    });
  }, [schema, onRun]);

  const handleEditorDidMount = (editor: editor.IStandaloneCodeEditor) => {
    editorRef.current = editor;
  };

  return (
    <Editor
      height="100%"
      language="sql"
      theme={theme === 'dark' ? 'vs-dark' : 'light'}
      value={value}
      onChange={(val) => onChange(val || '')}
      onMount={handleEditorDidMount}
      options={{
        minimap: { enabled: false },
        fontSize: 14,
        lineNumbers: 'on',
        roundedSelection: false,
        scrollBeyondLastLine: false,
        readOnly,
        wordWrap: 'on',
        automaticLayout: true,
        tabSize: 2,
        suggestOnTriggerCharacters: true,
        quickSuggestions: true,
      }}
    />
  );
}
