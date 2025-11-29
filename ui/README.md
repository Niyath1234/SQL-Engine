# SQL Engine UI - Querybook Style

Modern React-based SQL IDE for the Hypergraph SQL Engine.

## Quick Start

### Development Mode

1. Make sure the Rust backend is running on `http://localhost:8080`
2. Start the UI development server:

```bash
./start-dev.sh
# or
npm run dev
```

The UI will be available at `http://localhost:3000`

### Production Build

Build the UI for production (outputs to `../static`):

```bash
npm run build
```

Then the Rust server will serve the built files from the `static/` directory.

## Features

- 🎨 Monaco Editor with SQL syntax highlighting and autocomplete
- 📊 Virtualized result grid for large datasets
- 📁 Schema explorer with table/column browsing
- 📜 Query history with timestamps
- 💾 Saved queries with folder organization
- 🌓 Dark/Light theme support
- 📑 Multiple query tabs
- 🔄 Resizable panels
- ⌨️ Keyboard shortcuts (Ctrl/Cmd+Enter to run)

## Keyboard Shortcuts

- `Ctrl/Cmd + Enter` - Run query
- `Ctrl/Cmd + S` - Save current query
- `Ctrl/Cmd + D` - Duplicate tab

## Project Structure

```
ui/
├── src/
│   ├── components/     # React components
│   ├── pages/          # Page components
│   ├── api/            # API client
│   ├── store/          # State management (Zustand)
│   └── types/          # TypeScript types
└── package.json
```
