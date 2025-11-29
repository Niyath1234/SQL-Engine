# Starting the SQL Engine UI

## Quick Start

1. **Make sure the Rust backend is running on port 8080:**
   ```bash
   cargo run --release --bin web_admin
   ```

2. **Start the UI development server:**
   ```bash
   cd ui
   npm run dev
   ```

3. **Open your browser:**
   - Go to `http://localhost:3000`
   - The UI will automatically proxy API requests to `http://localhost:8080`

## Troubleshooting

### Port 3000 already in use
If you get an error that port 3000 is already in use:
```bash
# Kill the process using port 3000
lsof -ti:3000 | xargs kill -9

# Or use a different port
# Edit vite.config.ts and change port: 3000 to port: 3001
```

### Backend not running
Make sure the Rust backend is running on port 8080. The UI will show connection errors if the backend is not available.

### Check if server is running
```bash
# Check if port 3000 is in use
lsof -ti:3000

# Check if the server responds
curl http://localhost:3000
```

## Production Build

To build for production (served by Rust backend):
```bash
cd ui
npm run build
```

This will build the UI into `../static/` directory, which the Rust server will serve automatically.

