#!/bin/bash

# Simple run script - just starts the engine
# PostgreSQL should be started separately if needed

cd "$(dirname "$0")"

echo "🚀 Starting Hypergraph SQL Engine..."
echo "📊 Open http://127.0.0.1:3000 in your browser"
echo ""

cargo run --release

