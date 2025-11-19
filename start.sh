#!/bin/bash

# Start script for Hypergraph SQL Engine with PostgreSQL

echo "🚀 Starting Hypergraph SQL Engine..."
echo ""

# Check if PostgreSQL is installed
if command -v psql &> /dev/null; then
    echo "✓ PostgreSQL found"
else
    # Try to add PostgreSQL to PATH (Homebrew installation)
    if [ -f "/opt/homebrew/opt/postgresql@15/bin/psql" ]; then
        export PATH="/opt/homebrew/opt/postgresql@15/bin:$PATH"
        echo "✓ PostgreSQL found in Homebrew location"
    elif [ -f "/usr/local/opt/postgresql@15/bin/psql" ]; then
        export PATH="/usr/local/opt/postgresql@15/bin:$PATH"
        echo "✓ PostgreSQL found in Homebrew location"
    else
        echo "⚠️  PostgreSQL not found in PATH"
        echo "   Please install PostgreSQL or add it to your PATH"
        echo "   Install with: brew install postgresql@15"
    fi
fi

# Check if PostgreSQL is running
if pg_isready &> /dev/null; then
    echo "✓ PostgreSQL is running"
else
    echo "⚠️  PostgreSQL is not running"
    echo "   Attempting to start PostgreSQL..."
    
    # Try to start PostgreSQL via Homebrew services
    if command -v brew &> /dev/null; then
        brew services start postgresql@15 2>/dev/null || {
            echo "   Could not start PostgreSQL automatically"
            echo "   Please start it manually with:"
            echo "   brew services start postgresql@15"
            echo "   OR"
            echo "   pg_ctl -D /opt/homebrew/var/postgresql@15 start"
        }
        
        # Wait a bit for PostgreSQL to start
        sleep 2
        
        if pg_isready &> /dev/null; then
            echo "✓ PostgreSQL started successfully"
        else
            echo "⚠️  PostgreSQL still not running - continuing anyway"
            echo "   The Hypergraph Engine will work, but PostgreSQL comparison won't be available"
        fi
    fi
fi

echo ""
echo "📊 Starting Hypergraph SQL Engine Web Server..."
echo "   Open http://127.0.0.1:3000 in your browser"
echo ""

# Run the engine
cd "$(dirname "$0")"
cargo run --release

