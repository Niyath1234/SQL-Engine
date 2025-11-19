#!/bin/bash
# Start PostgreSQL script

echo "🐘 Starting PostgreSQL..."

# Try to add PostgreSQL to PATH
if [ -f "/opt/homebrew/opt/postgresql@15/bin/pg_ctl" ]; then
    export PATH="/opt/homebrew/opt/postgresql@15/bin:$PATH"
elif [ -f "/usr/local/opt/postgresql@15/bin/pg_ctl" ]; then
    export PATH="/usr/local/opt/postgresql@15/bin:$PATH"
fi

# Check if already running
if pg_isready &> /dev/null; then
    echo "✓ PostgreSQL is already running"
    exit 0
fi

# Try Homebrew services
if command -v brew &> /dev/null; then
    echo "Starting PostgreSQL via Homebrew services..."
    brew services start postgresql@15
    
    # Wait and check
    sleep 3
    if pg_isready &> /dev/null; then
        echo "✓ PostgreSQL started successfully"
    else
        echo "⚠️  Could not start PostgreSQL automatically"
        echo "   Try manually: brew services start postgresql@15"
    fi
else
    echo "⚠️  Homebrew not found. Please start PostgreSQL manually."
fi
