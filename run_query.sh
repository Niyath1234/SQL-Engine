#!/bin/bash

# Simple query runner for Hypergraph SQL Engine
# Usage: ./run_query.sh "SELECT * FROM enterprise_survey LIMIT 10"

CSV_FILE="/Users/niyathnair/Downloads/LQS/annual-enterprise-survey-2024-financial-year-provisional.csv"

if [ -z "$1" ]; then
    echo "Usage: $0 \"SQL_QUERY\""
    echo "Example: $0 \"SELECT * FROM enterprise_survey LIMIT 10\""
    exit 1
fi

QUERY="$1"

echo "🚀 Running query: $QUERY"
echo ""

# Run benchmark with the query
cargo run --release --bin benchmark -- "$CSV_FILE" "$QUERY"

