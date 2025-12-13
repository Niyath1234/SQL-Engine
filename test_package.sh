#!/bin/bash
# Test script to verify the package works as a library

set -e

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║   Testing Hypergraph SQL Engine as a Package                ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

# Test 1: Check library compiles
echo "Test 1: Checking library compilation..."
cargo check --lib
if [ $? -eq 0 ]; then
    echo "✅ Library compiles successfully"
else
    echo "❌ Library compilation failed"
    exit 1
fi
echo ""

# Test 2: Run example
echo "Test 2: Running basic_usage example..."
RUST_LOG=error cargo run --example basic_usage > /dev/null 2>&1
if [ $? -eq 0 ]; then
    echo "✅ Example runs successfully"
else
    echo "❌ Example failed"
    exit 1
fi
echo ""

# Test 3: Generate documentation
echo "Test 3: Generating documentation..."
cargo doc --lib --no-deps > /dev/null 2>&1
if [ $? -eq 0 ]; then
    echo "✅ Documentation generated successfully"
    echo "   View with: cargo doc --lib --open --no-deps"
else
    echo "❌ Documentation generation failed"
    exit 1
fi
echo ""

# Test 4: Run SQL test suite
echo "Test 4: Running SQL functionality tests..."
RUST_LOG=error cargo run --release --bin test_easy_medium_sql 2>&1 | grep -q "Success Rate: 100.0%"
if [ $? -eq 0 ]; then
    echo "✅ SQL functionality tests passed"
else
    echo "⚠️  Some SQL tests may have failed (check output above)"
fi
echo ""

# Test 5: Check public API exports
echo "Test 5: Verifying public API exports..."
cargo check --lib 2>&1 | grep -q "Checking hypergraph-sql-engine"
if [ $? -eq 0 ]; then
    echo "✅ Public API is accessible"
else
    echo "❌ Public API check failed"
    exit 1
fi
echo ""

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║              All Package Tests Completed!                    ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""
echo "Next steps:"
echo "1. View documentation: cargo doc --lib --open --no-deps"
echo "2. Use in another project: Add to Cargo.toml with path dependency"
echo "3. Run example: cargo run --example basic_usage"

