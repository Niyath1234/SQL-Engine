#!/bin/bash
# Test execution script for BAMJ features

echo "=== Building Project ==="
cargo build 2>&1 | tail -20

echo ""
echo "=== Running Bitmap Join Tests ==="
cargo test --test bitmap_join_tests 2>&1

echo ""
echo "=== Running Standalone Bitset Tests ==="
cargo test --test standalone_bitset_test 2>&1

echo ""
echo "=== Running All Tests ==="
cargo test 2>&1 | grep -E "(running|test result|passed|failed|error)" | tail -30

echo ""
echo "=== Test Summary ==="
cargo test 2>&1 | tail -5

