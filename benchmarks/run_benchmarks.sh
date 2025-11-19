#!/usr/bin/env bash

set -euo pipefail

# Simple reproducible benchmark runner for the Hypergraph SQL Engine.
# Usage:
#   ./benchmarks/run_benchmarks.sh /path/to/data.csv
#
# If no CSV path is given, it defaults to the annual enterprise survey CSV
# you’ve been using in this project (if it exists).

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CSV_PATH="${1:-}"

if [[ -z "${CSV_PATH}" ]]; then
  DEFAULT_CSV="${ROOT_DIR}/annual-enterprise-survey-2024-financial-year-provisional.csv"
  if [[ -f "${DEFAULT_CSV}" ]]; then
    CSV_PATH="${DEFAULT_CSV}"
  else
    echo "Usage: $0 /path/to/data.csv"
    echo "No CSV path provided and default CSV not found at:"
    echo "  ${DEFAULT_CSV}"
    exit 1
  fi
fi

echo "=== Hypergraph SQL Engine Benchmarks ==="
echo "Root directory : ${ROOT_DIR}"
echo "CSV dataset    : ${CSV_PATH}"
echo "Rust version   : $(rustc --version 2>/dev/null || echo 'unknown')"
echo

cd "${ROOT_DIR}"

echo "[1/2] Building release binary..."
cargo build --release --bin compare_engines

echo
echo "[2/2] Running full benchmark suite (Hypergraph vs PostgreSQL vs DuckDB)..."
echo "Command:"
echo "  target/release/compare_engines \"${CSV_PATH}\""
echo

# Run once to show full detailed output
target/release/compare_engines "${CSV_PATH}"


