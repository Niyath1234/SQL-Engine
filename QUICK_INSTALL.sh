#!/bin/bash
# Quick Rust Installation Script for macOS/Linux

set -e

echo "🔧 Rust Setup for Hypergraph SQL Engine"
echo "========================================"
echo ""

# Check if rust is already installed
if command -v rustc &> /dev/null; then
    echo "✅ Rust is already installed!"
    rustc --version
    cargo --version
    echo ""
    echo "Running cargo check..."
    cargo check
    exit 0
fi

echo "📦 Rust is not installed. Installing now..."
echo ""

# Detect OS
if [[ "$OSTYPE" == "darwin"* ]]; then
    echo "🍎 Detected macOS"
    
    # Check for Homebrew
    if command -v brew &> /dev/null; then
        echo "Installing via Homebrew..."
        brew install rust
    else
        echo "Installing via rustup (recommended)..."
        curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
        source $HOME/.cargo/env
    fi
elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
    echo "🐧 Detected Linux"
    echo "Installing via rustup (recommended)..."
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    source $HOME/.cargo/env
else
    echo "❌ Unsupported OS: $OSTYPE"
    echo "Please install Rust manually: https://www.rust-lang.org/tools/install"
    exit 1
fi

echo ""
echo "✅ Rust installation complete!"
echo ""

# Verify installation
if command -v rustc &> /dev/null; then
    rustc --version
    cargo --version
    echo ""
    echo "🔍 Verifying project setup..."
    cargo check
    echo ""
    echo "✅ Setup complete! You can now:"
    echo "   - cargo build    (build the project)"
    echo "   - cargo run      (run the project)"
    echo "   - cargo test     (run tests)"
else
    echo "❌ Installation may have failed. Please check the output above."
    echo "You may need to:"
    echo "  1. Add ~/.cargo/bin to your PATH"
    echo "  2. Run: source ~/.cargo/env"
    exit 1
fi

