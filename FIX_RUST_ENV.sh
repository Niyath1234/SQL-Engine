#!/bin/bash
# Fix Rust Environment Setup

echo "🔧 Fixing Rust Environment"
echo "=========================="
echo ""

# Check if cargo exists
if [ -f "$HOME/.cargo/bin/cargo" ]; then
    echo "✅ Found Cargo at $HOME/.cargo/bin/cargo"
    
    # Test if it works
    if "$HOME/.cargo/bin/cargo" --version > /dev/null 2>&1; then
        echo "✅ Cargo is executable"
        "$HOME/.cargo/bin/cargo" --version
        "$HOME/.cargo/bin/rustc" --version 2>/dev/null || echo "⚠️  rustc not found"
    else
        echo "❌ Cargo exists but cannot execute"
        exit 1
    fi
else
    echo "❌ Cargo not found. Installing Rust..."
    
    # Install rustup
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    
    # Source cargo env
    if [ -f "$HOME/.cargo/env" ]; then
        source "$HOME/.cargo/env"
    fi
fi

echo ""
echo "📝 Setting up environment..."

# Add to .zshrc if it exists
if [ -f "$HOME/.zshrc" ]; then
    if ! grep -q "cargo/env" "$HOME/.zshrc"; then
        echo "" >> "$HOME/.zshrc"
        echo "# Rust/Cargo" >> "$HOME/.zshrc"
        echo 'export PATH="$HOME/.cargo/bin:$PATH"' >> "$HOME/.zshrc"
        echo "✅ Added to ~/.zshrc"
    else
        echo "✅ Already in ~/.zshrc"
    fi
fi

# Add to .bashrc if it exists
if [ -f "$HOME/.bashrc" ]; then
    if ! grep -q "cargo/env" "$HOME/.bashrc"; then
        echo "" >> "$HOME/.bashrc"
        echo "# Rust/Cargo" >> "$HOME/.bashrc"
        echo 'export PATH="$HOME/.cargo/bin:$PATH"' >> "$HOME/.bashrc"
        echo "✅ Added to ~/.bashrc"
    else
        echo "✅ Already in ~/.bashrc"
    fi
fi

# Fix .zshenv if it has bad cargo/env reference
if [ -f "$HOME/.zshenv" ]; then
    if grep -q "\.cargo/env" "$HOME/.zshenv" && [ ! -f "$HOME/.cargo/env" ]; then
        echo "⚠️  Found broken .cargo/env reference in .zshenv"
        echo "   You may want to fix this manually"
    fi
fi

echo ""
echo "✅ Environment setup complete!"
echo ""
echo "To use Rust in this session, run:"
echo "  source ~/.cargo/env"
echo "  # or"
echo "  export PATH=\"\$HOME/.cargo/bin:\$PATH\""
echo ""
echo "To verify:"
echo "  cargo --version"
echo "  rustc --version"

