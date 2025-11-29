#!/bin/bash
echo "Checking UI server status..."
echo ""

# Check if port 3000 is in use
if lsof -ti:3000 > /dev/null 2>&1; then
    echo "✅ Port 3000 is in use"
    echo "   Process: $(lsof -ti:3000 | head -1)"
    echo ""
    
    # Try to connect
    if curl -s http://localhost:3000 > /dev/null 2>&1; then
        echo "✅ Server is responding on http://localhost:3000"
        echo ""
        echo "🌐 Open your browser and go to: http://localhost:3000"
    else
        echo "❌ Port 3000 is in use but not responding"
        echo "   The server might be starting up, wait a few seconds and try again"
    fi
else
    echo "❌ Port 3000 is not in use"
    echo ""
    echo "To start the server, run:"
    echo "  cd ui"
    echo "  npm run dev"
fi

echo ""
echo "Checking backend (port 8080)..."
if curl -s http://localhost:8080/api/health > /dev/null 2>&1; then
    echo "✅ Backend is running on http://localhost:8080"
else
    echo "⚠️  Backend is not running on http://localhost:8080"
    echo "   Start it with: cargo run --release --bin web_admin"
fi

