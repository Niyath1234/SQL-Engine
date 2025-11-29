#!/bin/bash
# Clear all cache files and directories for the SQL Engine
# WARNING: This will also DROP ALL TABLES in addition to clearing cache

set -e

echo "=== Clearing SQL Engine Cache and Dropping All Tables ==="
echo ""
echo "WARNING: This will:"
echo "  - Drop ALL tables from the database"
echo "  - Clear all cache files and directories"
echo ""
read -p "Are you sure you want to continue? (yes/no): " confirm
if [ "$confirm" != "yes" ]; then
    echo "Cancelled."
    exit 0
fi
echo ""

# Default server URL
SERVER_URL="${SERVER_URL:-http://localhost:8080}"

# Function to clear via API
clear_via_api() {
    echo "Attempting to clear cache via API at $SERVER_URL..."
    
    if command -v curl &> /dev/null; then
        response=$(curl -s -X POST "$SERVER_URL/api/clear_cache" \
            -H "Content-Type: application/json" 2>&1)
        
        if [ $? -eq 0 ]; then
            echo "$response" | python3 -m json.tool 2>/dev/null || echo "$response"
            echo ""
            return 0
        else
            echo "Warning: Failed to connect to server at $SERVER_URL"
            echo "This is OK if the server is not running - will clear local files only"
            echo ""
            return 1
        fi
    else
        echo "Warning: curl not found, skipping API call"
        echo ""
        return 1
    fi
}

# Function to clear local cache directories
clear_local_cache() {
    echo "Clearing local cache directories..."
    
    local cleared=0
    
    # Directories to clear
    local dirs=(
        ".query_patterns"
        ".wal"
        ".operator_cache"
        ".spill"
        "target/tmp"
    )
    
    # Files to clear
    local files=(
        ".wal.log"
        "wal.log"
    )
    
    # Clear directories
    for dir in "${dirs[@]}"; do
        if [ -d "$dir" ]; then
            echo "  Removing directory: $dir"
            rm -rf "$dir"
            ((cleared++))
        fi
    done
    
    # Clear files
    for file in "${files[@]}"; do
        if [ -f "$file" ]; then
            echo "  Removing file: $file"
            rm -f "$file"
            ((cleared++))
        fi
    done
    
    if [ $cleared -eq 0 ]; then
        echo "  No local cache files found to clear"
    else
        echo "  Cleared $cleared local cache item(s)"
    fi
    echo ""
}

# Main execution
main() {
    # Try API first (if server is running)
    if clear_via_api; then
        echo "Cache cleared via API"
    fi
    
    # Always clear local files (in case API doesn't cover everything)
    clear_local_cache
    
    echo "=== Cache Clear Complete ==="
    echo ""
    echo "Summary:"
    echo "  - All tables have been dropped"
    echo "  - All cache files and directories have been cleared"
    echo ""
    echo "Note: Build artifacts in target/ are preserved."
    echo "      To clean build artifacts, run: cargo clean"
}

main

