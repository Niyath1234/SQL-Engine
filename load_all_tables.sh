#!/bin/bash
# Load all sample tables into the SQL Engine

set -e

# Default server URL
SERVER_URL="${SERVER_URL:-http://localhost:8080}"

echo "=== Loading Sample Tables ==="
echo ""

# Function to execute SQL via API
execute_sql() {
    local sql="$1"
    local description="$2"
    
    echo "Creating $description..."
    
    if command -v curl &> /dev/null; then
        response=$(curl -s -X POST "$SERVER_URL/api/execute" \
            -H "Content-Type: application/json" \
            -d "{\"sql\": \"$sql\"}" 2>&1)
        
        if [ $? -ne 0 ]; then
            echo "  Error: Failed to connect to server at $SERVER_URL"
            echo "  Make sure the server is running: cargo run --bin web_admin"
            exit 1
        fi
        
        # Check if response indicates success
        if echo "$response" | grep -q '"success":true'; then
            echo "  ✓ $description created successfully"
        else
            echo "  ✗ Failed to create $description"
            echo "  Response: $response" | head -3
            return 1
        fi
    else
        echo "  Error: curl not found. Please install curl to use this script."
        exit 1
    fi
}

# Check if server is running
echo "Checking server connection..."
if ! curl -s "$SERVER_URL" > /dev/null 2>&1; then
    echo "Error: Cannot connect to server at $SERVER_URL"
    echo "Please start the server first:"
    echo "  cargo run --bin web_admin"
    exit 1
fi
echo "✓ Server is running"
echo ""

# Create tables (matching the UI sample tables)
# Using standard SQL types: INT/INTEGER for integers, VARCHAR for strings, DOUBLE for floats, DATE for dates
execute_sql "CREATE TABLE customers (customer_id INTEGER, name VARCHAR, email VARCHAR, region_id INTEGER);" "customers table"

execute_sql "CREATE TABLE orders (order_id INTEGER, customer_id INTEGER, product_id INTEGER, quantity INTEGER, order_date VARCHAR);" "orders table"

execute_sql "CREATE TABLE products (product_id INTEGER, name VARCHAR, price DOUBLE, category VARCHAR, warehouse_id INTEGER);" "products table"

execute_sql "CREATE TABLE warehouses (warehouse_id INTEGER, warehouse_name VARCHAR, location VARCHAR);" "warehouses table"

execute_sql "CREATE TABLE regions (region_id INTEGER, region_name VARCHAR, country VARCHAR);" "regions table"

echo ""
echo "=== Inserting Sample Data (100 rows per table) ==="
echo ""

# Insert 100 regions
echo "Inserting 100 regions..."
for i in {1..100}; do
    region_names=("North America" "South America" "Europe" "Asia" "Africa" "Oceania" "Antarctica")
    countries=("USA" "Canada" "UK" "Germany" "France" "Japan" "China" "India" "Brazil" "Australia")
    region_idx=$((($i - 1) % 7))
    country_idx=$((($i - 1) % 10))
    execute_sql "INSERT INTO regions VALUES ($i, '${region_names[$region_idx]}', '${countries[$country_idx]}');" "region $i" > /dev/null 2>&1
    if [ $((i % 20)) -eq 0 ]; then
        echo "  Inserted $i/100 regions..."
    fi
done
echo "  ✓ Inserted 100 regions"

# Insert 100 warehouses
echo "Inserting 100 warehouses..."
cities=("New York" "Los Angeles" "Chicago" "Houston" "Phoenix" "Philadelphia" "San Antonio" "San Diego" "Dallas" "San Jose" "London" "Paris" "Tokyo" "Berlin" "Sydney" "Toronto" "Mumbai" "Shanghai" "São Paulo" "Mexico City")
for i in {1..100}; do
    city_idx=$((($i - 1) % 20))
    execute_sql "INSERT INTO warehouses VALUES ($i, 'Warehouse $i', '${cities[$city_idx]}');" "warehouse $i" > /dev/null 2>&1
    if [ $((i % 20)) -eq 0 ]; then
        echo "  Inserted $i/100 warehouses..."
    fi
done
echo "  ✓ Inserted 100 warehouses"

# Insert 100 customers
echo "Inserting 100 customers..."
first_names=("John" "Jane" "Bob" "Alice" "Charlie" "David" "Emma" "Frank" "Grace" "Henry" "Ivy" "Jack" "Kate" "Liam" "Mia" "Noah" "Olivia" "Paul" "Quinn" "Rachel")
last_names=("Smith" "Johnson" "Williams" "Brown" "Jones" "Garcia" "Miller" "Davis" "Rodriguez" "Martinez" "Hernandez" "Lopez" "Wilson" "Anderson" "Thomas" "Taylor" "Moore" "Jackson" "Martin" "Lee")
for i in {1..100}; do
    first_idx=$((($i - 1) % 20))
    last_idx=$((($i - 1) % 20))
    region_id=$((($i - 1) % 100 + 1))
    first_lower=$(echo "${first_names[$first_idx]}" | tr '[:upper:]' '[:lower:]')
    last_lower=$(echo "${last_names[$last_idx]}" | tr '[:upper:]' '[:lower:]')
    execute_sql "INSERT INTO customers VALUES ($i, '${first_names[$first_idx]} ${last_names[$last_idx]}', '${first_lower}${last_lower}@example.com', $region_id);" "customer $i" > /dev/null 2>&1
    if [ $((i % 20)) -eq 0 ]; then
        echo "  Inserted $i/100 customers..."
    fi
done
echo "  ✓ Inserted 100 customers"

# Insert 100 products
echo "Inserting 100 products..."
product_names=("Laptop" "Mouse" "Keyboard" "Monitor" "Headphones" "Speaker" "Tablet" "Phone" "Camera" "Printer" "Scanner" "Router" "Hard Drive" "SSD" "RAM" "CPU" "GPU" "Motherboard" "Power Supply" "Case")
categories=("Electronics" "Computers" "Accessories" "Audio" "Peripherals" "Storage" "Networking" "Components")
for i in {1..100}; do
    name_idx=$((($i - 1) % 20))
    cat_idx=$((($i - 1) % 8))
    price=$(awk "BEGIN {printf \"%.2f\", ($i * 10.5 + 9.99)}")
    warehouse_id=$((($i - 1) % 100 + 1))
    execute_sql "INSERT INTO products VALUES ($i, '${product_names[$name_idx]} $i', $price, '${categories[$cat_idx]}', $warehouse_id);" "product $i" > /dev/null 2>&1
    if [ $((i % 20)) -eq 0 ]; then
        echo "  Inserted $i/100 products..."
    fi
done
echo "  ✓ Inserted 100 products"

# Insert 100 orders
echo "Inserting 100 orders..."
for i in {1..100}; do
    customer_id=$((($i - 1) % 100 + 1))
    product_id=$((($i - 1) % 100 + 1))
    quantity=$((($i % 10) + 1))
    year=2024
    month=$((($i % 12) + 1))
    day=$((($i % 28) + 1))
    order_date=$(printf "%04d-%02d-%02d" $year $month $day)
    execute_sql "INSERT INTO orders VALUES ($i, $customer_id, $product_id, $quantity, '$order_date');" "order $i" > /dev/null 2>&1
    if [ $((i % 20)) -eq 0 ]; then
        echo "  Inserted $i/100 orders..."
    fi
done
echo "  ✓ Inserted 100 orders"

echo ""
echo "=== Sample Tables Loaded Successfully ==="
echo ""
echo "Tables created with 100 rows each:"
echo "  - customers (100 rows)"
echo "  - orders (100 rows)"
echo "  - products (100 rows)"
echo "  - warehouses (100 rows)"
echo "  - regions (100 rows)"
echo ""
echo "You can now query these tables via the web UI at $SERVER_URL"
echo "or via the API using curl commands."

