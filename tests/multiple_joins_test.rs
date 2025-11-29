/// Test multiple joins: 2 joins, 3 joins, 4+ joins
use hypergraph_sql_engine::engine::HypergraphSQLEngine;

#[test]
fn test_two_joins() {
    println!("\n🧪 Testing 2 Joins");
    println!("{}", "=".repeat(60));
    
    let mut engine = HypergraphSQLEngine::new();
    
    // Create tables
    println!("\n📊 Creating tables...");
    
    // Customers table
    let create_customers = r#"
        CREATE TABLE customers (
            customer_id INT,
            name VARCHAR,
            country VARCHAR
        );
    "#;
    engine.execute_query(create_customers).expect("Failed to create customers table");
    println!("  ✅ Created 'customers' table");
    
    // Orders table
    let create_orders = r#"
        CREATE TABLE orders (
            order_id INT,
            customer_id INT,
            product_id INT,
            amount INT
        );
    "#;
    engine.execute_query(create_orders).expect("Failed to create orders table");
    println!("  ✅ Created 'orders' table");
    
    // Products table
    let create_products = r#"
        CREATE TABLE products (
            product_id INT,
            product_name VARCHAR,
            price INT
        );
    "#;
    engine.execute_query(create_products).expect("Failed to create products table");
    println!("  ✅ Created 'products' table");
    
    // Insert data
    println!("\n📝 Inserting data...");
    
    // Insert customers
    let insert_customers = r#"
        INSERT INTO customers VALUES
        (1, 'Alice', 'USA'),
        (2, 'Bob', 'UK'),
        (3, 'Charlie', 'USA');
    "#;
    engine.execute_query(insert_customers).expect("Failed to insert customers");
    println!("  ✅ Inserted 3 customers");
    
    // Insert orders
    let insert_orders = r#"
        INSERT INTO orders VALUES
        (101, 1, 201, 100.0),
        (102, 1, 202, 200.0),
        (103, 2, 201, 150.0),
        (104, 3, 203, 300.0);
    "#;
    engine.execute_query(insert_orders).expect("Failed to insert orders");
    println!("  ✅ Inserted 4 orders");
    
    // Insert products
    let insert_products = r#"
        INSERT INTO products VALUES
        (201, 'Laptop', 1000.0),
        (202, 'Mouse', 20.0),
        (203, 'Keyboard', 50.0);
    "#;
    engine.execute_query(insert_products).expect("Failed to insert products");
    println!("  ✅ Inserted 3 products");
    
    // Test 2 joins: customers JOIN orders JOIN products
    println!("\n🔍 Testing 2 joins query...");
    let query_2_joins = r#"
        SELECT c.name, c.country, o.order_id, o.amount, p.product_name, p.price
        FROM customers c
        JOIN orders o ON c.customer_id = o.customer_id
        JOIN products p ON o.product_id = p.product_id
    "#;
    
    match engine.execute_query(query_2_joins) {
        Ok(result) => {
            println!("  ✅ Query executed successfully!");
            println!("     Rows returned: {}", result.row_count);
            println!("     Execution time: {:.2}ms", result.execution_time_ms);
            
            // Verify we got expected results
            // Alice: 2 orders (101->Laptop, 102->Mouse)
            // Bob: 1 order (103->Laptop)
            // Charlie: 1 order (104->Keyboard)
            // Total: 4 rows
            assert!(result.row_count >= 4, "Expected at least 4 rows, got {}", result.row_count);
            println!("  ✅ Row count check passed");
        }
        Err(e) => {
            eprintln!("  ❌ Query failed: {}", e);
            panic!("2 joins query failed: {}", e);
        }
    }
    
    println!("\n✅ 2 joins test completed successfully!");
}

#[test]
fn test_three_joins() {
    println!("\n🧪 Testing 3 Joins");
    println!("{}", "=".repeat(60));
    
    let mut engine = HypergraphSQLEngine::new();
    
    // Create tables
    println!("\n📊 Creating tables...");
    
    // Departments table
    let create_depts = r#"
        CREATE TABLE departments (
            dept_id INT,
            dept_name VARCHAR
        );
    "#;
    engine.execute_query(create_depts).expect("Failed to create departments table");
    println!("  ✅ Created 'departments' table");
    
    // Employees table
    let create_employees = r#"
        CREATE TABLE employees (
            emp_id INT,
            name VARCHAR,
            dept_id INT,
            manager_id INT
        );
    "#;
    engine.execute_query(create_employees).expect("Failed to create employees table");
    println!("  ✅ Created 'employees' table");
    
    // Projects table
    let create_projects = r#"
        CREATE TABLE projects (
            project_id INT,
            project_name VARCHAR,
            lead_emp_id INT
        );
    "#;
    engine.execute_query(create_projects).expect("Failed to create projects table");
    println!("  ✅ Created 'projects' table");
    
    // Tasks table
    let create_tasks = r#"
        CREATE TABLE tasks (
            task_id INT,
            task_name VARCHAR,
            project_id INT,
            assigned_emp_id INT
        );
    "#;
    engine.execute_query(create_tasks).expect("Failed to create tasks table");
    println!("  ✅ Created 'tasks' table");
    
    // Insert data
    println!("\n📝 Inserting data...");
    
    // Insert departments
    let insert_depts = r#"
        INSERT INTO departments VALUES
        (1, 'Engineering'),
        (2, 'Sales'),
        (3, 'Marketing');
    "#;
    engine.execute_query(insert_depts).expect("Failed to insert departments");
    println!("  ✅ Inserted 3 departments");
    
    // Insert employees
    let insert_employees = r#"
        INSERT INTO employees VALUES
        (101, 'Alice', 1, NULL),
        (102, 'Bob', 1, 101),
        (103, 'Charlie', 2, NULL),
        (104, 'Diana', 3, NULL);
    "#;
    engine.execute_query(insert_employees).expect("Failed to insert employees");
    println!("  ✅ Inserted 4 employees");
    
    // Insert projects
    let insert_projects = r#"
        INSERT INTO projects VALUES
        (201, 'Project Alpha', 101),
        (202, 'Project Beta', 102),
        (203, 'Project Gamma', 103);
    "#;
    engine.execute_query(insert_projects).expect("Failed to insert projects");
    println!("  ✅ Inserted 3 projects");
    
    // Insert tasks
    let insert_tasks = r#"
        INSERT INTO tasks VALUES
        (301, 'Task 1', 201, 101),
        (302, 'Task 2', 201, 102),
        (303, 'Task 3', 202, 102),
        (304, 'Task 4', 203, 103);
    "#;
    engine.execute_query(insert_tasks).expect("Failed to insert tasks");
    println!("  ✅ Inserted 4 tasks");
    
    // Test 3 joins: departments JOIN employees JOIN projects JOIN tasks
    println!("\n🔍 Testing 3 joins query...");
    let query_3_joins = r#"
        SELECT d.dept_name, e.name AS emp_name, p.project_name, t.task_name
        FROM departments d
        JOIN employees e ON d.dept_id = e.dept_id
        JOIN projects p ON e.emp_id = p.lead_emp_id
        JOIN tasks t ON p.project_id = t.project_id
    "#;
    
    match engine.execute_query(query_3_joins) {
        Ok(result) => {
            println!("  ✅ Query executed successfully!");
            println!("     Rows returned: {}", result.row_count);
            println!("     Execution time: {:.2}ms", result.execution_time_ms);
            
            // Verify we got expected results
            // Engineering: Alice -> Project Alpha -> Task 1, Task 2
            // Engineering: Bob -> Project Beta -> Task 3
            // Sales: Charlie -> Project Gamma -> Task 4
            // Total: 4 rows
            assert!(result.row_count >= 4, "Expected at least 4 rows, got {}", result.row_count);
            println!("  ✅ Row count check passed");
        }
        Err(e) => {
            eprintln!("  ❌ Query failed: {}", e);
            panic!("3 joins query failed: {}", e);
        }
    }
    
    println!("\n✅ 3 joins test completed successfully!");
}

#[test]
fn test_four_joins() {
    println!("\n🧪 Testing 4 Joins");
    println!("{}", "=".repeat(60));
    
    let mut engine = HypergraphSQLEngine::new();
    
    // Create tables
    println!("\n📊 Creating tables...");
    
    // Countries table
    let create_countries = r#"
        CREATE TABLE countries (
            country_id INT,
            country_name VARCHAR
        );
    "#;
    engine.execute_query(create_countries).expect("Failed to create countries table");
    println!("  ✅ Created 'countries' table");
    
    // Cities table
    let create_cities = r#"
        CREATE TABLE cities (
            city_id INT,
            city_name VARCHAR,
            country_id INT
        );
    "#;
    engine.execute_query(create_cities).expect("Failed to create cities table");
    println!("  ✅ Created 'cities' table");
    
    // Stores table
    let create_stores = r#"
        CREATE TABLE stores (
            store_id INT,
            store_name VARCHAR,
            city_id INT
        );
    "#;
    engine.execute_query(create_stores).expect("Failed to create stores table");
    println!("  ✅ Created 'stores' table");
    
    // Products table
    let create_products = r#"
        CREATE TABLE products (
            product_id INT,
            product_name VARCHAR,
            category_id INT
        );
    "#;
    engine.execute_query(create_products).expect("Failed to create products table");
    println!("  ✅ Created 'products' table");
    
    // Categories table
    let create_categories = r#"
        CREATE TABLE categories (
            category_id INT,
            category_name VARCHAR
        );
    "#;
    engine.execute_query(create_categories).expect("Failed to create categories table");
    println!("  ✅ Created 'categories' table");
    
    // Sales table
    let create_sales = r#"
        CREATE TABLE sales (
            sale_id INT,
            store_id INT,
            product_id INT,
            amount INT
        );
    "#;
    engine.execute_query(create_sales).expect("Failed to create sales table");
    println!("  ✅ Created 'sales' table");
    
    // Insert data
    println!("\n📝 Inserting data...");
    
    // Insert countries
    let insert_countries = r#"
        INSERT INTO countries VALUES
        (1, 'USA'),
        (2, 'UK'),
        (3, 'Canada');
    "#;
    engine.execute_query(insert_countries).expect("Failed to insert countries");
    println!("  ✅ Inserted 3 countries");
    
    // Insert cities
    let insert_cities = r#"
        INSERT INTO cities VALUES
        (101, 'New York', 1),
        (102, 'London', 2),
        (103, 'Toronto', 3),
        (104, 'Los Angeles', 1);
    "#;
    engine.execute_query(insert_cities).expect("Failed to insert cities");
    println!("  ✅ Inserted 4 cities");
    
    // Insert stores
    let insert_stores = r#"
        INSERT INTO stores VALUES
        (201, 'Store NY', 101),
        (202, 'Store London', 102),
        (203, 'Store Toronto', 103),
        (204, 'Store LA', 104);
    "#;
    engine.execute_query(insert_stores).expect("Failed to insert stores");
    println!("  ✅ Inserted 4 stores");
    
    // Insert categories
    let insert_categories = r#"
        INSERT INTO categories VALUES
        (1, 'Electronics'),
        (2, 'Clothing'),
        (3, 'Food');
    "#;
    engine.execute_query(insert_categories).expect("Failed to insert categories");
    println!("  ✅ Inserted 3 categories");
    
    // Insert products
    let insert_products = r#"
        INSERT INTO products VALUES
        (301, 'Laptop', 1),
        (302, 'T-Shirt', 2),
        (303, 'Pizza', 3),
        (304, 'Phone', 1);
    "#;
    engine.execute_query(insert_products).expect("Failed to insert products");
    println!("  ✅ Inserted 4 products");
    
    // Insert sales
    let insert_sales = r#"
        INSERT INTO sales VALUES
        (401, 201, 301, 1000.0),
        (402, 201, 304, 800.0),
        (403, 202, 302, 50.0),
        (404, 203, 303, 20.0),
        (405, 204, 301, 1200.0);
    "#;
    engine.execute_query(insert_sales).expect("Failed to insert sales");
    println!("  ✅ Inserted 5 sales");
    
    // Test 4 joins: countries JOIN cities JOIN stores JOIN sales JOIN products JOIN categories
    println!("\n🔍 Testing 4 joins query...");
    let query_4_joins = r#"
        SELECT 
            co.country_name,
            ci.city_name,
            s.store_name,
            p.product_name,
            cat.category_name,
            sa.amount
        FROM countries co
        JOIN cities ci ON co.country_id = ci.country_id
        JOIN stores s ON ci.city_id = s.city_id
        JOIN sales sa ON s.store_id = sa.store_id
        JOIN products p ON sa.product_id = p.product_id
        JOIN categories cat ON p.category_id = cat.category_id
    "#;
    
    match engine.execute_query(query_4_joins) {
        Ok(result) => {
            println!("  ✅ Query executed successfully!");
            println!("     Rows returned: {}", result.row_count);
            println!("     Execution time: {:.2}ms", result.execution_time_ms);
            
            // Verify we got expected results
            // Should have 5 rows (one for each sale)
            assert!(result.row_count >= 5, "Expected at least 5 rows, got {}", result.row_count);
            println!("  ✅ Row count check passed");
        }
        Err(e) => {
            eprintln!("  ❌ Query failed: {}", e);
            panic!("4 joins query failed: {}", e);
        }
    }
    
    println!("\n✅ 4 joins test completed successfully!");
}

#[test]
fn test_five_joins() {
    println!("\n🧪 Testing 5 Joins");
    println!("{}", "=".repeat(60));
    
    let mut engine = HypergraphSQLEngine::new();
    
    // Create tables
    println!("\n📊 Creating tables...");
    
    // Create a chain of 6 tables for 5 joins
    let create_tables = vec![
        ("regions", "region_id INT, region_name VARCHAR"),
        ("countries", "country_id INT, country_name VARCHAR, region_id INT"),
        ("cities", "city_id INT, city_name VARCHAR, country_id INT"),
        ("stores", "store_id INT, store_name VARCHAR, city_id INT"),
        ("products", "product_id INT, product_name VARCHAR"),
        ("sales", "sale_id INT, store_id INT, product_id INT, amount INT"),
    ];
    
    for (table_name, columns) in create_tables {
        let sql = format!("CREATE TABLE {} ({});", table_name, columns);
        engine.execute_query(&sql).expect(&format!("Failed to create {} table", table_name));
        println!("  ✅ Created '{}' table", table_name);
    }
    
    // Insert data
    println!("\n📝 Inserting data...");
    
    let insert_data = vec![
        ("regions", "INSERT INTO regions VALUES (1, 'North America'), (2, 'Europe');"),
        ("countries", "INSERT INTO countries VALUES (101, 'USA', 1), (102, 'UK', 2);"),
        ("cities", "INSERT INTO cities VALUES (201, 'New York', 101), (202, 'London', 102);"),
        ("stores", "INSERT INTO stores VALUES (301, 'Store NY', 201), (302, 'Store London', 202);"),
        ("products", "INSERT INTO products VALUES (401, 'Laptop'), (402, 'Phone');"),
        ("sales", "INSERT INTO sales VALUES (501, 301, 401, 1000.0), (502, 302, 402, 800.0);"),
    ];
    
    for (table_name, sql) in insert_data {
        engine.execute_query(sql).expect(&format!("Failed to insert into {}", table_name));
        println!("  ✅ Inserted data into '{}'", table_name);
    }
    
    // Test 5 joins
    println!("\n🔍 Testing 5 joins query...");
    let query_5_joins = r#"
        SELECT 
            r.region_name,
            co.country_name,
            ci.city_name,
            s.store_name,
            p.product_name,
            sa.amount
        FROM regions r
        JOIN countries co ON r.region_id = co.region_id
        JOIN cities ci ON co.country_id = ci.country_id
        JOIN stores s ON ci.city_id = s.city_id
        JOIN sales sa ON s.store_id = sa.store_id
        JOIN products p ON sa.product_id = p.product_id
    "#;
    
    match engine.execute_query(query_5_joins) {
        Ok(result) => {
            println!("  ✅ Query executed successfully!");
            println!("     Rows returned: {}", result.row_count);
            println!("     Execution time: {:.2}ms", result.execution_time_ms);
            
            // Verify we got expected results
            assert!(result.row_count >= 2, "Expected at least 2 rows, got {}", result.row_count);
            println!("  ✅ Row count check passed");
        }
        Err(e) => {
            eprintln!("  ❌ Query failed: {}", e);
            panic!("5 joins query failed: {}", e);
        }
    }
    
    println!("\n✅ 5 joins test completed successfully!");
}

