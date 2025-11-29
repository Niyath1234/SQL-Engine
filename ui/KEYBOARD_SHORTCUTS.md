# Keyboard Shortcuts

## Query Execution

- **Ctrl/Cmd + Enter** - Run query
  - If text is selected: runs only the selected SQL
  - If no selection: runs the entire query in the editor

- **Shift + Enter** - Run query (same behavior as Ctrl/Cmd+Enter)
  - If text is selected: runs only the selected SQL
  - If no selection: runs the entire query in the editor

## Usage Examples

### Run Full Query
1. Type your SQL query
2. Press `Ctrl+Enter` (or `Shift+Enter`)
3. Query executes and results appear below

### Run Selected Query
1. Type multiple queries in the editor:
   ```sql
   SELECT * FROM customers;
   SELECT * FROM orders;
   SELECT * FROM products;
   ```
2. Select one query (e.g., `SELECT * FROM orders;`)
3. Press `Ctrl+Enter` (or `Shift+Enter`)
4. Only the selected query runs

### Run Partial Query
1. Type a complex query:
   ```sql
   SELECT c.name, o.order_id 
   FROM customers c 
   JOIN orders o ON c.customer_id = o.customer_id
   WHERE o.quantity > 5;
   ```
2. Select just the JOIN part to test it:
   ```sql
   SELECT c.name, o.order_id 
   FROM customers c 
   JOIN orders o ON c.customer_id = o.customer_id
   ```
3. Press `Shift+Enter` to run just that part

## Tips

- Use selection to test parts of complex queries
- Use selection to run multiple queries one at a time
- Both shortcuts work the same way - use whichever is more convenient

