use anyhow::Result;

fn main() -> Result<()> {
    println!("Hypergraph SQL Engine - CLI Mode");
    println!("{}", "=".repeat(80));
    println!("Use the 'compare_engines' binary for benchmarking:");
    println!("  cargo run --bin compare_engines -- <csv_file> [query1] [query2] ...");
    println!();
    println!("Example:");
    println!("  cargo run --bin compare_engines -- data.csv \"SELECT * FROM table LIMIT 10\"");
    println!();
    
    Ok(())
}
