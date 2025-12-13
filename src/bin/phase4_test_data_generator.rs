/// Phase 4: Test Data Generator for Production-Scale Testing
/// 
/// Generates large datasets for benchmarking:
/// - 1M, 10M, 100M row datasets
/// - Realistic data distributions
/// - CSV format for easy loading

use std::fs::File;
use std::io::{BufWriter, Write};
use rand::Rng;
use anyhow::Result;

fn main() -> Result<()> {
    println!("╔══════════════════════════════════════════════════════════════════════════════╗");
    println!("║          Phase 4: Test Data Generator                                        ║");
    println!("╚══════════════════════════════════════════════════════════════════════════════╝");
    println!();
    
    let sizes = vec![
        (1_000_000, "1M"),
        (10_000_000, "10M"),
        (100_000_000, "100M"),
    ];
    
    for (size, label) in sizes {
        println!("Generating {} rows dataset...", label);
        generate_dataset(size, &format!("test_data_{}.csv", label))?;
        println!("  ✅ Generated test_data_{}.csv", label);
    }
    
    println!();
    println!("✅ All test datasets generated successfully!");
    
    Ok(())
}

fn generate_dataset(num_rows: usize, filename: &str) -> Result<()> {
    let file = File::create(filename)?;
    let mut writer = BufWriter::new(file);
    
    // Write header
    writeln!(writer, "Year,Industry,Value,Region,Category")?;
    
    let mut rng = rand::thread_rng();
    let industries = vec!["Technology", "Finance", "Healthcare", "Retail", "Manufacturing"];
    let regions = vec!["US", "EU", "ASIA", "LATAM", "MEA"];
    let categories = vec!["A", "B", "C", "D", "E"];
    
    // Write data in chunks for efficiency
    const CHUNK_SIZE: usize = 10_000;
    let mut rows_written = 0;
    
    while rows_written < num_rows {
        let chunk_size = (num_rows - rows_written).min(CHUNK_SIZE);
        
        for _ in 0..chunk_size {
            let year = rng.gen_range(2020..=2024);
            let industry = industries[rng.gen_range(0..industries.len())];
            let value = rng.gen_range(100..=10000);
            let region = regions[rng.gen_range(0..regions.len())];
            let category = categories[rng.gen_range(0..categories.len())];
            
            writeln!(writer, "{},{},{},{},{}", year, industry, value, region, category)?;
            rows_written += 1;
        }
        
        // Flush periodically
        if rows_written % 100_000 == 0 {
            writer.flush()?;
            print!("  Progress: {:.1}%\r", (rows_written as f64 / num_rows as f64) * 100.0);
        }
    }
    
    writer.flush()?;
    println!();
    
    Ok(())
}

