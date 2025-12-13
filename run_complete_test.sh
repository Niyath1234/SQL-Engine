#!/bin/bash

PORT=8115
BASE="http://localhost:$PORT"

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║  COMPLETE PIPELINE TEST                                      ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

# Step 1: Load Data
echo "STEP 1: Load Data via API"
curl -s -X POST $BASE/api/ingest/load_sample_data \
  -H "Content-Type: application/json" -d '{}' | jq -r \
  '"✅ Status: " + .status, (.tables_loaded[] | "  • \(.table): \(.records) records")'
echo ""

# Step 2: Sync Schema
echo "STEP 2: Sync Schema"
curl -s -X POST $BASE/api/schema/sync \
  -H "Content-Type: application/json" -d '{}' | jq -r '"✅ " + .status'
echo ""
echo ""

# Question 1
echo "═══════════════════════════════════════════════════════════"
echo "QUESTION 1: What are the total sales by customer?"
echo "═══════════════════════════════════════════════════════════"
curl --max-time 60 -s -X POST $BASE/api/ask \
  -H "Content-Type: application/json" \
  -d '{"intent":"What are the total sales by customer?","ollama_url":"http://localhost:11434","model":"llama3.2"}' | \
  jq -r 'if .status == "success" then 
    "✅ SUCCESS!", 
    "", 
    "SQL: \(.sql)", 
    "", 
    "Results (\(.result.row_count) rows):",
    (.result.rows[] | "  • \(.[1]): $\(.[0])")
  else 
    "❌ " + .status + ": " + ((.error // .reason // "unknown")[:100])
  end'
echo ""
echo ""

# Question 2
echo "═══════════════════════════════════════════════════════════"
echo "QUESTION 2: How many orders does each customer have?"
echo "═══════════════════════════════════════════════════════════"
curl --max-time 60 -s -X POST $BASE/api/ask \
  -H "Content-Type: application/json" \
  -d '{"intent":"How many orders does each customer have?","ollama_url":"http://localhost:11434","model":"llama3.2"}' | \
  jq -r 'if .status == "success" then 
    "✅ SUCCESS!", 
    "",
    "SQL: \(.sql)",
    "",
    "Results (\(.result.row_count) rows):",
    (.result.rows[] | "  • \(.[1]): \(.[0]) orders")
  else 
    "❌ " + .status + ": " + ((.error // .reason // "unknown")[:100])
  end'
echo ""
echo ""

# Question 3
echo "═══════════════════════════════════════════════════════════"
echo "QUESTION 3: What is the total revenue?"
echo "═══════════════════════════════════════════════════════════"
curl --max-time 60 -s -X POST $BASE/api/ask \
  -H "Content-Type: application/json" \
  -d '{"intent":"What is the total revenue?","ollama_url":"http://localhost:11434","model":"llama3.2"}' | \
  jq -r 'if .status == "success" then 
    "✅ SUCCESS!", 
    "",
    "SQL: \(.sql)",
    "",
    "Result: $\(.result.rows[0][0])"
  else 
    "❌ " + .status + ": " + ((.error // .reason // "unknown")[:100])
  end'
echo ""
echo ""

# Question 4
echo "═══════════════════════════════════════════════════════════"
echo "QUESTION 4: List all customers"
echo "═══════════════════════════════════════════════════════════"
curl --max-time 60 -s -X POST $BASE/api/ask \
  -H "Content-Type: application/json" \
  -d '{"intent":"List all customers","ollama_url":"http://localhost:11434","model":"llama3.2"}' | \
  jq -r 'if .status == "success" then 
    "✅ SUCCESS!", 
    "",
    "SQL: \(.sql)",
    "",
    "Results (\(.result.row_count) rows):",
    (.result.rows[] | "  • \(.[1])")
  else 
    "❌ " + .status + ": " + ((.error // .reason // "unknown")[:100])
  end'
echo ""
echo ""

# Question 5
echo "═══════════════════════════════════════════════════════════"
echo "QUESTION 5: Show me products with their categories"
echo "═══════════════════════════════════════════════════════════"
curl --max-time 60 -s -X POST $BASE/api/ask \
  -H "Content-Type: application/json" \
  -d '{"intent":"Show me products with their categories","ollama_url":"http://localhost:11434","model":"llama3.2"}' | \
  jq -r 'if .status == "success" then 
    "✅ SUCCESS!", 
    "",
    "SQL: \(.sql)",
    "",
    "Results (\(.result.row_count) rows, showing first 5):",
    (.result.rows[0:5][] | "  • \(.[0]): \(.[1])")
  else 
    "❌ " + .status + ": " + ((.error // .reason // "unknown")[:100])
  end'
echo ""
echo ""

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║  TEST COMPLETE - SUMMARY                                     ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""
echo "✅ Tested 5 basic questions via complete pipeline"
echo ""
echo "🎯 Pipeline: API → Data → Schema → LLM → Corrector"
echo "   → Normalizer → Validator → SQL → Engine → Results"


