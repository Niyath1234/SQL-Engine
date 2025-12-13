#!/bin/bash
# Audit script to find all unwrap(), expect(), panic!, unreachable! usage
# Helps track progress on Phase 0 safety enforcement

echo "=== Unwrap/Expect/Panic Audit ==="
echo ""

echo "Execution module:"
grep -rn "\.unwrap()\|\.expect(\|panic!\|unreachable!" src/execution/ | wc -l
echo ""

echo "Query module:"
grep -rn "\.unwrap()\|\.expect(\|panic!\|unreachable!" src/query/ | wc -l
echo ""

echo "Top 20 files with most unsafe patterns:"
grep -rn "\.unwrap()\|\.expect(\|panic!\|unreachable!" src/ | \
    cut -d: -f1 | sort | uniq -c | sort -rn | head -20
echo ""

echo "Breakdown by pattern:"
echo "unwrap():"
grep -rn "\.unwrap()" src/execution/ src/query/ | wc -l
echo "expect():"
grep -rn "\.expect(" src/execution/ src/query/ | wc -l
echo "panic!:"
grep -rn "panic!" src/execution/ src/query/ | wc -l
echo "unreachable!:"
grep -rn "unreachable!" src/execution/ src/query/ | wc -l

