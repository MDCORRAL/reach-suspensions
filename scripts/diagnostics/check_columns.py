#!/usr/bin/env python3
"""Check if diagnostic script pattern matches regression pattern"""

import pyarrow.parquet as pq
import re
from pathlib import Path

# Load parquet file
data_path = Path("data-stage/susp_v6_teacher_features.parquet")
table = pq.read_table(data_path)
all_columns = table.column_names

print("=" * 70)
print("COMPARING COLUMN PATTERNS")
print("=" * 70)
print()

# Diagnostic pattern
diagnostic_pattern = r"teacher.*_(african_american|asian|hispanic|white|filipino|american_indian|native_hawaiian|pacific_islander|two_or_more).*_share$"

# Regression pattern (using TEACHER_RACE_SLUGS)
TEACHER_RACE_SLUGS = [
    "african_american",
    "asian",
    "filipino",
    "hispanic_or_latino",
    "american_indian_or_alaska_native",
    "native_hawaiian_pacific_islander",
    "pacific_islander",
    "white",
    "two_or_more_races",
    "not_reported"
]
regression_pattern = r"teacher.*_(" + "|".join(TEACHER_RACE_SLUGS) + r")_share$"

# Find matching columns
diagnostic_cols = [c for c in all_columns if re.match(diagnostic_pattern, c, re.IGNORECASE)]
regression_cols = [c for c in all_columns if re.match(regression_pattern, c, re.IGNORECASE)]

print("1. DIAGNOSTIC PATTERN MATCHES")
print("-" * 70)
print(f"Total columns: {len(diagnostic_cols)}")
print("Sample (first 5):")
for col in diagnostic_cols[:5]:
    print(f"  - {col}")
print()

print("2. REGRESSION PATTERN MATCHES")
print("-" * 70)
print(f"Total columns: {len(regression_cols)}")
print("Sample (first 5):")
for col in regression_cols[:5]:
    print(f"  - {col}")
print()

# Find differences
in_regression_not_diagnostic = set(regression_cols) - set(diagnostic_cols)
in_diagnostic_not_regression = set(diagnostic_cols) - set(regression_cols)

print("3. DIFFERENCES")
print("-" * 70)
if in_regression_not_diagnostic:
    print("⚠️  Columns matched by REGRESSION but NOT by DIAGNOSTIC:")
    for col in sorted(in_regression_not_diagnostic):
        print(f"  - {col}")
    print()
else:
    print("✓ No columns in regression but not in diagnostic")
    print()

if in_diagnostic_not_regression:
    print("⚠️  Columns matched by DIAGNOSTIC but NOT by REGRESSION:")
    for col in sorted(in_diagnostic_not_regression):
        print(f"  - {col}")
    print()
else:
    print("✓ No columns in diagnostic but not in regression")
    print()

if not in_regression_not_diagnostic and not in_diagnostic_not_regression:
    print("✓ ✓ ✓ PATTERNS MATCH EXACTLY ✓ ✓ ✓")
    print()
else:
    print("❌ PATTERNS DO NOT MATCH - DIAGNOSTIC SCRIPT MAY BE INACCURATE")
    print()
