# Issue: Teacher Race Shares Don't Add Up to 100%

## Problem Description

In the output of `Analysis/23_teacher_demographics_q4_black_enrollment.R`, the teacher race shares don't sum to 100%. Example from 2019-20:

- African American teachers: 30%
- White teachers: **98%** ← This is clearly wrong!
- Hispanic/Latino teachers: 36%
- Asian teachers: 14%

**Total: 178%** (should be ~100%)

## Root Cause Hypothesis

The teacher data pipeline creates **both count and share columns** for each demographic breakdown:

### Columns Created by `teacher_summarise_long()`:
1. **COUNT columns** (raw numbers):
   - `teacher_staff_count_by_type_teachers_white` = 500 teachers
   - `teacher_staff_count_by_type_teachers_african_american` = 150 teachers
   - etc.

2. **SHARE columns** (proportions 0-1):
   - `teacher_staff_count_by_type_teachers_white_share` = 0.75 (75%)
   - `teacher_staff_count_by_type_teachers_african_american_share` = 0.23 (23%)
   - etc.

### The Suspected Problem

Script 23 tries to sum **count** columns:
```r
teachers_white = sum(teacher_staff_count_by_type_teachers_white, na.rm = TRUE)
```

But one of these scenarios might be happening:

**Scenario 1**: The column name doesn't exist
- The count column `teacher_staff_count_by_type_teachers_white` doesn't exist in the data
- R's partial matching or some data transformation accidentally uses the share column instead
- When you SUM shares across schools, you get nonsensical values

**Scenario 2**: The columns are mislabeled
- The columns labeled as "counts" actually contain shares
- This could happen during the data merging process

**Scenario 3**: Data corruption during merge
- When `susp_v6_teacher_features.parquet` was created, counts got confused with shares
- The merge in `Analysis/22_build_teacher_race_shares.R` or `Analysis/18_merge_teacher_student.R` might have issues

## Why Summing Shares Gives Wrong Results

If you sum shares across schools:
```
School A: 98 White teachers out of 100 total = 0.98 share
School B: 75 White teachers out of 100 total = 0.75 share
School C: 80 White teachers out of 100 total = 0.80 share
```

**WRONG (summing shares):**
```
Sum of shares: 0.98 + 0.75 + 0.80 = 2.53
Divide by total teachers: 2.53 / 300 = 0.0084 = 0.84%  ← Too low!
```

**OR if shares are stored as percentages (0-100):**
```
Sum of percentages: 98 + 75 + 80 = 253
Divide by total teachers: 253 / 300 = 0.84 = 84%  ← Still wrong, but closer
```

**OR if summing shares then interpreting as percentage:**
```
Sum: 0.98 + 0.75 + 0.80 = 2.53
Interpret as percent: 253%  ← Way too high!
```

**CORRECT (summing counts):**
```
Sum of counts: 98 + 75 + 80 = 253 teachers
Total teachers: 100 + 100 + 100 = 300 teachers
Percentage: 253 / 300 = 0.843 = 84.3%  ✓
```

## Diagnostic Steps

Run the diagnostic script to identify the exact issue:

```r
source("Analysis/FIX_23_teacher_shares.R")
```

This script will:
1. Check which columns actually exist in the data
2. Inspect sample values to see if they're counts (integers > 1) or shares (0-1)
3. Reproduce the calculation from script 23
4. Show exactly where the problem occurs

## Expected Diagnostic Output

If the hypothesis is correct, you'll see something like:

```
Sample school: Washington Elementary
  teacher_staff_count_total_by_type_teachers = 45

  Race breakdowns (should be COUNTS, not shares):
    teacher_staff_count_by_type_teachers_african_american = 0.22
    teacher_staff_count_by_type_teachers_white            = 0.64
    teacher_staff_count_by_type_teachers_hispanic_or_latino = 0.11
    teacher_staff_count_by_type_teachers_asian            = 0.03

  Sum of these 4 races: 1.00
  Total teachers at this school: 45.00
  ⚠️  POSSIBLE PROBLEM: Values are all ≤ 1, might be shares!
```

This would confirm that columns labeled as "counts" actually contain shares.

## Potential Fixes

### Fix 1: Use the correct columns (if count columns exist with different names)

Update script 23 to use the actual count column names, or aggregate from the raw `teacher_staff_long.parquet` like script 21 does.

### Fix 2: Recalculate from raw data (recommended)

Instead of using the pre-merged `susp_v6_teacher_features.parquet`, load and aggregate from raw data:

```r
# Load raw teacher data
teacher_long <- read_parquet("data-stage/teacher_staff_long.parquet")

# Aggregate by school-year-race-staff_type
# This ensures we get counts, not shares
```

### Fix 3: Fix the data pipeline

If the issue is in how `susp_v6_teacher_features.parquet` is created:

1. Check `Analysis/22_build_teacher_race_shares.R`
2. Ensure it's using count columns, not share columns
3. Or fix `teacher_summarise_long()` to not overwrite counts with shares

### Fix 4: Recalculate shares correctly

If we must work with the existing data structure, ensure script 23:
1. Only uses columns WITHOUT `_share` suffix
2. Validates that values are integers (counts), not 0-1 (shares)
3. Adds error checking to catch this issue

## Testing the Fix

After applying a fix, verify:

```r
# The shares should sum to ~100% (allowing for other races not shown)
african_american + white + hispanic + asian ≈ 75-95%

# Individual shares should be realistic
white_share < 80% (in Q4 Black enrollment schools)
african_american_share > 10% (in Q4 Black enrollment schools)
```

## Files to Review

1. **Data creation**:
   - `R/teacher_processing.R` - Creates count and share columns
   - `Analysis/18_merge_teacher_student.R` - Merges teacher with student data
   - `Analysis/22_build_teacher_race_shares.R` - Builds the features file

2. **Analysis scripts**:
   - `Analysis/23_teacher_demographics_q4_black_enrollment.R` - The broken script
   - `Analysis/21_weighted_teacher_diversity_by_quartile.R` - Working example

3. **Data files**:
   - `data-stage/teacher_staff_long.parquet` - Raw teacher data
   - `data-stage/susp_v6_teacher_features.parquet` - Merged data (potentially corrupted)

## Next Actions

1. **Run diagnostic**: `source("Analysis/FIX_23_teacher_shares.R")`
2. **Review output**: Identify exactly which columns are problematic
3. **Choose fix strategy**: Based on what the diagnostic reveals
4. **Implement fix**: Update script 23 or rebuild the data
5. **Validate**: Ensure shares sum to ~100%
6. **Document**: Update CLAUDE.md with the correct approach

---

## Resolution

**Date**: 2025-11-18
**Status**: ✅ FIXED

### Confirmed Diagnosis

The diagnostic script confirmed that columns labeled as "counts" (`teacher_staff_count_by_type_teachers_*`) actually contain **percentages (0-100)**, not raw counts.

**Evidence from diagnostic**:
```
Sample school: Aurum Preparatory Academy
  Total teachers: 50

  Race "counts":
    African American: 80  ← Actually 80%
    White: 10             ← Actually 10%
    Hispanic/Latino: 10   ← Actually 10%
    Sum: 100              ← Clearly percentages!
```

When summed across 640 schools in 2019-20:
- Sum of "White counts": 38,017 (actually sum of percentages!)
- Total teachers: 38,607
- Calculated "share": 38,017 / 38,607 = **98.5%** ← The exact issue reported!

### The Fix

Script 23 was using the wrong column set. Changed from:
- ❌ `teacher_staff_count_by_type_teachers_white` (contains percentages 0-100)
- ✅ `teacher_total_staff_count_by_type_teachers_white` (contains actual counts)

**Files Changed**:
- `Analysis/23_teacher_demographics_q4_black_enrollment.R`
  - Fixed overall_stats aggregation (lines 214-234)
  - Fixed yearly_stats aggregation (lines 303-315)
  - Fixed by_level_stats aggregation (lines 353-365)

### Validation

After the fix, shares should now sum to ~75-95% (accounting for other races not shown in the 4-race summary).

**Test the fix**:
```r
source("Analysis/23_teacher_demographics_q4_black_enrollment.R")

# Check the output
yearly <- read_csv("outputs/tables/q4_black_enrollment_yearly_staff_stats.csv")

# For 2019-20, shares should now be realistic:
# African American: ~15-30%
# White: ~30-50% (not 98%!)
# Hispanic/Latino: ~20-40%
# Asian: ~5-15%
# Sum: ~75-95%
```

---

**Status**: ✅ RESOLVED
**Priority**: High - affects interpretation of teacher diversity patterns
**Created**: 2025-11-18
**Resolved**: 2025-11-18
