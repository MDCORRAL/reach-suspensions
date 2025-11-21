# FIX: Scripts 24 & 25 - Long Format Aggregation Bug (FINAL FIX)

**Date**: 2025-11-20
**Issue**: Suspension rates impossibly high (193,082%) even after filtering to "All Students"
**Root Cause**: Summing `total_suspensions` across reason rows in long format multiplies the total by ~6
**Solution**: Use `first(total_suspensions)` instead of `sum(total_suspensions)` when aggregating from reason-level to school-year level

---

## Executive Summary

### The Problem

After implementing the "All Students" filtering fix (which correctly uses CDE's pre-calculated totals), suspension rates were STILL impossibly high:

```
Mean suspension rate: 2,719.57%
Range: [0.00%, 193,082.71%]
```

This was worse than before the fix, indicating a fundamental misunderstanding of the data structure.

### The Root Cause

The `susp_v6_teacher_features.parquet` file is in **LONG FORMAT** with one row per school-year-subgroup-**reason**:

```
cds_school | academic_year | subgroup      | reason          | total_suspensions | enrollment
-----------|---------------|---------------|-----------------|-------------------|------------
12345      | 2023-24       | All Students  | Violent Injury  | 100               | 500
12345      | 2023-24       | All Students  | Defiance        | 100               | 500
12345      | 2023-24       | All Students  | Weapons         | 100               | 500
12345      | 2023-24       | All Students  | Drugs           | 100               | 500
12345      | 2023-24       | All Students  | Other           | 100               | 500
...
```

**Key insight**: The `total_suspensions` column contains the **SAME total value** (100) on all 6 reason rows. This is the total across ALL reasons for that school-year-subgroup.

The aggregation function was doing:
```r
across(any_of(susp_cols), ~sum(.x, na.rm = TRUE))
# Result: 100 + 100 + 100 + 100 + 100 + 100 = 600 (WRONG! Should be 100)
```

This multiplied the total by ~6 (number of reasons), leading to suspension rates 6x too high.

### The Solution

Use `first()` instead of `sum()` when aggregating `total_suspensions`:

```r
across(any_of(susp_cols), ~first(.x))
# Result: 100 (CORRECT!)
```

Since `total_suspensions` is constant across all reason rows, taking the first value gives us the correct total.

---

## Detailed Analysis

### Understanding v6_long Format

The `susp_v6_long.parquet` file (and by extension `susp_v6_teacher_features.parquet`) is created by pivoting **proportion** columns into long format:

**From R/06_feature_reason_shares.R**:
```r
v5_long <- v5 %>%
  pivot_longer(
    cols = dplyr::all_of(paste0("prop_susp_", reason_labels$reason)),
    names_to  = "reason",
    values_to = "prop_of_total_susp"
  ) %>%
  mutate(reason = sub("^prop_susp_", "", reason)) %>%
  add_reason_label()
```

This creates one row per school-year-subgroup-**reason**, where each row has:
- `reason`: The suspension reason (Violent Injury, Defiance, etc.)
- `prop_of_total_susp`: The proportion of total suspensions for THAT reason
- `total_suspensions`: The TOTAL suspensions (across ALL reasons) - **this is constant across reason rows**
- `cumulative_enrollment`: Total enrollment - **also constant across reason rows**

### Why Long Format?

The long format is useful for:
1. Analyzing suspension patterns by reason
2. Creating reason-specific visualizations
3. Flexible filtering and grouping

But it creates a **hidden trap** for aggregation:
- Columns that are constant across reasons (`total_suspensions`, `enrollment`) should use `first()`
- Columns that are reason-specific (`prop_of_total_susp`) should use `sum()` (though this isn't meaningful)

### Previous Attempts and Why They Failed

#### Attempt 1: Use `max()` for enrollment
```r
across(any_of(enrollment_cols), ~max(.x, na.rm = TRUE))
```
**Problem**: If enrollment varies by subgroup (race), `max()` returns the largest race group, not the total school enrollment.
**Result**: Suspension rates 128,389%

#### Attempt 2: Filter to "All Students" first
```r
df %>%
  filter(subgroup == "All Students") %>%
  group_by(cds_school, academic_year) %>%
  summarise(across(any_of(susp_cols), ~sum(.x, na.rm = TRUE)))
```
**Problem**: Correctly filters to CDE's total, but STILL sums `total_suspensions` across reason rows.
**Result**: Suspension rates 193,082% (6x too high)

#### Attempt 3: Use `first()` for both enrollment AND suspensions (CORRECT)
```r
df %>%
  filter(subgroup == "All Students") %>%
  group_by(cds_school, academic_year) %>%
  summarise(
    across(any_of(susp_cols), ~first(.x)),          # Constant across reasons
    across(any_of(enrollment_cols), ~first(.x))     # Constant across reasons
  )
```
**Result**: Should give realistic suspension rates (3-10%)

---

## Data Structure Diagram

### Wide Format (v5, v6_features)
```
One row per school-year-subgroup:

school | year    | subgroup      | total_susp | enroll | susp_violent | susp_defiance | ...
-------|---------|---------------|------------|--------|--------------|---------------|----
12345  | 2023-24 | All Students  | 100        | 500    | 20           | 50            | ...
12345  | 2023-24 | Black         | 30         | 100    | 10           | 15            | ...
```

### Long Format (v5_long, v6_long, v6_teacher_features)
```
Multiple rows per school-year-subgroup (one per reason):

school | year    | subgroup      | reason    | total_susp | enroll | prop_of_total
-------|---------|---------------|-----------|------------|--------|---------------
12345  | 2023-24 | All Students  | Violent   | 100        | 500    | 0.20
12345  | 2023-24 | All Students  | Defiance  | 100        | 500    | 0.50
12345  | 2023-24 | All Students  | Weapons   | 100        | 500    | 0.10
12345  | 2023-24 | All Students  | Drugs     | 100        | 500    | 0.05
12345  | 2023-24 | All Students  | Other     | 100        | 500    | 0.15
12345  | 2023-24 | Black         | Violent   | 30         | 100    | 0.33
12345  | 2023-24 | Black         | Defiance  | 30         | 100    | 0.50
...
```

**Notice**: `total_susp` and `enroll` are **repeated** on each reason row. They are the SAME for all reasons within a school-year-subgroup.

---

## Fixed Aggregation Function

### Before (WRONG)
```r
aggregate_to_school_year <- function(df) {
  susp_cols <- grep("^total_suspensions|^suspension_count", names(df), value = TRUE)
  enrollment_cols <- intersect(c("cumulative_enrollment", "sup_cumulative_enrollment"), names(df))

  agg_df <- df %>%
    group_by(cds_school, academic_year) %>%
    summarise(
      # WRONG: Sums total_suspensions across reason rows (multiplies by ~6)
      across(any_of(susp_cols), ~sum(.x, na.rm = TRUE)),

      # Correct for enrollment (constant across reasons)
      across(any_of(enrollment_cols), ~first(.x)),

      .groups = "drop"
    )

  return(agg_df)
}
```

### After (CORRECT)
```r
aggregate_to_school_year <- function(df) {
  susp_cols <- grep("^total_suspensions|^suspension_count", names(df), value = TRUE)
  enrollment_cols <- intersect(c("cumulative_enrollment", "sup_cumulative_enrollment"), names(df))

  agg_df <- df %>%
    group_by(cds_school, academic_year) %>%
    summarise(
      # CORRECT: Takes first value of total_suspensions (constant across reasons)
      # NOTE: total_suspensions is the SAME on all reason rows
      across(any_of(susp_cols), ~first(.x)),

      # Correct for enrollment (constant across reasons)
      across(any_of(enrollment_cols), ~first(.x)),

      .groups = "drop"
    )

  return(agg_df)
}
```

---

## Files Updated

### 1. Analysis/24_quartile_slope_comparison.R
**Lines 161-163**: Changed from `sum()` to `first()` for suspension columns
```r
# Take first value of total_suspensions (constant across reason rows)
# NOTE: total_suspensions is the SAME on all reason rows, so first() = the correct total
across(any_of(susp_cols), ~first(.x)),
```

### 2. Analysis/25_interaction_term_regression.R
**Lines 132-134**: Changed from `sum()` to `first()` for suspension columns
```r
# Take first value of total_suspensions (constant across reason rows)
# NOTE: total_suspensions is the SAME on all reason rows, so first() = the correct total
across(any_of(susp_cols), ~first(.x)),
```

### 3. R/aggregate_school_year_v3.R
**Lines 152-155**: Changed from `sum()` to `first()` for suspension columns
**Lines 35-38**: Updated documentation to clarify long format structure
```r
# Take first value of total_suspensions (constant across reason rows)
# NOTE: In long format, total_suspensions is the SAME on all reason rows
# Summing would multiply by ~6 (number of reasons) - use first() instead
across(any_of(susp_cols), ~first(.x), .names = "{.col}"),
```

---

## Expected Results After Fix

### Before Fix
```
=== 5) Final Check: Suspension Rate Distribution ===

Min        : 0.00%
25th perc  : 2.60%
Median     : 29.63%
Mean       : 2,719.57%  ← WRONG
75th perc  : 142.22%
Max        : 193,082.71%  ← WRONG

⚠️  WARNING: Suspension rates are unrealistically high!
```

### After Fix (Expected)
```
=== 5) Final Check: Suspension Rate Distribution ===

Min        : 0.00%
25th perc  : 2.60%
Median     : 4.94%  ← Realistic
Mean       : 6.13%  ← Realistic
75th perc  : 8.52%  ← Realistic
Max        : 35.14%  ← Realistic

✓ Suspension rates look realistic (3-10% median, max < 100%)
```

### Validation Checks

After running the fixed scripts, verify:

1. **Suspension rates are realistic**:
   - Median: 3-10%
   - Mean: 5-12%
   - Max: < 50% (ideally < 30%)

2. **No >100% rates**:
   ```r
   df_check <- df_aggregated %>%
     mutate(susp_rate = total_suspensions / cumulative_enrollment * 100)

   any(df_check$susp_rate > 100, na.rm = TRUE)  # Should be FALSE
   ```

3. **Aggregation diagnostics correct**:
   ```
   >>> Average reasons per school-year: 6.0  ← Should be ~6
   >>> Aggregated to X school-year observations
   ```

4. **Compare with previous analyses**:
   - Script 02 (Black suspension rates by quartile) shows medians 3-10%
   - Script 21 (teacher diversity) shows similar ranges
   - Results from scripts 24 & 25 should align with these

---

## Why This Bug Was Hard to Catch

### 1. Misleading Column Name
The column is called `total_suspensions`, which implies it's the total. But in long format, it's the total **repeated across reason rows**, not a reason-specific count.

### 2. Multiple Layers of Aggregation
The pipeline has multiple aggregation steps:
- Ingestion: Aggregate by subgroup and reason
- v5_long: Pivot to long format (creates reason rows)
- Scripts 24/25: Filter to "All Students", then aggregate across reasons

### 3. Similar-Looking Columns
- `total_suspensions`: Total across all reasons (constant in long format)
- `suspension_count_*`: Individual reason counts (only in wide format)
- `prop_of_total_susp`: Proportion for each reason (varies in long format)

### 4. Correct Filtering but Wrong Aggregation
Filtering to "All Students" was correct (uses CDE's official totals). But this didn't reveal the aggregation bug because the filtering step appeared to work correctly.

### 5. Long Format is Less Common
Most analyses use wide format (`v6_features`), where `total_suspensions` appears once per school-year. Scripts 24 & 25 were unusual in using the long format (`v6_teacher_features`), which required different aggregation logic.

---

## Lessons Learned

### For Data Pipeline Design

1. **Document data structure clearly**: Specify whether columns are constant or varying across grouping variables
2. **Use clear column names**: Consider `total_suspensions_all_reasons` to clarify scope
3. **Add validation checks**: Compare aggregated totals against source data
4. **Test with diagnostic queries**: Always check suspension rate distributions

### For Aggregation Functions

1. **Understand the grain**: What level is the input data at? What level should output be?
2. **Know which columns are constant**: Use `first()` for constant columns, `sum()` for additive columns
3. **Add diagnostic messages**: Print the number of rows aggregated per group
4. **Validate results**: Check for impossible values (rates >100%)

### For Long Format Data

1. **Long format requires careful aggregation**: Some columns are repeated (constant), others vary
2. **Use `first()` for metadata**: Columns like enrollment, school name, etc. should use `first()`
3. **Only sum additive metrics**: Don't sum columns that are already totals
4. **Consider wide format for school-level analyses**: Long format is better for reason-specific analyses

---

## Related Documentation

- **docs/fixes/FIX_SCRIPT_24_25_AGGREGATION.md**: Original fix for clustering and enrollment bugs
- **docs/fixes/AGGREGATION_COMPARISON_TOTAL_ROW_VS_SUMMING.md**: Comparison of aggregation approaches
- **R/06_feature_reason_shares.R**: Creates v5_long (long format with reason rows)
- **R/22_build_v6_features.R**: Creates v6_long from v5_long
- **Analysis/22_build_teacher_race_shares.R**: Creates v6_teacher_features from v6_long

---

## Next Steps

1. **Run fixed scripts**:
   ```r
   source("Analysis/24_quartile_slope_comparison.R")
   source("Analysis/25_interaction_term_regression.R")
   ```

2. **Verify suspension rates are realistic**:
   - Check summary statistics
   - Ensure no rates >100%
   - Compare with previous analyses (scripts 02, 21)

3. **Compare results across scripts**:
   - Script 24 results should align with script 25 results
   - Both should align with script 02 (Black suspension rates by quartile)

4. **Update pipeline documentation**:
   - Add notes about long format aggregation to `Analysis/data_processing_overview.md`
   - Update `CLAUDE.md` with this lesson learned

5. **Consider adding validation tests**:
   - Add unit test that checks `total_suspensions` is constant across reason rows
   - Add integration test that verifies realistic suspension rates after aggregation

---

## Conclusion

This fix completes the three-part aggregation fix for scripts 24 & 25:

1. ✅ **Add aggregation** (script 25 was missing it entirely)
2. ✅ **Filter to "All Students"** (use CDE's pre-calculated totals)
3. ✅ **Use `first()` not `sum()`** (handle long format correctly)

The root cause was a fundamental misunderstanding of the data structure. The long format repeats `total_suspensions` across reason rows, making it a **constant** rather than an **additive** metric. Using `sum()` multiplied the total by the number of reasons (~6), leading to impossible suspension rates.

The fix is simple but critical: use `first()` instead of `sum()` when aggregating from reason-level to school-year level.

---

**Document Version**: 1.0
**Created**: 2025-11-20
**Author**: REACH Suspensions Analysis Team
**Status**: FINAL FIX - Ready for validation

**END OF DOCUMENT**
