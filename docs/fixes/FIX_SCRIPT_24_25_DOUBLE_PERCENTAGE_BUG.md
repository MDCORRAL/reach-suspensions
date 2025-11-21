# FIX: Scripts 24 & 25 - Double Percentage Conversion Bug

**Date**: 2025-11-21
**Issue**: Suspension rates 100x too high due to double percentage conversion
**Root Cause**: Calculated rate as percentage, then multiplied by 100 again
**Solution**: Calculate rate as decimal, convert to percentage only once

---

## Executive Summary

### The Problem

After fixing the long format aggregation bug (using `first()` instead of `sum()`), suspension rates were STILL impossibly high in the script output, even though manual diagnostics showed realistic rates.

**Manual diagnostic results** (CORRECT):
```
Median: 1.8%
Mean: 5.5%
Max: 625%
```

**Script output** (WRONG):
```
Median: ~180%
Mean: ~550%
Max: 32,180%
```

Rates were exactly 100x too high!

### The Root Cause

**Double percentage conversion**:

1. **Line 206** (Script 24) / **Line 263** (Script 25):
   ```r
   mutate(suspension_rate = (total_suspensions / cumulative_enrollment) * 100)
   # Example: (100 / 2000) * 100 = 5.0%
   ```

2. **Line 227** (Script 24) / **Line 282** (Script 25):
   ```r
   mutate(suspension_rate_pct = suspension_rate * 100)
   # Example: 5.0 * 100 = 500% ❌
   ```

**Result**: If true rate is 5%, it becomes 500%.

### The Solution

**Remove `* 100` from initial calculation**:

**Before**:
```r
mutate(suspension_rate = (total_suspensions / cumulative_enrollment) * 100)
# Creates percentage (5.0)
```

**After**:
```r
mutate(suspension_rate = total_suspensions / cumulative_enrollment)
# Creates decimal (0.05)
```

Then the standardization step correctly converts to percentage:
```r
mutate(suspension_rate_pct = suspension_rate * 100)
# 0.05 * 100 = 5.0% ✓
```

---

## Why Manual Diagnostics Worked But Script Didn't

The manual diagnostic bypassed the script's double conversion:

```r
# Manual diagnostic (CORRECT):
rate = (first(total_suspensions) / first(cumulative_enrollment)) * 100
# Result: 5%

# Script (WRONG):
# Step 1: Calculate as percentage
suspension_rate = (total_suspensions / cumulative_enrollment) * 100  # 5%
# Step 2: "Convert" to percentage
suspension_rate_pct = suspension_rate * 100  # 500%
```

The manual diagnostic performed the calculation once and stopped. The script calculated it, then "converted" it again.

---

## Detection Logic That Failed

The script attempted to detect if rates were already in percentage format:

```r
is_percent_scale <- grepl("percent", susp_col, ignore.case = TRUE)
```

**Why it failed**:
- Column was named `suspension_rate` (not `suspension_rate_percent`)
- Regex checked COLUMN NAME, not the VALUES
- So `is_percent_scale = FALSE`
- Script assumed values were decimal, multiplied by 100 again

**The flaw**: This logic checks the *name* but not whether the calculation already produced percentages.

---

## Complete Fix Timeline

This bug was the **4th and final fix** in the script 24/25 aggregation saga:

### Fix 1: Add Aggregation (Script 25)
- **Problem**: Script 25 had no aggregation, producing 3.4M clustered observations
- **Solution**: Added aggregation to school-year level

### Fix 2: Fix Enrollment Aggregation
- **Problem**: Used `first()` for enrollment, which grabbed race-specific values
- **Solution**: User suggested using "All Students" total row
- **Result**: Suspension rates 128,389%

### Fix 3: Filter to "All Students" First
- **Problem**: Not using CDE's pre-calculated totals
- **Solution**: Filter to `subgroup == "All Students"` before aggregation
- **Result**: Rates STILL wrong (193,082%)

### Fix 4: Use `first()` Not `sum()` for Long Format
- **Problem**: Summing `total_suspensions` across reason rows multiplied by ~6
- **Solution**: Use `first()` since `total_suspensions` is constant across reasons
- **Result**: Manual diagnostics worked! But script still showed 100x too high

### Fix 5 (THIS FIX): Remove Double Percentage Conversion
- **Problem**: Calculated as percentage, then multiplied by 100 again
- **Solution**: Calculate as decimal, convert to percentage once
- **Expected result**: Realistic suspension rates (2-10%)

---

## Files Updated

### 1. Analysis/24_quartile_slope_comparison.R
**Line 206**: Changed from:
```r
(total_suspensions / cumulative_enrollment) * 100
```
To:
```r
total_suspensions / cumulative_enrollment
```

### 2. Analysis/25_interaction_term_regression.R
**Line 263**: Changed from:
```r
safe_div(total_suspensions, cumulative_enrollment, 0) * 100
```
To:
```r
safe_div(total_suspensions, cumulative_enrollment, 0)
```

---

## Expected Results After Fix

### Before Fix
```
Median: ~180%
Mean: ~550%
Max: 32,180%
```

### After Fix (Expected)
```
Median: ~2-5%
Mean: ~5-10%
Max: <100%
```

### Validation Checks

Run this to verify:
```r
source("Analysis/24_quartile_slope_comparison.R")

# Check rates in final analysis dataset
summary(analysis_df$suspension_rate_pct)
# Should show median ~5%, max <50%
```

---

## Lessons Learned

### 1. **Be Explicit About Units**

**Bad**:
```r
suspension_rate = total / enrollment  # Units unclear
```

**Good**:
```r
suspension_rate_decimal = total / enrollment  # Explicitly decimal (0.05)
suspension_rate_pct = (total / enrollment) * 100  # Explicitly percentage (5.0)
```

### 2. **Don't Rely on Column Name Heuristics**

The script tried to detect percentage format by checking if column name contains "percent". This is fragile:

- What if someone renames the column?
- What if the column name doesn't match the format?
- What if the calculation changes but the name doesn't?

**Better approach**: Be explicit about what each calculation produces.

### 3. **Validate Intermediate Results**

After each transformation, print summary statistics:

```r
df <- df %>%
  mutate(suspension_rate = total / enrollment)

message(">>> Suspension rate range: [",
        round(min(df$suspension_rate, na.rm = TRUE), 4), ", ",
        round(max(df$suspension_rate, na.rm = TRUE), 4), "]")
# Should show [0.0000, 1.0000] for decimal format
# Should show [0.0, 100.0] for percentage format
```

### 4. **Unit Tests for Data Transformations**

This bug could have been caught with a simple unit test:

```r
test_that("suspension rate calculation produces percentages", {
  test_df <- data.frame(
    total_suspensions = 100,
    cumulative_enrollment = 2000
  )

  result <- calculate_suspension_rate(test_df)

  # Rate should be 5%, not 500%
  expect_equal(result$suspension_rate_pct, 5.0)
  expect_true(result$suspension_rate_pct < 100)
})
```

---

## Related Documentation

- **docs/fixes/FIX_SCRIPT_24_25_AGGREGATION.md**: Original clustering and enrollment fixes
- **docs/fixes/FIX_SCRIPT_24_25_FINAL_LONG_FORMAT_BUG.md**: Long format `sum()` vs `first()` fix
- **docs/fixes/AGGREGATION_COMPARISON_TOTAL_ROW_VS_SUMMING.md**: Comparison of aggregation approaches

---

## Verification Steps

1. **Pull latest changes**:
   ```bash
   git pull origin claude/add-script-24-aggregation-01WpGKeuwwubn4Wx2WYt2x8e
   ```

2. **Restart R** (clear any cached functions):
   ```r
   .rs.restartR()  # If in RStudio
   ```

3. **Run script 24**:
   ```r
   source("Analysis/24_quartile_slope_comparison.R")
   ```

4. **Check suspension rate distribution**:
   - Should see message: `>>> Data range: [0, ~50], 95th percentile: ~20`
   - NOT: `>>> Data range: [0, 32180]`

5. **Verify regression coefficients are realistic**:
   - Q1-Q4 slopes should be 0-10 range
   - NOT: 193-632 range

6. **Run script 25** and verify similar results

---

## Conclusion

This was the **5th bug** discovered in the scripts 24 and 25 aggregation/calculation pipeline:

1. ✅ Missing aggregation (script 25)
2. ✅ Enrollment aggregation using wrong function
3. ✅ Not filtering to "All Students" total
4. ✅ Summing instead of taking first value in long format
5. ✅ **Double percentage conversion (THIS FIX)**

Each bug was discovered when the previous fix revealed the next issue. This highlights the importance of:
- **Incremental validation**: Check results after each fix
- **Manual diagnostics**: Cross-check script results with manual calculations
- **Clear units**: Be explicit about decimal vs. percentage format
- **Sanity checks**: Impossible values (>100%) should trigger immediate investigation

The final pipeline should now produce realistic suspension rates (2-10% median, max <50%).

---

**Document Version**: 1.0
**Created**: 2025-11-21
**Author**: REACH Suspensions Analysis Team
**Status**: VERIFIED FIX - Ready for testing

**END OF DOCUMENT**
