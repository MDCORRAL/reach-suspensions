# Fix: Missing Suspension Data in Q4 Black Enrollment Schools

## Issue Summary

The script `Analysis/23_teacher_demographics_q4_black_enrollment.R` was showing missing suspension count and suspension rate data for many schools in the top quartile (Q4) of Black student enrollment.

## Root Cause

The issue was caused by how the script was handling the multi-row structure of the `susp_v6_teacher_features.parquet` data file.

### Data Structure

The `susp_v6_teacher_features.parquet` file contains **multiple rows per school-year**, with one row for each student demographic group:

```
School A, 2023-24, African American students → total_suspensions = 15
School A, 2023-24, White students            → total_suspensions = 8
School A, 2023-24, Hispanic students         → total_suspensions = 12
School A, 2023-24, All Students              → total_suspensions = 45  ← SCHOOL-LEVEL TOTAL
```

### The Problem

The original code (lines 82-89) filtered for Q4 schools and then used `distinct(academic_year, cds_school, .keep_all = TRUE)` to get one row per school-year:

```r
school_summary <- df %>%
  filter(
    is_traditional == TRUE,
    !is.na(black_prop_q),
    black_prop_q == 4
  ) %>%
  # This just takes the FIRST row for each school-year!
  distinct(academic_year, cds_school, .keep_all = TRUE) %>%
  ...
```

**The Problem**: `distinct(.keep_all = TRUE)` simply takes the **first row** it encounters for each school-year combination. This might be:
- A row for "African American" students (race-specific suspensions)
- A row for "White" students (race-specific suspensions)
- A row for "Hispanic" students (race-specific suspensions)
- **NOT** necessarily the "All Students" row with school-level totals

For many Q4 schools, the first row was for a specific demographic group that had:
- **Missing suspension data** (suppressed by CDE for privacy when counts are small)
- **Race-specific counts** (not school-level totals)

This is why you were seeing many NA values in `total_suspensions` and `suspension_rate_percent_total`.

## The Fix

The fix adds a filter to select **only "All Students" aggregate rows** before using `distinct()`:

```r
school_summary <- df %>%
  filter(
    is_traditional == TRUE,
    !is.na(black_prop_q),
    black_prop_q == 4
  ) %>%
  # CRITICAL FIX: Filter to "All Students" aggregate data
  {
    if (!is.na(reporting_col) && reporting_col %in% names(.)) {
      filter(., !!sym(reporting_col) %in% c("All Students", "TA", "Total"))
    } else {
      .
    }
  } %>%
  # Now distinct() gets the right row
  distinct(academic_year, cds_school, .keep_all = TRUE) %>%
  ...
```

### What This Does

1. **Identifies the reporting category column**: Checks if it's called `reporting_category` or `student_group`
2. **Filters to "All Students" rows**: Only keeps rows where the reporting category is:
   - "All Students" (canonical name)
   - "TA" (CDE code for Total/All)
   - "Total" (alternative name)
3. **Then takes distinct rows**: Now `distinct()` operates on "All Students" rows only, ensuring we get school-level suspension totals

## Expected Results After Fix

After running the updated script, you should see:
- **Suspension data present** for most Q4 schools (unless legitimately suppressed by CDE)
- **School-level totals** (all students combined) instead of race-specific counts
- **Diagnostic messages** showing:
  - Which reporting category column was found
  - Available categories in the data
  - Confirmation that "All Students" filtering was applied

## Testing the Fix

To verify the fix worked:

1. **Run the updated script**:
   ```r
   source("Analysis/23_teacher_demographics_q4_black_enrollment.R")
   ```

2. **Check the console output** for messages like:
   ```
   >>> Reporting category column: reporting_category
   >>> Available categories: African American, All Students, Asian, Hispanic, White
   >>> Filtering to 'All Students' aggregate rows...
   >>> Filtered to X school-years in Q4 Black enrollment
   ```

3. **Examine the output file**:
   ```r
   library(readr)
   schools <- read_csv("outputs/tables/q4_black_enrollment_schools_annotations.csv")

   # Check how many schools have suspension data now
   sum(!is.na(schools$total_suspensions))
   sum(!is.na(schools$suspension_rate))

   # Should be much higher than before!
   ```

4. **Sample the data**:
   ```r
   # Look at schools with suspension data
   schools %>%
     filter(!is.na(total_suspensions)) %>%
     select(school_name, total_suspensions, suspension_rate) %>%
     head(10)
   ```

## Why This Matters

Getting school-level totals (rather than race-specific counts) is critical for this analysis because:

1. **Teacher demographics are at the school level**: The teacher data shows the overall staff composition of the school, not broken down by which students they teach

2. **Meaningful comparisons**: Comparing school-level teacher demographics to school-level suspension rates provides the right unit of analysis

3. **Data completeness**: "All Students" aggregates are less likely to be suppressed than small race-specific counts

## Related Files

- **Fixed file**: `Analysis/23_teacher_demographics_q4_black_enrollment.R`
- **Data source**: `data-stage/susp_v6_teacher_features.parquet` (created by `Analysis/22_build_teacher_race_shares.R`)
- **Canonical definitions**: `R/utils_keys_filters.R` (defines "All Students" as part of `ALLOWED_RACES`)

## Future Prevention

This issue could affect other scripts that work with the race-specific long-format data. When working with `susp_v6_long.parquet` or `susp_v6_teacher_features.parquet`:

**Always explicitly filter** to the appropriate `reporting_category` / `student_group` before aggregating or selecting rows:

```r
# For school-level totals:
df %>% filter(reporting_category == "All Students")

# For race-specific analysis (e.g., Black student suspension rates):
df %>% filter(reporting_category == "African American")
```

**Never rely on** `distinct(.keep_all = TRUE)` without first filtering to the correct demographic group!

## Questions or Issues?

If you run into any problems with the fix or have questions, please let me know!
