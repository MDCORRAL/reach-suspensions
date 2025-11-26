# Script 18 Fix: Merging Teacher Data with Race-Specific Student Data

## Problem Identified

**Script 18 was using the wrong input file**, preventing it from merging teacher demographics with race-specific student suspension data.

### Original Issue:
- **Input**: `susp_v6_features.parquet` (wide-format, school-level aggregates)
  - One row per school-year
  - Contains only "All Students" aggregate data
  - No race/ethnicity breakdown
  - Size: ~3.3 MB

- **Result**: Teacher data could only be merged with school-level aggregates, not race-specific suspension patterns

### What Was Needed:
- **Input**: `susp_v6_long.parquet` (long-format, race-specific data)
  - Multiple rows per school-year (one per race/subgroup)
  - Contains `subgroup` or `race` column with specific racial/ethnic categories
  - Enables intersectional analysis of teacher-student demographics
  - Size: ~25 MB

## Changes Made

### 1. Updated Input File (Line 19)
```r
# BEFORE:
V6_PATH <- here("data-stage", "susp_v6_features.parquet")

# AFTER:
V6_PATH <- here("data-stage", "susp_v6_long.parquet")  # CHANGED: Use long format for race-specific data
```

### 2. Updated Output File (Line 20)
```r
# BEFORE:
OUT_PATH <- here("data-stage", "susp_v6_teacher_features.parquet")

# AFTER:
OUT_PATH <- here("data-stage", "susp_v6_teacher_long.parquet")  # CHANGED: Output reflects long format
```

### 3. Removed Incorrect Uniqueness Assertion (Line 56)
```r
# BEFORE:
v6 <- assert_unique_campus(v6, campus_col = "cds_school", year_col = "academic_year")

# AFTER:
# Note: v6_long has multiple rows per school-year (one per race/subgroup), so we don't assert unique campus
```

**Reason**: The long-format data intentionally has multiple rows per school-year (one per race category), so asserting unique campus-year combinations would fail.

### 4. Changed Join Relationship (Line 61)
```r
# BEFORE:
combined <- v6 %>%
  left_join(teacher_summary, by = join_keys, relationship = "one-to-one")

# AFTER:
combined <- v6 %>%
  left_join(teacher_summary, by = join_keys, relationship = "many-to-one")
```

**Reason**:
- **Many student rows** (one per race/subgroup per school-year)
- **One teacher summary row** (per school-year)
- This correctly broadcasts teacher demographics to all student race categories within each school-year

### 5. Enhanced Coverage Reporting (Lines 71-81)
Added dual reporting:
- **Row-level coverage**: How many student subgroup rows have teacher data
- **School-level coverage**: How many unique campus-years have teacher data

This helps distinguish between:
- Schools with no teacher data at all
- Schools with teacher data (replicated across all race rows)

## What This Enables

With the fixed script, you can now analyze:

### 1. **Race-Specific Suspension Patterns by Teacher Demographics**
```r
# Example: Black student suspension rates in schools with different teacher diversity
combined %>%
  filter(subgroup == "Black or African American") %>%
  mutate(teacher_diversity = cut(teacher_staff_count_african_american_share, breaks = 4)) %>%
  group_by(teacher_diversity) %>%
  summarise(avg_suspension_rate = mean(suspension_rate, na.rm = TRUE))
```

### 2. **Intersectional Analysis**
```r
# Example: Do Black students have different suspension rates in schools with more Black teachers?
combined %>%
  filter(subgroup == "Black or African American") %>%
  ggplot(aes(x = teacher_staff_count_african_american_share, y = suspension_rate)) +
  geom_point(alpha = 0.3) +
  geom_smooth(method = "lm")
```

### 3. **Cross-Racial Patterns**
```r
# Example: Compare Hispanic vs White student suspension rates by teacher composition
combined %>%
  filter(subgroup %in% c("Hispanic or Latino", "White")) %>%
  group_by(subgroup, academic_year) %>%
  summarise(
    avg_rate = mean(suspension_rate, na.rm = TRUE),
    avg_hispanic_teacher_share = mean(teacher_staff_count_hispanic_or_latino_share, na.rm = TRUE)
  )
```

## Testing Instructions

Once teacher data is available, test the fix with:

```r
source("Analysis/18_merge_teacher_student.R")
```

Expected output:
```
[18] Loading teacher long parquet ...
[18] Summarising teacher demographics ...
[18] Loading suspension v6 long (race-specific data) ...
[18] Joining teacher metrics onto v6 long (many student race rows to one teacher summary) ...
[18] Teacher coverage: XXXXX of XXXXX student subgroup rows.
[18] Unique school coverage: XXXX of XXXX campus-years.
[18] Wrote data-stage/susp_v6_teacher_long.parquet (rows: XXXXX)
```

### Verification Checks:

1. **Row count should be large** (~25M compressed, many rows when loaded)
2. **Multiple rows per school-year** (one per race category)
3. **Teacher columns replicated** across all race rows within a school-year
4. **Subgroup column present** with values like "Black or African American", "Hispanic or Latino", etc.

## Files Modified

- `Analysis/18_merge_teacher_student.R`

## Next Steps

To run the full pipeline with teacher data:

1. **Copy teacher TXT files** to data-raw:
   ```bash
   # Use the helper script
   ./COPY_TEACHER_FILES.sh
   ```

2. **Run teacher ingestion**:
   ```r
   source("R/01c_ingest_teacher_demographics.R")
   ```

3. **Run the merge**:
   ```r
   source("Analysis/18_merge_teacher_student.R")
   ```

4. **Use the merged data** in analyses:
   ```r
   library(arrow)
   df <- read_parquet("data-stage/susp_v6_teacher_long.parquet")
   ```

## Reference

See canonical analysis example in `Analysis/02_black_rates_by_quartiles.R:33` which also uses `susp_v6_long.parquet` for race-specific analyses.
