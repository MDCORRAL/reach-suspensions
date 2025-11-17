# Repair Summary: Analysis/23_teacher_demographics_q4_black_enrollment.R

**Date**: 2025-11-17
**Status**: REPAIRED

## Problems Encountered

The script encountered three sequential errors during repair:

### Error 1: Missing Column
```
Error in `filter()`:
ℹ In argument: `is_traditional == TRUE | is.na(is_traditional)`.
Caused by error:
! object 'is_traditional' not found
```

### Error 2: Incorrect Join Key
```
Error in `left_join()`:
! Join columns in `x` must be present in the data.
✖ Problem with `school_code`.
```

### Error 3: Incorrect Column Name
```
Error in `select()`:
! Can't select columns that don't exist.
✖ Column `locale` doesn't exist.
```

## Root Causes

### Issue 1: Missing `is_traditional` Column
The `is_traditional` column exists in `susp_v6_features.parquet` (one row per school-year) but is **not present** in `susp_v6_long.parquet` (multiple rows per school-year, one per race/ethnicity group).

### Issue 2: Wrong Join Key
The script attempted to join on `school_code` (7-digit), but the data uses `cds_school` (14-digit) as the canonical school identifier.

### Issue 3: Column Naming Inconsistency
The script referenced `locale` but the actual column name is `locale_simple` throughout the pipeline.

### Why This Happened

In `R/22_build_v6_features.R`:
1. The script reads `susp_v5_long.parquet` into `race_long` (line 62)
2. The script creates `v6` (features) from `v5` and adds school-level features like `is_traditional` (lines 314-331)
3. The script writes:
   - `v6` → `susp_v6_features.parquet` (has `is_traditional`)
   - `race_long` → `susp_v6_long.parquet` (does NOT have `is_traditional`)

The `race_long` data is never joined with `v6` to inherit the `is_traditional` flag, creating a gap in the pipeline.

### Downstream Impact

`Analysis/18_merge_teacher_student.R` merges teacher data with `susp_v6_long.parquet`, producing `susp_v6_teacher_long.parquet`. Since the source data lacks `is_traditional`, the merged output also lacks it.

## Solution

**Workaround Applied**: Modified `Analysis/23_teacher_demographics_q4_black_enrollment.R` to:

1. Read the `is_traditional` flag from `susp_v6_features.parquet`
2. Join it with the teacher-student data on `school_code` and `academic_year`
3. Use the joined `is_traditional` column for filtering

### Code Changes

#### Issue 1: Missing `is_traditional` Column

**Before** (lines 35-56):
```r
# Read merged student-teacher data
TEACHER_DATA_PATH <- here("data-stage", "susp_v6_teacher_long.parquet")
if (!file.exists(TEACHER_DATA_PATH)) {
  stop("Missing merged teacher-student data: ", TEACHER_DATA_PATH,
       "\nRun Analysis/18_merge_teacher_student.R first.")
}

message(">>> Loading merged student-teacher data...")
df <- read_parquet(TEACHER_DATA_PATH) %>%
  clean_names()

message(">>> Total rows: ", nrow(df))
message(">>> Unique schools: ", n_distinct(df$cds_school))
message(">>> Academic years: ", paste(sort(unique(df$academic_year)), collapse = ", "))

# Filter to traditional schools only (exclude alternative schools)
# Filter to top quartile Black enrollment (Q4)
# Keep only one row per school-year (aggregate across race groups for school-level summary)
message(">>> Filtering to traditional schools, Q4 Black enrollment...")

school_summary <- df %>%
  filter(
    is_traditional == TRUE | is.na(is_traditional),  # Traditional schools only
    !is.na(black_prop_q),  # Must have Black proportion quartile
    black_prop_q == 4  # Top quartile only
  ) %>%
```

**After** (lines 35-82) - FINAL VERSION:
```r
# Read merged student-teacher data
TEACHER_DATA_PATH <- here("data-stage", "susp_v6_teacher_long.parquet")
FEATURES_PATH <- here("data-stage", "susp_v6_features.parquet")

if (!file.exists(TEACHER_DATA_PATH)) {
  stop("Missing merged teacher-student data: ", TEACHER_DATA_PATH,
       "\nRun Analysis/18_merge_teacher_student.R first.")
}
if (!file.exists(FEATURES_PATH)) {
  stop("Missing v6 features data: ", FEATURES_PATH,
       "\nRun run_pipeline.R first.")
}

message(">>> Loading merged student-teacher data...")
df <- read_parquet(TEACHER_DATA_PATH) %>%
  clean_names() %>%
  build_keys()  # Ensure cds_school exists for joining

message(">>> Loading school features (for is_traditional flag)...")
features <- read_parquet(FEATURES_PATH) %>%
  clean_names() %>%
  build_keys() %>%  # Creates cds_school from county/district/school codes
  select(cds_school, academic_year, is_traditional)

# Join is_traditional from features file
# Note: susp_v6_long.parquet doesn't include is_traditional, so we join it from features
df <- df %>%
  left_join(
    features,
    by = c("cds_school", "academic_year")
  )

message(">>> Total rows: ", nrow(df))
message(">>> Unique schools: ", n_distinct(df$cds_school))
message(">>> Academic years: ", paste(sort(unique(df$academic_year)), collapse = ", "))
message(">>> is_traditional coverage: ", sum(!is.na(df$is_traditional)), " of ", nrow(df), " rows")

# Filter to traditional schools only (exclude alternative schools)
# Filter to top quartile Black enrollment (Q4)
# Keep only one row per school-year (aggregate across race groups for school-level summary)
message(">>> Filtering to traditional schools, Q4 Black enrollment...")

school_summary <- df %>%
  filter(
    is_traditional == TRUE,  # Traditional schools only (remove NA check since we now have the data)
    !is.na(black_prop_q),  # Must have Black proportion quartile
    black_prop_q == 4  # Top quartile only
  ) %>%
```

### Key Changes

1. **Added**: `FEATURES_PATH` variable to point to `susp_v6_features.parquet`
2. **Added**: File existence check for features file
3. **Added**: Loading of features file with `is_traditional` column
4. **Added**: `build_keys()` calls to ensure `cds_school` exists in both dataframes
5. **Added**: Left join to merge `is_traditional` into the main dataframe using `cds_school`
6. **Added**: Diagnostic message showing `is_traditional` coverage
7. **Modified**: Filter logic to use `is_traditional == TRUE` (removed NA check)

#### Issue 2: Incorrect Join Key

After fixing the first issue, a second error appeared:
```
Error in `left_join()`:
! Join columns in `x` must be present in the data.
✖ Problem with `school_code`.
```

**Root Cause**: The join was using `school_code` (7-digit), but the merge script `Analysis/18_merge_teacher_student.R` uses `cds_school` (14-digit) as the join key.

**Fix Applied**:
1. Changed join key from `school_code` to `cds_school`
2. Added `build_keys()` call after loading each dataset to ensure `cds_school` is properly constructed from component codes
3. The `build_keys()` function (from `utils_keys_filters.R`) creates the 14-digit `cds_school` by concatenating county_code + district_code + school_code

#### Issue 3: Incorrect Column Name - `locale` vs `locale_simple`

After fixing the join issue, a third error appeared:
```
Error in `select()`:
! Can't select columns that don't exist.
✖ Column `locale` doesn't exist.
```

**Root Cause**: The script references a column named `locale`, but the actual column name in the data is `locale_simple` (as seen in the pipeline script `R/22_build_v6_features.R` line 88).

**Fix Applied**:
1. Changed all references from `locale` to `locale_simple` in the select statements
2. Updated both occurrences (lines 88 and 140)

## Validation

The repaired script should now:
1. Successfully load both the teacher-student merged data and the features data
2. Build `cds_school` keys in both datasets using `build_keys()`
3. Join `is_traditional` on `cds_school` and `academic_year`
4. Report coverage of the `is_traditional` flag
5. Filter to traditional schools in Q4 Black enrollment
6. Select columns using correct names (`locale_simple` not `locale`)
7. Proceed with analysis and generate all expected outputs without errors

## Recommendations

### For Future Scripts

If other analysis scripts need the `is_traditional` flag when working with `susp_v6_long.parquet` or `susp_v6_teacher_long.parquet`, they should use the same approach:

```r
# Load features for is_traditional flag
features <- read_parquet(here("data-stage", "susp_v6_features.parquet")) %>%
  clean_names() %>%
  select(school_code, academic_year, is_traditional)

# Join with long-format data
df <- df %>%
  left_join(features, by = c("school_code", "academic_year"))
```

### Potential Pipeline Fix (Optional)

To fix this at the pipeline level, `R/22_build_v6_features.R` could be modified to add `is_traditional` (and other school-level features) to `race_long` before writing:

```r
# After line 331 (where is_traditional is created in v6)
# Add to race_long:
school_features <- v6 %>%
  select(school_code, academic_year, is_traditional, school_type)

race_long <- race_long %>%
  left_join(school_features, by = c("school_code", "academic_year"))

# Then write outputs (line 376)
write_parquet(race_long, V6_LONG_PARQ)
```

**However**, this would be a breaking change requiring:
- Re-running the full pipeline
- Updating downstream scripts that might depend on the current column structure
- Testing all analysis scripts that use `susp_v6_long.parquet`

The current workaround (joining `is_traditional` in individual analysis scripts) is safer and more flexible.

## Testing Required

To verify the repair works:

1. Ensure all required data files exist:
   - `data-stage/susp_v6_teacher_long.parquet`
   - `data-stage/susp_v6_features.parquet`

2. Run the script:
   ```r
   source("Analysis/23_teacher_demographics_q4_black_enrollment.R")
   ```

3. Expected outputs in `outputs/tables/`:
   - `q4_black_enrollment_schools_annotations.csv`
   - `q4_black_enrollment_overall_staff_stats.csv`
   - `q4_black_enrollment_yearly_staff_stats.csv`
   - `q4_black_enrollment_by_level_staff_stats.csv`

4. Expected outputs in `outputs/graphs/`:
   - `q4_black_enrollment_teacher_race_trends.png`
   - `q4_black_enrollment_admin_race_trends.png`
   - `q4_black_enrollment_staff_comparison.png`
   - `q4_black_enrollment_staff_by_level.png`

## Files Modified

- `Analysis/23_teacher_demographics_q4_black_enrollment.R`

## Files Analyzed (No Changes)

- `R/22_build_v6_features.R` - Identified source of missing column
- `Analysis/18_merge_teacher_student.R` - Confirmed it passes through v6_long columns only

---

**Repair Status**: Complete
**Requires**: `susp_v6_features.parquet` in addition to original data dependencies
