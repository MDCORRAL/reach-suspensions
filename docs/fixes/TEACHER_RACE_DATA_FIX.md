# Fix: Teacher Race/Ethnicity Data Not Being Extracted

## Problem

After fixing script 18 to use race-specific student data (`susp_v6_long.parquet`), analysis scripts like `Analysis/21_weighted_teacher_diversity_by_quartile.R` were still unable to find teacher race/ethnicity columns.

The output showed:
```
>>> Found 116 teacher demographic columns
>>> Found 0 teacher race columns
>>> Identified key columns:
>>>   African American: NOT FOUND
>>>   White: NOT FOUND
>>>   Hispanic: NOT FOUND
>>>   Asian: NOT FOUND
>>> WARNING: Teacher race/ethnicity data not available
```

## Root Cause

**Bug in `R/teacher_processing.R` lines 264 and 283:**

The teacher summarization function had the `coalesce()` arguments in the wrong order:

```r
# WRONG: Uses staff type (Teachers, Administrators) instead of race
race_label = dplyr::coalesce(reporting_category_description, race_ethnicity)
```

This line prioritizes `reporting_category_description` (which contains staff types like "Teachers", "Administrators", "Pupil Services") over `race_ethnicity` (which contains actual race categories like "African American", "White", "Hispanic or Latino", etc.).

Since `reporting_category_description` is never NA, the function **never used the actual race data**, instead creating "race breakdowns" based on staff types.

## Evidence

The teacher ingestion log showed race data WAS present:
```
[01c] Sample: Teachers vs. Administrators by race (2024-25):
# A tibble: 9 × 4
  race_ethnicity                     ADM    TCH TCH_to_ADM_ratio
  <fct>                            <dbl>  <dbl>            <dbl>
1 American Indian or Alaska Native    91   1702            18.7
2 Asian                             1138  20000            17.6
3 Filipino                           245   4799            19.6
...
```

But the summarized output only had staff type and gender breakdowns, no race:
```
>>> teacher_staff_count_total
>>> teacher_staff_count_administrators
>>> teacher_staff_count_pupil_services
>>> teacher_staff_count_teachers
>>> teacher_staff_count_by_gender_female
>>> teacher_staff_count_by_gender_male
...
(NO race columns!)
```

## The Fix

Changed lines 264 and 283 in `R/teacher_processing.R`:

```r
# BEFORE (line 264):
race_label = dplyr::coalesce(reporting_category_description, race_ethnicity),

# AFTER (line 264):
race_label = dplyr::coalesce(race_ethnicity, reporting_category_description),  # FIX: Use race_ethnicity first

# Also added additional filtering to exclude staff type slugs:
dplyr::filter(!race_slug %in% c("total", "all", "all_students", "all_staff", "teachers", "administrators", "pupil_services", "other_staff"))
```

Same fix applied to line 283 (for the `race_by_type_tbl` section).

## Impact

After this fix, `teacher_summarise_long()` will now create columns like:
- `teacher_staff_count_african_american`
- `teacher_staff_count_white`
- `teacher_staff_count_hispanic_or_latino`
- `teacher_staff_count_asian`
- `teacher_staff_count_filipino`
- `teacher_staff_count_native_hawaiian_pacific_islander`
- `teacher_staff_count_american_indian_or_alaska_native`
- `teacher_staff_count_two_or_more_races`
- `teacher_staff_count_not_reported`

Plus their corresponding `_share` columns (e.g., `teacher_staff_count_african_american_share`).

## Next Steps

1. **Re-run script 18** to regenerate the merged dataset:
   ```r
   source("Analysis/18_merge_teacher_student.R")
   ```

   This will create `data-stage/susp_v6_teacher_long.parquet` with correct race columns.

2. **Run analysis script 21**:
   ```r
   source("Analysis/21_weighted_teacher_diversity_by_quartile.R")
   ```

   It should now find teacher race columns and produce:
   - Tables: `outputs/tables/21_teacher_diversity_by_quartile_*.csv`
   - Graphs: `outputs/graphs/21_teacher_diversity_*.png`

3. **Verify the fix** by checking the output:
   ```
   >>> Found [X] teacher race columns  # Should be > 0 now!
   >>> Identified key columns:
   >>>   African American: teacher_staff_count_african_american  # Found!
   >>>   White: teacher_staff_count_white  # Found!
   >>>   Hispanic: teacher_staff_count_hispanic_or_latino  # Found!
   >>>   Asian: teacher_staff_count_asian  # Found!
   ```

## Files Modified

- `R/teacher_processing.R` - Fixed race data extraction logic (lines 264, 283, 267, 286)

## Technical Details

### Column Naming Pattern

The teacher summary creates columns with this pattern:
```
teacher_{metric}_{race_slug}
```

Where:
- `{metric}` = `staff_count` (from the numeric columns in teacher_staff_long)
- `{race_slug}` = slugified race name (e.g., "african_american", "hispanic_or_latino")

Examples:
- `teacher_staff_count_african_american` → count of African American staff
- `teacher_staff_count_african_american_share` → proportion of African American staff

### How Analysis Scripts Find These Columns

Script 21 uses this grep pattern to find teacher race columns:
```r
teacher_race_cols <- grep(
  "^teacher_staff_count_(african|american_indian|asian|filipino|hispanic|pacific|white|two_or_more|not_reported)",
  names(analysis_df),
  value = TRUE,
  perl = TRUE
)
```

This matches columns like:
- `teacher_staff_count_african_american` (matches "african")
- `teacher_staff_count_hispanic_or_latino` (matches "hispanic")
- `teacher_staff_count_white` (matches "white")
- etc.

## Why This Bug Was Hard to Spot

1. **The teacher data WAS being loaded** - ingestion worked fine
2. **116 teacher columns WERE created** - but they were staff types and genders, not races
3. **No error messages** - the code ran without errors, just produced wrong results
4. **Similar column names** - `reporting_category_description` and `race_ethnicity` are both character columns describing categories

The bug was a subtle logic error in data transformation, not a missing file or syntax error.

## Validation

After re-running script 18, you can verify the fix with:

```r
library(arrow)
library(dplyr)

# Load merged data
df <- read_parquet("data-stage/susp_v6_teacher_long.parquet")

# Check for race columns
race_cols <- grep("^teacher_staff_count_(african|hispanic|white|asian)",
                  names(df), value = TRUE)
print(paste("Found", length(race_cols), "teacher race columns"))
print(race_cols)

# Sample a few rows
df %>%
  select(cds_school, academic_year, subgroup,
         matches("^teacher_staff_count_(african|white|hispanic)")) %>%
  head(20)
```

Expected output:
```
[1] "Found 9 teacher race columns"  # Or more, depending on breakdown
[1] "teacher_staff_count_african_american"
[2] "teacher_staff_count_white"
[3] "teacher_staff_count_hispanic_or_latino"
[4] "teacher_staff_count_asian"
...
```

## Related Issues

- **Script 18 fix** (already completed): Changed to use `susp_v6_long.parquet` for race-specific student data
- **This fix** (teacher processing): Ensures teacher race data is correctly extracted and summarized
- **Together**, these fixes enable intersectional analyses of teacher-student demographics and suspension patterns
