# PROTOCOL: Teacher Data Merge - Correct Approach

**Date:** 2025-11-14
**Source:** Analysis of working script `Analysis/21_weighted_teacher_diversity_by_quartile.R`
**Status:** ✅ **VALIDATED - This is the correct approach**

---

## Executive Summary

After analyzing the **working** teacher diversity script, the correct protocol for merging teacher and student data is now clear. The key insight: **DO THE MERGE AT RUNTIME** rather than relying on a pre-merged file.

---

## The Working Approach (Lines 41-97)

### Step 1: Load Source Files Separately

```r
# Define paths to SOURCE files (not merged files)
V6_LONG_PATH <- here::here("data-stage", "susp_v6_long.parquet")
TEACHER_PATH <- here::here("data-stage", "teacher_staff_long.parquet")

# Check prerequisites
if (!file.exists(V6_LONG_PATH)) {
  stop("Missing susp_v6_long.parquet. Run run_pipeline.R first.")
}
if (!file.exists(TEACHER_PATH)) {
  stop("Missing teacher_staff_long.parquet. Run R/01c_ingest_teacher_demographics.R first.")
}
```

### Step 2: Load and Aggregate Student Data

```r
# Load student data (LONG format - multiple rows per school-year)
df_students_raw <- arrow::read_parquet(V6_LONG_PATH) %>%
  janitor::clean_names() %>%
  build_keys() %>%
  filter(
    aggregate_level == "S" | tolower(aggregate_level) == "school",
    !school_code %in% SPECIAL_SCHOOL_CODES
  )

# Aggregate to school level (one row per school-year)
# Use "All Students" totals to avoid race stratification
df_students <- df_students_raw %>%
  filter(
    category_type == "Race/Ethnicity",
    canon_race_label(subgroup) == "All Students"
  ) %>%
  distinct(cds_school, academic_year, .keep_all = TRUE)

# Verify uniqueness
df_students <- assert_unique_campus(df_students, campus_col = "cds_school",
                                    year_col = "academic_year")
```

**Key Point:** The working script aggregates to **school level first** (one row per school-year) before merging.

### Step 3: Load and Summarize Teacher Data

```r
# Load teacher processing utilities
source(here::here("R", "teacher_processing.R"))

# Load teacher long data
teacher_long <- arrow::read_parquet(TEACHER_PATH) %>%
  janitor::clean_names() %>%
  build_keys()

# Summarize to wide format with race/gender shares
teacher_summary <- teacher_summarise_long(teacher_long)

# Sanitize NaN/Inf values
teacher_summary <- teacher_summary %>%
  mutate(across(where(is.numeric), ~ {
    out <- .x
    out[is.nan(out)] <- NA_real_
    dplyr::na_if(out, Inf)
  }))
```

**Key Point:** The `teacher_summarise_long()` function creates columns like:
- `teacher_staff_count_total`
- `teacher_staff_count_african_american`
- `teacher_staff_count_african_american_share`
- `teacher_staff_count_by_type_administrators_african_american`
- `teacher_staff_count_by_type_administrators_african_american_share`

### Step 4: Merge

```r
# LEFT JOIN: preserve all student data
df <- df_students %>%
  left_join(
    teacher_summary,
    by = c("academic_year", "cds_school"),
    relationship = "one-to-one"  # Both are unique by school-year
  )
```

### Step 5: Validate Merge

```r
# Check for teacher columns
teacher_cols <- grep("^teacher_", names(df), value = TRUE)
if (!length(teacher_cols)) {
  stop("No teacher_* columns found. Check merge in Analysis/18_merge_teacher_student.R")
}

message(">>> Found ", length(teacher_cols), " teacher demographic columns")
```

---

## Robust Column Detection (Lines 214-279)

The working script has **comprehensive diagnostics**:

### Debug: Show All Available Columns

```r
# Show ALL teacher columns for debugging
all_teacher_cols <- grep("^teacher_staff_count_", names(analysis_df), value = TRUE)
message(">>> DEBUG: All teacher_staff_count_* columns (", length(all_teacher_cols), " total):")
message(">>> ", paste(all_teacher_cols, collapse = "\n>>> "))

# Show columns with race keywords
race_keyword_cols <- grep("(african|white|hispanic|asian|latino|black|race|ethnicity)",
                          names(analysis_df), value = TRUE, ignore.case = TRUE)
message("\n>>> Columns with race/ethnicity keywords (", length(race_keyword_cols), " total):")
if (length(race_keyword_cols) > 0) {
  message(">>> ", paste(race_keyword_cols, collapse = "\n>>> "))
} else {
  message(">>> NONE FOUND - Teacher data does not include race/ethnicity breakdowns")
}
```

### Dynamic Column Finding

```r
# Find teacher race columns dynamically
teacher_race_cols <- grep(
  "^teacher_staff_count_(african|american_indian|asian|filipino|hispanic|pacific|white|two_or_more|not_reported)",
  names(analysis_df),
  value = TRUE,
  perl = TRUE
)

# Exclude _share columns (we want raw counts)
teacher_race_cols <- grep("_share$", teacher_race_cols, value = TRUE, invert = TRUE, perl = TRUE)

message(">>> Found ", length(teacher_race_cols), " teacher race columns")

# Flag for analysis
has_teacher_race_data <- length(teacher_race_cols) > 0
```

### Find Specific Race Columns

```r
# Helper function to find primary column for a race category
find_primary_teacher_race_col <- function(patterns) {
  if (!length(teacher_race_cols)) {
    return(NA_character_)
  }

  for (pattern in patterns) {
    matches <- grep(pattern, teacher_race_cols, value = TRUE)
    if (length(matches)) {
      return(matches[[1]])
    }
  }

  NA_character_
}

# Find key race columns
col_african_american <- find_primary_teacher_race_col(c("african_american$", "black$"))
col_white <- find_primary_teacher_race_col(c("white$"))
col_hispanic <- find_primary_teacher_race_col(c("hispanic_or_latino$", "hispanic$", "latino$"))
col_asian <- find_primary_teacher_race_col(c("asian$"))

# Report what was found
message(">>> Identified key columns:")
message(">>>   African American: ", ifelse(is.na(col_african_american), "NOT FOUND", col_african_american))
message(">>>   White: ", ifelse(is.na(col_white), "NOT FOUND", col_white))
message(">>>   Hispanic: ", ifelse(is.na(col_hispanic), "NOT FOUND", col_hispanic))
message(">>>   Asian: ", ifelse(is.na(col_asian), "NOT FOUND", col_asian))
```

### Fail Gracefully with Clear Error

```r
if (!has_teacher_race_data) {
  message("\n>>> ERROR: Cannot proceed with teacher diversity analysis")
  message(">>> The merged teacher dataset does not include race/ethnicity counts.")
  message(">>> Expected columns like teacher_staff_count_african_american, teacher_staff_count_white, etc.")
  message(">>> Run R/01c_ingest_teacher_demographics.R and Analysis/18_merge_teacher_student.R after placing stre*.txt files under data-raw/.")
  stop("Teacher race/ethnicity breakdowns missing. See docs/guides/TEACHER_DATA_SETUP_GUIDE.md for acquisition steps.")
}
```

---

## Key Differences from Broken Regression Script

| Aspect | ❌ Broken Regression Script | ✅ Working Script |
|--------|---------------------------|------------------|
| **Data loading** | Loads `susp_v6_teacher_features.parquet` (pre-merged) | Loads `susp_v6_long.parquet` + `teacher_staff_long.parquet` separately |
| **Merge strategy** | Expects pre-merged file to exist | Does merge at runtime using `teacher_summarise_long()` |
| **Student data structure** | Expects LONG format (multiple rows per school) | Aggregates to school level FIRST (one row per school-year) |
| **Column detection** | Basic pattern matching, silent failures | Comprehensive diagnostics, verbose logging |
| **Error handling** | Generic "file not found" message | Specific, actionable error messages |
| **Validation** | Minimal | Extensive (shows all columns, checks existence, reports coverage) |

---

## The CORRECT Protocol for Future Scripts

### When to Use Each Approach

#### Use Runtime Merge (RECOMMENDED):
- **When:** Analyzing school-level aggregates
- **When:** Don't need student race stratification
- **Advantages:**
  - No dependency on pre-merged files
  - Always uses latest source data
  - Easier to debug
  - More flexible

```r
# Template for runtime merge
V6_LONG_PATH <- here::here("data-stage", "susp_v6_long.parquet")
TEACHER_PATH <- here::here("data-stage", "teacher_staff_long.parquet")

# Load student data (aggregate to school level)
df_students <- read_parquet(V6_LONG_PATH) %>%
  filter(category_type == "Race/Ethnicity",
         canon_race_label(subgroup) == "All Students") %>%
  distinct(cds_school, academic_year, .keep_all = TRUE)

# Load and summarize teacher data
teacher_long <- read_parquet(TEACHER_PATH)
teacher_summary <- teacher_summarise_long(teacher_long)

# Merge
df <- df_students %>%
  left_join(teacher_summary, by = c("academic_year", "cds_school"))
```

#### Use Pre-Merged File:
- **When:** Need student race-specific analysis (e.g., regressions stratified by student race)
- **File:** `susp_v6_teacher_features.parquet` (created by `Analysis/22_build_teacher_race_shares.R`)
- **Structure:** LONG format (one row per school-year-student_group)
- **Prerequisite:** Must run `Analysis/22_build_teacher_race_shares.R` first

```r
# Template for pre-merged file
MERGED_PATH <- here::here("data-stage", "susp_v6_teacher_features.parquet")

if (!file.exists(MERGED_PATH)) {
  stop("Missing susp_v6_teacher_features.parquet.\n",
       "Run: source('Analysis/22_build_teacher_race_shares.R')")
}

df <- read_parquet(MERGED_PATH)

# CRITICAL: Must have student_group column
if (!"student_group" %in% names(df)) {
  stop("student_group column missing. File may be corrupted.")
}

# Validate it's in long format
if (any(duplicated(df[c("cds_school", "academic_year")]))) {
  message("✓ Data is in LONG format (multiple rows per school-year)")
} else {
  warning("Data appears to be in WIDE format (one row per school-year)")
}
```

---

## Required Source Files

Both approaches require these source files:

### 1. `data-stage/susp_v6_long.parquet`
- **Created by:** `run_pipeline.R` (final stage: `22_build_v6_features.R`)
- **Structure:** Long format, one row per school-year-race-demographic category
- **Key columns:**
  - `cds_school`, `academic_year`
  - `category_type`, `subgroup`, `reporting_category`
  - `cumulative_enrollment`, `total_suspensions`
  - `black_prop_q` (Black enrollment quartile)

### 2. `data-stage/teacher_staff_long.parquet`
- **Created by:** `R/01c_ingest_teacher_demographics.R`
- **Source:** Raw CDE teacher TXT files (`data-raw/stre*.txt`)
- **Structure:** Long format, one row per school-year-race-gender-staff_type
- **Key columns:**
  - `cds_school`, `academic_year`
  - `reporting_category` (staff type: ALL, TCH, ADM, PSV, OTH)
  - `race_ethnicity` (African American, White, Asian, etc.)
  - `staff_gender_code` (GF, GM, GX, GZ, ALL)
  - `staff_count`, `fte` (numeric metrics)

**CRITICAL:** If `teacher_staff_long.parquet` doesn't exist, you need to:
1. Obtain raw CDE teacher files: `stre1718.txt`, `stre1819.txt`, etc.
2. Place them in `data-raw/`
3. Run `source("R/01c_ingest_teacher_demographics.R")`

---

## Column Naming Conventions

After `teacher_summarise_long()`, expect these columns:

### Total Counts
```
teacher_staff_count_total          # Total staff (all types)
teacher_fte_total                  # Total FTE (if available)
```

### Race Counts and Shares
```
teacher_staff_count_african_american
teacher_staff_count_african_american_share
teacher_staff_count_asian
teacher_staff_count_asian_share
teacher_staff_count_white
teacher_staff_count_white_share
teacher_staff_count_hispanic_or_latino
teacher_staff_count_hispanic_or_latino_share
... (9 race categories total)
```

### Staff Type × Race
```
teacher_staff_count_by_type_teachers_african_american
teacher_staff_count_by_type_teachers_african_american_share
teacher_staff_count_by_type_administrators_african_american
teacher_staff_count_by_type_administrators_african_american_share
teacher_staff_count_by_type_pupil_services_white
teacher_staff_count_by_type_pupil_services_white_share
... (staff_type × race combinations)
```

### Gender
```
teacher_staff_count_by_gender_female
teacher_staff_count_by_gender_female_share
teacher_staff_count_by_gender_male
teacher_staff_count_by_gender_male_share
```

---

## Diagnostic Checklist

Before running any teacher diversity analysis:

### ✅ Step 1: Verify Source Files Exist
```r
file.exists("data-stage/susp_v6_long.parquet")       # Should be TRUE
file.exists("data-stage/teacher_staff_long.parquet")  # Should be TRUE
```

### ✅ Step 2: Inspect Teacher Columns After Merge
```r
# After merge, check what you got
teacher_cols <- grep("^teacher_", names(df), value = TRUE)
message("Found ", length(teacher_cols), " teacher columns")

# Show them
print(head(teacher_cols, 20))
```

### ✅ Step 3: Check for Race Columns
```r
race_cols <- grep("(african|asian|white|hispanic)", teacher_cols, value = TRUE)
message("Found ", length(race_cols), " race-related columns")

if (length(race_cols) == 0) {
  stop("ERROR: No teacher race columns found. Check source data.")
}
```

### ✅ Step 4: Validate Share Columns
```r
share_cols <- grep("_share$", teacher_cols, value = TRUE)
message("Found ", length(share_cols), " share columns")

# Check that shares are in [0, 1]
if (length(share_cols) > 0) {
  sample_col <- share_cols[1]
  sample_vals <- df[[sample_col]][!is.na(df[[sample_col]])]

  if (length(sample_vals) > 0) {
    message("Sample share column: ", sample_col)
    message("  Min: ", min(sample_vals))
    message("  Max: ", max(sample_vals))
    message("  Mean: ", mean(sample_vals))

    if (any(sample_vals < 0 | sample_vals > 1)) {
      warning("Share values outside [0, 1] range!")
    }
  }
}
```

---

## Summary: Lock This Protocol Into Memory

### The Golden Rule
**Always do the merge at runtime using the two source parquet files, unless you specifically need student race stratification.**

### The Two-File Approach
1. `susp_v6_long.parquet` (student data)
2. `teacher_staff_long.parquet` (teacher data)

### The Merge Process
1. Load student data → aggregate to school level (one row per school-year)
2. Load teacher data → summarize with `teacher_summarise_long()`
3. Left join on `(academic_year, cds_school)`
4. Validate columns exist
5. Fail gracefully with clear messages if data is missing

### The Validation Protocol
1. Show ALL available columns (verbose debugging)
2. Dynamically find race columns
3. Check if data exists before proceeding
4. Provide actionable error messages with exact commands to run

---

**This protocol is now locked in and should be used for all future teacher diversity analyses.**

