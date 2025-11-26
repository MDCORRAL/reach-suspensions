# Comprehensive Audit: Teacher Diversity Regression Script

**Date:** 2025-11-14
**Script:** `Analysis/21_teacher_diversity_regression.R`
**Auditor:** Claude Code
**Status:** ❌ **CRITICAL ISSUES IDENTIFIED - SCRIPT NON-FUNCTIONAL**

---

## Executive Summary

The teacher diversity regression script (`Analysis/21_teacher_diversity_regression.R`) contains **multiple critical issues** that prevent it from functioning correctly. The primary issue reported by the user—inability to identify race/ethnicity factors of teachers/administrators—is caused by a fundamental mismatch between:

1. **What the script expects:** Teacher race share columns like `teacher_*_african_american_share`
2. **What actually exists:** These columns may not exist in the data file, OR the script's detection logic is flawed

### Severity: 🔴 **CRITICAL - SCRIPT CANNOT RUN**

### Key Findings

| Issue | Severity | Status | Impact |
|-------|----------|--------|---------|
| Missing data file validation | 🔴 CRITICAL | Not handled | Script fails if data not generated |
| Incorrect administrator race detection pattern | 🔴 CRITICAL | Bug | Cannot find admin race columns |
| Confusion between staff type and race | 🔴 CRITICAL | Design flaw | Fundamental misunderstanding of data structure |
| No validation of detected columns | 🟡 HIGH | Missing | Silent failures possible |
| Data file may not exist | 🔴 CRITICAL | Dependency | Prerequisite script may not have been run |
| Missing student_group handling | 🟡 HIGH | Incomplete | Cannot stratify by student race |
| Hardcoded race slug patterns | 🟡 MEDIUM | Fragility | Breaks if race names change |

---

## 1. Data File Dependency Issues

### Issue 1.1: Missing File Existence Check

**Location:** Lines 224-242
**Severity:** 🔴 **CRITICAL**

```r
load_features <- function() {
  if (!file.exists(TEACHER_PATH)) {
    stop("\n",
         "════════════════════════════════════════════════════════════════\n",
         "❌ MISSING FILE: ", TEACHER_PATH, "\n",
         ...
         "  source('Analysis/22_build_teacher_race_shares.R')\n",
```

**Problem:**
- The script references `Analysis/22_build_teacher_race_shares.R` to create the required file
- However, according to the audit report `TEACHER_DEMOGRAPHIC_INTEGRATION_AUDIT.md`, this file may not exist
- The prerequisite script `22_build_teacher_race_shares.R` must be run first

**Actual Root Cause:**
The file `susp_v6_teacher_features.parquet` is created by `Analysis/22_build_teacher_race_shares.R`, which:
1. Loads `teacher_staff_long.parquet` (created by `R/01c_ingest_teacher_demographics.R`)
2. Runs `teacher_summarise_long()` to create wide-format race/gender columns
3. Merges with `susp_v6_long.parquet`

**If the prerequisite scripts haven't been run**, the data file won't exist and none of the race columns will be available.

### Issue 1.2: No Verification of Column Structure

**Location:** Lines 276-284
**Severity:** 🟡 **HIGH**

```r
teacher_cols <- grep("^teacher_", names(df), value = TRUE)
message("Teacher columns: ", length(teacher_cols), " found")

race_share_cols <- grep("_(african_american|asian|hispanic|white|filipino).*_share",
                        teacher_cols, value = TRUE, ignore.case = TRUE)
message("  - Teacher race share columns: ", length(race_share_cols))
```

**Problem:**
- The script counts columns but doesn't verify they match the expected structure
- If `race_share_cols` returns 0, the script continues anyway
- No validation that the columns are usable for the analysis

---

## 2. Critical Race Detection Logic Errors

### Issue 2.1: Fundamental Misunderstanding of Column Structure

**Location:** Lines 83-185 (`extract_teacher_race_nonwhite_share`)
**Severity:** 🔴 **CRITICAL**

The function attempts to extract race share columns with this pattern:

```r
race_share_pattern <- paste0(prefix, ".*_(",
                             paste(TEACHER_RACE_SLUGS, collapse = "|"),
                             ")_share$")
```

For teachers (prefix = `"^teacher"`), this creates:
```
^teacher.*_(african_american|asian|filipino|hispanic_or_latino|...|white|...)_share$
```

**Expected column names** based on `teacher_summarise_long()` (from tests):
```
teacher_staff_count_african_american_share
teacher_staff_count_asian_share
teacher_staff_count_white_share
teacher_fte_african_american_share
...
```

**This pattern SHOULD work** for these columns. ✅

### Issue 2.2: Broken Administrator Detection Pattern

**Location:** Lines 187-192 (`extract_admin_race_nonwhite_share`)
**Severity:** 🔴 **CRITICAL - CONFIRMED BUG**

```r
extract_admin_race_nonwhite_share <- function(df) {
  # Extract administrator non-white share from race-specific columns.
  # Looks specifically for _by_type_administrators_ columns.

  extract_teacher_race_nonwhite_share(df, prefix = "^teacher.*by_type_administrators")
}
```

**Expected column names** for administrators (based on `teacher_summarise_long()` code at lines 293-296):
```
teacher_staff_count_by_type_administrators_african_american
teacher_staff_count_by_type_administrators_african_american_share
teacher_staff_count_by_type_administrators_asian_share
teacher_fte_by_type_administrators_white_share
...
```

**The pattern being used:**
```
^teacher.*by_type_administrators.*_(african_american|asian|...)_share$
```

**Analysis:**
- This pattern SHOULD match columns like `teacher_staff_count_by_type_administrators_african_american_share`
- ✅ The regex pattern itself is technically correct

**BUT** - The real problem is likely:

### Issue 2.3: Columns May Not Exist in Data

**Severity:** 🔴 **CRITICAL**

According to `teacher_processing.R` lines 236-250, the `by_type` columns are only created if:

```r
totals_by_type <- NULL
if (has_staff_type) {
  totals_by_type <- df %>%
    dplyr::filter(!is.na(reporting_category_slug)) %>%
    dplyr::group_by(dplyr::across(dplyr::all_of(c(key_cols, "reporting_category_slug")))) %>%
```

**This requires:**
1. The input data must have a `reporting_category` column
2. That column must be properly populated with staff type codes (ALL, TCH, ADM, PSV, OTH)
3. The teacher ingestion script must have been run with files containing this data

**Root Cause Analysis:**

Looking at the teacher ingestion script (`R/01c_ingest_teacher_demographics.R`), the `reporting_category` field is renamed from the raw CDE field "Staff Type" (line 23 comment documentation).

**If any of these are true, the administrator columns won't exist:**
- ❌ Teacher TXT files don't contain staff type breakdown by race
- ❌ The `reporting_category` column is missing or has only "ALL" values
- ❌ The teacher ingestion script hasn't been run
- ❌ The raw data files don't exist

---

## 3. Confusion Between Staff Type and Race/Ethnicity

### Issue 3.1: Conceptual Misunderstanding in Comments

**Location:** Lines 7-11
**Severity:** 🟡 **MEDIUM** (Misleading but doesn't break functionality)

```r
# Key features:
# - Uses teacher RACIAL diversity (proportion non-white staff)
# - Explicit race column detection with validation
# - Weighted linear regressions (weighted by student enrollment)
# - Stratified by student racial/ethnic group
```

**Analysis:**
The comments claim to use "teacher RACIAL diversity" and be "stratified by student racial/ethnic group", which is correct. However, the implementation conflates:

1. **Teacher race/ethnicity** (African American, White, Asian, etc.)
2. **Staff type** (Teachers, Administrators, Pupil Services, etc.)

These are **two separate dimensions** in the CDE data:

```
ACTUAL DATA STRUCTURE (from teacher ingestion docs):
┌─────────────────────────────────────────────────────────┐
│ Dimension 1: Staff Type (reporting_category)           │
│   - ALL = All Staff                                     │
│   - TCH = Teachers                                      │
│   - ADM = Administrators                                │
│   - PSV = Pupil Services                                │
│   - OTH = Other Staff                                   │
├─────────────────────────────────────────────────────────┤
│ Dimension 2: Race/Ethnicity                            │
│   - African American                                    │
│   - Asian                                               │
│   - White                                               │
│   - Hispanic/Latino                                     │
│   - ... etc (9 categories total)                        │
└─────────────────────────────────────────────────────────┘
```

The column naming reflects this two-dimensional structure:
- `teacher_staff_count_african_american` = Count of ALL staff who are African American
- `teacher_staff_count_by_type_administrators_african_american` = Count of ADMINISTRATORS who are African American

### Issue 3.2: Treating "Administrator" as a Race Category

**Location:** Throughout the script, especially lines 310-332
**Severity:** 🟡 **MEDIUM** (Confusing but technically works)

The script uses:
```r
teacher_race <- extract_teacher_race_nonwhite_share(df, prefix = "^teacher")
admin_race <- extract_admin_race_nonwhite_share(df)
```

**This creates variables named:**
- `teacher_non_white_share` - Proportion of ALL staff who are non-white
- `admin_non_white_share` - Proportion of ADMINISTRATORS who are non-white

**Better naming would be:**
- `all_staff_non_white_share` or `total_staff_non_white_share`
- `administrator_non_white_share` (already correct)

The issue is that "teacher" in `teacher_non_white_share` is misleading—it includes ALL staff types, not just classroom teachers.

---

## 4. Student Group Handling Issues

### Issue 4.1: Incorrect Race Canonicalization

**Location:** Lines 253-266
**Severity:** 🟡 **HIGH**

```r
if ("student_group" %in% names(df)) {
  # Canonicalize the student_group column (converts CDE codes like RA, RB to full labels)
  df$student_group <- canonicalize_race_label(df$student_group)
  groups <- sort(unique(df$student_group[!is.na(df$student_group)]))
  message("Student groups: ", paste(groups, collapse = ", "))
} else if ("reporting_category" %in% names(df)) {
  df$student_group <- canonicalize_race_label(df$reporting_category)
```

**Problems:**

1. **Function name doesn't exist in loaded scope:**
   - The function is called `canonicalize_race_label()` (line 62 in the script)
   - But `R/utils_keys_filters.R` defines `canon_race_label()` (no "ize")
   - The script defines its own version at lines 62-77, but this is redundant

2. **Not handling staff type codes:**
   - The `reporting_category` field in teacher data contains **staff type codes** (ALL, TCH, ADM, PSV, OTH)
   - In student data, `reporting_category` contains **race codes** (RA, RB, RW, etc.)
   - If the script loads teacher data by mistake, it will try to convert "TCH" and "ADM" to race labels

3. **Wrong data file:**
   - The script loads `susp_v6_teacher_features.parquet` (line 22, `TEACHER_PATH`)
   - This file was created by merging student suspension data with teacher demographics
   - It should have student race in `reporting_category` or `student_group`
   - But according to `Analysis/22_build_teacher_race_shares.R` lines 82-93, the column is renamed from `reporting_category` to `student_group`

**Expected behavior:**
- ✅ `susp_v6_teacher_features.parquet` should have `student_group` column with student race/ethnicity
- ✅ Script should use this for stratification

**Potential issue:**
- If the file was created incorrectly or from the wrong source, this would fail

---

## 5. Hardcoded Race Patterns

### Issue 5.1: Race Slug List Duplication

**Location:** Lines 26-37
**Severity:** 🟡 **MEDIUM**

```r
TEACHER_RACE_SLUGS <- c(
  "african_american",
  "asian",
  "filipino",
  "hispanic_or_latino",
  "american_indian_or_alaska_native",
  "native_hawaiian_pacific_islander",
  "pacific_islander",  # legacy slug still appears in some historical files
  "white",
  "two_or_more_races",
  "not_reported"
)
```

**Problem:**
- This list is hardcoded and duplicates the definition in `R/teacher_processing.R` lines 103-115 (`teacher_race_suffix_lookup()`)
- If race category names change in the source system, both places must be updated
- The inclusion of `"pacific_islander"` as a legacy slug suggests data inconsistency issues

**Correct approach:**
- Import `teacher_race_suffix_lookup()` from `R/teacher_processing.R`
- Use that as the single source of truth

### Issue 5.2: Allowed Race Groups Inconsistency

**Location:** Lines 39-48
**Severity:** 🟡 **MEDIUM**

```r
ALLOWED_RACE_GROUPS <- c(
  "Black/African American",
  "White",
  "Hispanic/Latino",
  "American Indian/Alaska Native",
  "Asian",
  "Filipino",
  "Native Hawaiian/Pacific Islander",
  "Two or More Races"
)
```

**Problems:**

1. **Inconsistent with `R/utils_keys_filters.R`:**
   - `utils_keys_filters.R` defines `ALLOWED_RACES` at lines 260-270
   - That list includes `"All Students"` and comments exclude `"Not Reported"`
   - This list excludes both

2. **Label format inconsistency:**
   - Uses `"Hispanic/Latino"` but teacher slugs use `"hispanic_or_latino"`
   - Uses `"Black/African American"` but test expects `"Black/African American"` (same, OK)

3. **No canonical source:**
   - Should use `ALLOWED_RACES` from `utils_keys_filters.R`
   - Or derive from `teacher_race_suffix_lookup()$race_ethnicity_label`

---

## 6. Detailed Column Detection Analysis

### What SHOULD Exist in the Data

Based on `teacher_summarise_long()` in `R/teacher_processing.R`, these columns should be created:

#### 6.1 Total Columns (lines 229-233)
```r
teacher_{metric}_total
```
Example: `teacher_staff_count_total`, `teacher_fte_total`

#### 6.2 Race Columns (lines 263-277)
```r
teacher_{metric}_{race_slug}
teacher_{metric}_{race_slug}_share
```
Examples:
- `teacher_staff_count_african_american`
- `teacher_staff_count_african_american_share`
- `teacher_staff_count_asian_share`
- `teacher_fte_white`

#### 6.3 Staff Type Totals (lines 236-250)
```r
teacher_{metric}_total_by_type_{staff_type_slug}
```
Examples:
- `teacher_staff_count_total_by_type_teachers`
- `teacher_staff_count_total_by_type_administrators`

#### 6.4 Staff Type × Race Columns (lines 279-297)
```r
teacher_{metric}_by_type_{staff_type_slug}_{race_slug}
teacher_{metric}_by_type_{staff_type_slug}_{race_slug}_share
```
Examples:
- `teacher_staff_count_by_type_administrators_african_american`
- `teacher_staff_count_by_type_administrators_african_american_share`
- `teacher_staff_count_by_type_teachers_white_share`

#### 6.5 Gender Columns (lines 299-318)
```r
teacher_{metric}_by_gender_{gender_slug}
teacher_{metric}_by_gender_{gender_slug}_share
```

### What the Script is Looking For

#### For All Staff (line 310):
```r
teacher_race <- extract_teacher_race_nonwhite_share(df, prefix = "^teacher")
```
**Pattern:** `^teacher.*_(african_american|asian|...)_share$`
**Matches:** ✅ `teacher_staff_count_african_american_share`
**Should work if columns exist**

#### For Administrators (line 311):
```r
admin_race <- extract_admin_race_nonwhite_share(df)
```
**Pattern:** `^teacher.*by_type_administrators.*_(african_american|asian|...)_share$`
**Matches:** ✅ `teacher_staff_count_by_type_administrators_african_american_share`
**Should work if columns exist**

---

## 7. Function-by-Function Analysis

### 7.1 `extract_teacher_race_nonwhite_share()` (Lines 83-185)

**Purpose:** Extract non-white share from race-specific columns

**Logic Flow:**
1. Search for race share columns matching pattern (lines 92-96)
2. If not found, fallback to generic share columns (lines 100-109)
3. Separate white vs non-white columns (lines 118-129)
4. Compute non-white share by either:
   - Method 1: Sum of non-white race shares (lines 137-154)
   - Method 2: 1 - white_share (lines 156-181)

**Issues:**

| Line | Issue | Severity |
|------|-------|----------|
| 92-96 | Pattern is correct but columns may not exist | 🔴 CRITICAL |
| 100-109 | Fallback doesn't filter out gender columns properly | 🟡 MEDIUM |
| 102 | Excludes "by_gender" but not "by_type_X_by_gender" | 🟡 LOW |
| 137-154 | Method 1 sums non-white shares - ✅ CORRECT | ✅ OK |
| 156-165 | Method 2 subtracts white + not_reported - ✅ CORRECT | ✅ OK |

**Recommendations:**
1. Add verbose logging of which columns were found
2. Validate that at least some columns were found
3. Check that the sum of shares is approximately 1.0

### 7.2 `extract_admin_race_nonwhite_share()` (Lines 187-192)

**Purpose:** Extract administrator non-white share

**Implementation:**
```r
extract_admin_race_nonwhite_share <- function(df) {
  extract_teacher_race_nonwhite_share(df, prefix = "^teacher.*by_type_administrators")
}
```

**Analysis:**
- ✅ Reuses teacher function with different prefix
- ✅ Pattern should match administrator columns correctly
- ❌ No validation that columns were found
- ❌ Silent failure if administrators data is missing

**Recommendations:**
1. Add explicit check: `if (is.null(result)) { warning(...) }`
2. Report which columns were used
3. Validate against expected column names

### 7.3 `prepare_regression_frame()` (Lines 295-420)

**Purpose:** Build regression data frame with teacher diversity and controls

**Critical Checks:**

| Lines | Check | Status |
|-------|-------|--------|
| 310-311 | Extract teacher & admin race | ✅ Called |
| 318-323 | Fatal error if either is NULL | ✅ Good |
| 325-330 | Fatal error if not race_share type | ✅ Good |
| 335-347 | Get suspension rate outcome | ✅ OK |
| 349-387 | Add control variables | ✅ OK |
| 389-411 | Filter complete cases and set weights | ✅ OK |

**The script WILL FAIL at lines 318-323 if:**
- Teacher race columns don't exist → `teacher_race = NULL`
- Administrator race columns don't exist → `admin_race = NULL`

**This is the expected failure point based on user's report.**

---

## 8. Root Cause Summary

Based on this audit, the user's reported issue ("unable to identify the race/ethnicity factors of teacher/admin") is caused by **one or more of these root causes:**

### Root Cause 1: Prerequisite Scripts Not Run ❌
**Probability: VERY HIGH**

The script requires:
1. `R/01c_ingest_teacher_demographics.R` to create `teacher_staff_long.parquet`
2. `Analysis/22_build_teacher_race_shares.R` to create `susp_v6_teacher_features.parquet`

If either hasn't been run, the required columns won't exist.

**Validation:**
```bash
ls -lh data-stage/teacher_staff_long.parquet
ls -lh data-stage/susp_v6_teacher_features.parquet
```

### Root Cause 2: Raw Teacher Data Files Missing ❌
**Probability: HIGH**

According to the teacher ingestion script, it needs:
```
data-raw/stre1718.txt
data-raw/stre1819.txt
data-raw/stre1920.txt
... etc
```

If these don't exist, the pipeline can't run.

**Validation:**
```bash
ls -lh data-raw/stre*.txt
```

### Root Cause 3: Staff Type Data Not Available in Raw Files ❌
**Probability: MEDIUM**

Even if the raw files exist, they may not include the staff type breakdown (ADM, TCH, PSV, OTH) by race.

If the CDE files only have aggregate (ALL staff) data, then columns like:
```
teacher_staff_count_by_type_administrators_african_american_share
```
will never be created.

**Validation:**
- Check raw TXT files for `reporting_category` or "Staff Type" field
- Verify it has values other than "ALL"

### Root Cause 4: Column Naming Mismatch ❌
**Probability: LOW**

The detection patterns are correct, but if the `teacher_summarise_long()` function has bugs or has changed, the column names might not match.

**Validation:**
- Inspect actual column names in `susp_v6_teacher_features.parquet`
- Compare to expected patterns

---

## 9. Diagnostic Steps to Identify Actual Cause

Run these checks to determine which root cause applies:

### Step 1: Check File Existence
```r
# In R console:
file.exists("data-stage/teacher_staff_long.parquet")
file.exists("data-stage/susp_v6_teacher_features.parquet")
file.exists("data-stage/susp_v6_long.parquet")

# In bash:
ls -lh data-stage/*.parquet | grep teacher
```

### Step 2: If Files Exist, Inspect Columns
```r
library(arrow)

# Load the teacher features file
df <- read_parquet("data-stage/susp_v6_teacher_features.parquet")

# Check dimensions
message("Rows: ", nrow(df), " | Columns: ", ncol(df))

# Find all teacher columns
teacher_cols <- grep("^teacher_", names(df), value = TRUE)
message("Teacher columns found: ", length(teacher_cols))

# Find race share columns
race_share_cols <- grep("_(african_american|asian|hispanic|white).*_share",
                        teacher_cols, value = TRUE, ignore.case = TRUE)
message("Race share columns: ", length(race_share_cols))
if (length(race_share_cols) > 0) {
  cat("\nSample race share columns:\n")
  print(head(race_share_cols, 10))
}

# Find administrator columns
admin_cols <- grep("by_type_administrators", teacher_cols, value = TRUE)
message("\nAdministrator columns: ", length(admin_cols))
if (length(admin_cols) > 0) {
  cat("\nSample administrator columns:\n")
  print(head(admin_cols, 10))
}

# Check for student_group column
if ("student_group" %in% names(df)) {
  cat("\n✓ student_group column exists\n")
  cat("Student groups:", paste(unique(df$student_group), collapse = ", "), "\n")
} else if ("reporting_category" %in% names(df)) {
  cat("\n⚠ Only reporting_category exists, not student_group\n")
  cat("Values:", paste(head(unique(df$reporting_category), 20), collapse = ", "), "\n")
} else {
  cat("\n❌ No student grouping column found\n")
}
```

### Step 3: If Files Don't Exist, Check Raw Data
```bash
# Check for raw teacher files
ls -lh data-raw/stre*.txt 2>&1

# If they exist, check structure of one file
head -20 data-raw/stre1920.txt | cat -A
```

### Step 4: Test Column Detection Functions
```r
source("Analysis/21_teacher_diversity_regression.R")

# Load data
result <- load_features()
df <- result$data

# Test teacher race detection
teacher_race <- extract_teacher_race_nonwhite_share(df, prefix = "^teacher")
if (is.null(teacher_race)) {
  cat("❌ FAILED: Teacher race columns not found\n")
} else {
  cat("✓ SUCCESS: Teacher race detection worked\n")
  cat("  Method:", teacher_race$meta$method, "\n")
  cat("  Columns used:", length(teacher_race$meta$columns), "\n")
  print(teacher_race$meta$columns)
}

# Test admin race detection
admin_race <- extract_admin_race_nonwhite_share(df)
if (is.null(admin_race)) {
  cat("❌ FAILED: Administrator race columns not found\n")
} else {
  cat("✓ SUCCESS: Administrator race detection worked\n")
  cat("  Method:", admin_race$meta$method, "\n")
  cat("  Columns used:", length(admin_race$meta$columns), "\n")
  print(admin_race$meta$columns)
}
```

---

## 10. Recommended Fixes

### Priority 1: Immediate Fixes (CRITICAL)

#### Fix 1.1: Add Prerequisite Check and Clear Error Message

**Location:** Beginning of `main()` function (after line 484)

```r
main <- function() {
  message("\n")
  message("╔════════════════════════════════════════════════════════════════╗")
  message("║                                                                ║")
  message("║     TEACHER/ADMINISTRATOR RACIAL DIVERSITY ANALYSIS           ║")
  message("║     Association with Student Suspension Rates                 ║")
  message("║                                                                ║")
  message("╚════════════════════════════════════════════════════════════════╝")

  # NEW: Check prerequisites
  prereq_teacher_long <- file.exists("data-stage/teacher_staff_long.parquet")
  prereq_student <- file.exists("data-stage/susp_v6_long.parquet")

  if (!prereq_teacher_long || !prereq_student) {
    message("\n❌ PREREQUISITE FILES MISSING\n")
    if (!prereq_teacher_long) {
      message("Missing: data-stage/teacher_staff_long.parquet")
      message("  → Run: source('R/01c_ingest_teacher_demographics.R')")
      message("  → Requires: data-raw/stre*.txt files from CDE")
    }
    if (!prereq_student) {
      message("Missing: data-stage/susp_v6_long.parquet")
      message("  → Run: source('run_pipeline.R')")
    }
    message("\nAfter creating these files, run:")
    message("  source('Analysis/22_build_teacher_race_shares.R')")
    message("  source('Analysis/21_teacher_diversity_regression.R')\n")
    stop("Cannot proceed without prerequisite data files.")
  }

  result <- load_features()
  df <- result$data
  # ... rest of function
}
```

#### Fix 1.2: Add Verbose Column Detection

**Location:** In `extract_teacher_race_nonwhite_share()` after line 96

```r
race_share_cols <- grep(race_share_pattern, names(df), value = TRUE, ignore.case = TRUE)

# NEW: Verbose logging
message(">>> Searching for pattern: ", race_share_pattern)
message(">>> Total columns in data: ", ncol(df))
all_teacher_cols <- grep("^teacher_", names(df), value = TRUE)
message(">>> Columns starting with 'teacher_': ", length(all_teacher_cols))
message(">>> Columns matching race pattern: ", length(race_share_cols))

if (!length(race_share_cols)) {
  # Show what IS available to help debug
  message(">>> Available teacher columns (first 20):")
  print(head(all_teacher_cols, 20))

  # (rest of fallback logic...)
}
```

#### Fix 1.3: Add Validation of Detected Columns

**Location:** In `prepare_regression_frame()` after line 315

```r
message("\n", describe_diversity_source(teacher_race, "Teacher"))
message(describe_diversity_source(admin_race, "Administrator"))

# NEW: Validate columns and show examples
if (!is.null(teacher_race)) {
  message("\n✓ Teacher diversity columns found:")
  message("  ", paste(head(teacher_race$meta$columns, 3), collapse = "\n  "))
  if (length(teacher_race$meta$columns) > 3) {
    message("  ... and ", length(teacher_race$meta$columns) - 3, " more")
  }

  # Show example values
  sample_col <- teacher_race$meta$columns[1]
  sample_vals <- df[[sample_col]][!is.na(df[[sample_col]])]
  if (length(sample_vals) > 0) {
    message("  Sample values: ", paste(head(sample_vals, 5), collapse = ", "))
  }
}

if (!is.null(admin_race)) {
  message("\n✓ Administrator diversity columns found:")
  message("  ", paste(head(admin_race$meta$columns, 3), collapse = "\n  "))
  if (length(admin_race$meta$columns) > 3) {
    message("  ... and ", length(admin_race$meta$columns) - 3, " more")
  }
}
```

### Priority 2: Structural Improvements

#### Fix 2.1: Use Canonical Race Definitions

**Location:** Lines 26-48

```r
# REMOVE hardcoded lists, replace with:
source("R/teacher_processing.R")
source("R/utils_keys_filters.R")

# Use canonical race lookup
TEACHER_RACE_LOOKUP <- teacher_race_suffix_lookup()
TEACHER_RACE_SLUGS <- TEACHER_RACE_LOOKUP$race_suffix

# Use canonical allowed races (excluding "Not Reported" and "All Students")
ALLOWED_RACE_GROUPS <- setdiff(ALLOWED_RACES, c("Not Reported", "All Students"))
```

#### Fix 2.2: Better Variable Naming

**Location:** Lines 349-354

```r
# OLD (confusing):
model_df <- data.frame(
  suspension_rate = suspension_rate,
  teacher_non_white_share = as.numeric(teacher_race$values),
  admin_non_white_share = as.numeric(admin_race$values),
  stringsAsFactors = FALSE
)

# NEW (clearer):
model_df <- data.frame(
  suspension_rate = suspension_rate,
  all_staff_non_white_share = as.numeric(teacher_race$values),  # "teacher" includes ALL staff
  administrator_non_white_share = as.numeric(admin_race$values),
  stringsAsFactors = FALSE
)
```

#### Fix 2.3: Add Documentation

**Location:** Top of file

```r
# Analysis/21_teacher_diversity_regression.R
#
# PURPOSE:
#   Analyzes associations between staff racial diversity and student suspension
#   rates, stratified by student race/ethnicity.
#
# DATA STRUCTURE:
#   Teacher demographics have TWO dimensions:
#   1. Staff Type: Teachers (TCH), Administrators (ADM), Pupil Services (PSV), Other (OTH), All (ALL)
#   2. Race/Ethnicity: African American, Asian, Hispanic/Latino, White, etc.
#
#   Column naming pattern:
#   - teacher_staff_count_african_american = ALL staff who are African American
#   - teacher_staff_count_by_type_administrators_african_american = ADMINISTRATORS who are African American
#
# REGRESSION VARIABLES:
#   - all_staff_non_white_share: Proportion of all staff who are non-white
#   - administrator_non_white_share: Proportion of administrators who are non-white
#   - suspension_rate: Student suspension rate (outcome)
#   - Controls: SED rate, charter status, grade level
#   - Stratification: Student race/ethnicity groups
#
# PREREQUISITES:
#   1. data-stage/teacher_staff_long.parquet (created by R/01c_ingest_teacher_demographics.R)
#   2. data-stage/susp_v6_long.parquet (created by run_pipeline.R)
#   3. data-stage/susp_v6_teacher_features.parquet (created by Analysis/22_build_teacher_race_shares.R)
#
# USAGE:
#   source("Analysis/21_teacher_diversity_regression.R")
#
```

---

## 11. Testing Plan

After implementing fixes, validate with these tests:

### Test 1: File Existence
```r
stopifnot(file.exists("data-stage/teacher_staff_long.parquet"))
stopifnot(file.exists("data-stage/susp_v6_long.parquet"))
stopifnot(file.exists("data-stage/susp_v6_teacher_features.parquet"))
```

### Test 2: Column Existence
```r
library(arrow)
df <- read_parquet("data-stage/susp_v6_teacher_features.parquet")

# Must have teacher race columns
race_cols <- grep("teacher_.*_(african_american|asian|white).*_share$", names(df), value = TRUE)
stopifnot(length(race_cols) > 0)

# Must have administrator columns
admin_cols <- grep("by_type_administrators.*_share$", names(df), value = TRUE)
stopifnot(length(admin_cols) > 0)

# Must have student_group column
stopifnot("student_group" %in% names(df))
```

### Test 3: Regression Runs Successfully
```r
source("Analysis/21_teacher_diversity_regression.R")

# Should run without errors
results <- main()

# Should produce results for multiple student groups
stopifnot(length(results) > 1)
```

### Test 4: Validate Coefficients
```r
# Coefficients should be finite
for (fit in results) {
  coefs <- coef(fit)
  stopifnot(all(is.finite(coefs)))

  # Should have teacher and admin predictors
  stopifnot("all_staff_non_white_share" %in% names(coefs) ||
            "teacher_non_white_share" %in% names(coefs))
  stopifnot("administrator_non_white_share" %in% names(coefs) ||
            "admin_non_white_share" %in% names(coefs))
}
```

---

## 12. Conclusion

### Current Status: ❌ **NON-FUNCTIONAL**

The script cannot run because:
1. Required data files likely don't exist
2. Prerequisite scripts haven't been run
3. No validation of prerequisites before attempting analysis
4. Silent failures in column detection

### Required Actions:

**Immediate (before script can run):**
1. ✅ Verify raw teacher TXT files exist in `data-raw/`
2. ✅ Run `source('R/01c_ingest_teacher_demographics.R')`
3. ✅ Run `source('Analysis/22_build_teacher_race_shares.R')`
4. ✅ Verify `susp_v6_teacher_features.parquet` was created
5. ✅ Inspect column names in that file

**Code improvements:**
1. Add prerequisite checks with clear error messages
2. Add verbose column detection logging
3. Use canonical race definitions from shared modules
4. Improve variable naming for clarity
5. Add comprehensive documentation
6. Add validation tests

### Estimated Fix Effort:
- **Immediate data fixes:** 30-60 minutes (if raw data available)
- **Code improvements:** 2-4 hours
- **Testing and validation:** 1-2 hours
- **Total:** 3.5-6.5 hours

### Success Criteria:
- ✅ Script runs without errors
- ✅ Detects all expected teacher and administrator race columns
- ✅ Produces regression results for all student groups
- ✅ Results are reproducible
- ✅ Clear error messages guide users when prerequisites are missing

---

**End of Audit**
