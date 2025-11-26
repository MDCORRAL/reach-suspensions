# Fix for Analysis/21_teacher_diversity_regression.R

**Date:** 2025-11-14
**Issue:** Script cannot identify teacher/administrator race/ethnicity factors
**Root Cause:** Uses wrong data loading approach
**Solution:** Apply the correct protocol from `docs/protocols/PROTOCOL_TEACHER_DATA_MERGE.md`

---

## Quick Fix Summary

The regression script is trying to load `susp_v6_teacher_features.parquet`, which may not exist or may not have the expected structure. The **correct approach** (validated by the working script `21_weighted_teacher_diversity_by_quartile.R`) is to:

1. **Load the two source files separately**
2. **Do the merge at runtime**
3. **Add comprehensive diagnostics**

---

## Specific Changes Required

### Change 1: Update Data Loading (Lines 22-24, 223-289)

**BEFORE (broken):**
```r
TEACHER_PATH <- file.path("data-stage", "susp_v6_teacher_features.parquet")
FALLBACK_PATH <- file.path("data-stage", "susp_v6_features.parquet")

load_features <- function() {
  if (!file.exists(TEACHER_PATH)) {
    stop("Missing file: ", TEACHER_PATH)
  }

  df <- as.data.frame(arrow::read_parquet(TEACHER_PATH))
  # ...
}
```

**AFTER (correct):**
```r
# Define paths to SOURCE files
V6_LONG_PATH <- file.path("data-stage", "susp_v6_long.parquet")
TEACHER_LONG_PATH <- file.path("data-stage", "teacher_staff_long.parquet")

load_features <- function() {
  # Check prerequisites
  if (!file.exists(V6_LONG_PATH)) {
    stop("Missing susp_v6_long.parquet. Run run_pipeline.R first.")
  }
  if (!file.exists(TEACHER_LONG_PATH)) {
    stop("Missing teacher_staff_long.parquet.\n",
         "Run: source('R/01c_ingest_teacher_demographics.R')\n",
         "Requires: data-raw/stre*.txt files from CDE")
  }

  message("\n════════════════════════════════════════════════════════════════")
  message("📊 Loading and Merging Teacher-Student Data")
  message("════════════════════════════════════════════════════════════════\n")

  # Load student data (long format)
  message(">>> Loading student data...")
  df_students_raw <- arrow::read_parquet(V6_LONG_PATH)

  # Aggregate to school level (one row per school-year)
  # Use "All Students" to avoid race stratification at this stage
  df_students <- df_students_raw %>%
    filter(
      aggregate_level == "S" | tolower(aggregate_level) == "school",
      !school_code %in% SPECIAL_SCHOOL_CODES,
      category_type == "Race/Ethnicity",
      canon_race_label(subgroup) == "All Students"
    ) %>%
    distinct(cds_school, academic_year, .keep_all = TRUE)

  message(">>> Student data: ", format_number(nrow(df_students)), " school-years")

  # Load teacher processing utilities
  source("R/teacher_processing.R")
  # Helpers like build_keys(), canon_race_label(), and SPECIAL_SCHOOL_CODES
  # live in utils_keys_filters, so source it before calling load_features()
  source("R/utils_keys_filters.R")

  # Load and summarize teacher data
  message(">>> Loading teacher data...")
  teacher_long <- arrow::read_parquet(TEACHER_LONG_PATH) %>%
    janitor::clean_names() %>%
    build_keys()

  message(">>> Summarizing teacher demographics...")
  teacher_summary <- teacher_summarise_long(teacher_long)

  # Sanitize NaN/Inf
  teacher_summary <- teacher_summary %>%
    mutate(across(where(is.numeric), ~ {
      out <- .x
      out[is.nan(out)] <- NA_real_
      dplyr::na_if(out, Inf)
    }))

  # Merge
  message(">>> Merging teacher and student data...")
  df <- df_students %>%
    left_join(
      teacher_summary,
      by = c("academic_year", "cds_school"),
      relationship = "one-to-one"
    )

  message(">>> Merged data: ", format_number(nrow(df)), " rows")

  # Validate teacher columns exist
  teacher_cols <- grep("^teacher_", names(df), value = TRUE)
  message(">>> Found ", length(teacher_cols), " teacher columns")

  if (!length(teacher_cols)) {
    stop("No teacher_* columns found after merge. Check source data.")
  }

  # Debug: Show available teacher columns
  race_cols <- grep("(african|asian|hispanic|white)", teacher_cols, value = TRUE, ignore.case = TRUE)
  message(">>>   Race-related columns: ", length(race_cols))

  if (length(race_cols) > 0) {
    message(">>>   Sample columns:")
    message(">>>     ", paste(head(race_cols, 5), collapse = "\n>>>     "))
  } else {
    stop("ERROR: No teacher race columns found.\n",
         "Expected columns like teacher_staff_count_african_american_share\n",
         "Check that teacher_staff_long.parquet contains race/ethnicity breakdowns.")
  }

  message("\n════════════════════════════════════════════════════════════════\n")

  # Note: df is now at SCHOOL level, not student-race level
  # For student race stratification, would need different approach
  list(data = df, source = "runtime_merge")
}
```

### Change 2: Update Student Group Handling (Lines 253-266)

**Issue:** The current code expects `student_group` column for stratification by student race. But after the fix above, the data is aggregated to school level (no student race stratification).

**Options:**

**Option A: Remove Student Race Stratification** (simpler)
- Only run aggregate regression (all students combined)
- Modify `main()` to skip the student group loop

**Option B: Load Pre-Merged File for Stratification** (if student race stratification is required)
- Keep the current approach of loading `susp_v6_teacher_features.parquet`
- But add validation that the file exists and was created correctly
- This requires running `Analysis/22_build_teacher_race_shares.R` first

**Recommended:** Start with **Option A** to get the script working, then add Option B if stratification is needed.

### Change 3: Add Comprehensive Column Detection (Lines 83-185)

**Add after line 96 in `extract_teacher_race_nonwhite_share()`:**

```r
race_share_cols <- grep(race_share_pattern, names(df), value = TRUE, ignore.case = TRUE)

# NEW: Verbose diagnostics
if (!length(race_share_cols)) {
  message(">>> ⚠ WARNING: No columns matching race pattern")
  message(">>> Pattern searched: ", race_share_pattern)

  # Show what IS available
  all_teacher_cols <- grep("^teacher_", names(df), value = TRUE)
  message(">>> Available teacher columns (", length(all_teacher_cols), " total):")

  if (length(all_teacher_cols) > 0) {
    message(">>>   ", paste(head(all_teacher_cols, 20), collapse = "\n>>>   "))
    if (length(all_teacher_cols) > 20) {
      message(">>>   ... and ", length(all_teacher_cols) - 20, " more")
    }
  } else {
    message(">>>   NONE - teacher data completely missing")
  }

  # Broaden the search...
  # (existing fallback code continues)
}
```

### Change 4: Update Main Function for School-Level Analysis

**BEFORE:**
```r
main <- function() {
  result <- load_features()
  df <- result$data

  # Prepare regressions for each student group
  groups <- if ("student_group" %in% names(df)) {
    unique(df$student_group[!is.na(df$student_group)])
  } else {
    NULL
  }

  if (!is.null(groups)) {
    # Student group stratification
    # ...
  }
}
```

**AFTER:**
```r
main <- function() {
  result <- load_features()
  df <- result$data

  # NOTE: Data is at SCHOOL level (one row per school-year)
  # Cannot stratify by student race unless we use different data source

  message("\n╔════════════════════════════════════════════════════════════════╗")
  message("║     SCHOOL-LEVEL ANALYSIS: Teacher Diversity & Suspensions    ║")
  message("╚════════════════════════════════════════════════════════════════╝\n")

  message("⚠️  Note: Running aggregate analysis (not stratified by student race)")
  message("    To stratify by student race, use susp_v6_teacher_features.parquet\n")

  # Run aggregate regression
  model_info <- prepare_regression_frame(df, student_group = NULL)

  if (!is.null(model_info)) {
    results <- list(run_regression(model_info))
  } else {
    stop("Could not prepare regression data. Check diagnostics above.")
  }

  message("\n╔════════════════════════════════════════════════════════════════╗")
  message("║                       ANALYSIS COMPLETE                        ║")
  message("╚════════════════════════════════════════════════════════════════╝\n")

  message("⚠️  IMPORTANT REMINDERS:")
  message("  • These are ASSOCIATIONS, not causal effects")
  message("  • Results describe correlations in observational data")
  message("  • Do not interpret coefficients as causal impacts\n")

  invisible(results)
}
```

---

## Alternative: Quick Patch to Check Data First

If you want a minimal change to diagnose the issue first:

**Add this at the very beginning of `main()`:**

```r
main <- function() {
  # DIAGNOSTIC: Check what files exist
  message("\n=== DIAGNOSTIC: Checking available data files ===")

  files_to_check <- c(
    "data-stage/susp_v6_long.parquet",
    "data-stage/teacher_staff_long.parquet",
    "data-stage/susp_v6_teacher_features.parquet",
    "data-stage/susp_v6_features.parquet"
  )

  for (f in files_to_check) {
    if (file.exists(f)) {
      size_mb <- file.info(f)$size / 1024^2
      message("✓ ", f, " (", round(size_mb, 1), " MB)")
    } else {
      message("✗ ", f, " - NOT FOUND")
    }
  }

  # If teacher_staff_long.parquet exists, inspect it
  if (file.exists("data-stage/teacher_staff_long.parquet")) {
    message("\n=== Inspecting teacher_staff_long.parquet ===")
    teacher_long <- arrow::read_parquet("data-stage/teacher_staff_long.parquet")
    message("Rows: ", nrow(teacher_long))
    message("Columns: ", ncol(teacher_long))
    message("Sample columns: ", paste(head(names(teacher_long), 10), collapse = ", "))

    # Check for race data
    race_cols <- grep("race|ethnicity|african|asian|white|hispanic", names(teacher_long), value = TRUE, ignore.case = TRUE)
    message("Race-related columns (", length(race_cols), "): ", paste(head(race_cols, 5), collapse = ", "))
  }

  message("\n=== END DIAGNOSTIC ===\n")

  # Continue with normal execution...
}
```

This will show you exactly what data is available and help diagnose the root cause.

---

## Testing the Fix

After applying changes:

```r
# 1. Source the fixed script
source("Analysis/21_teacher_diversity_regression.R")

# 2. Should see diagnostic messages showing:
#    - Files being loaded
#    - Merge happening
#    - Column counts
#    - Sample column names

# 3. Should complete without errors or provide clear error about missing data

# 4. Check that regression results are returned
results <- main()
length(results)  # Should be > 0
```

---

## Summary

The core issue is that the script was trying to load a pre-merged file that may not exist or may not have the expected structure. The fix is to:

1. **Load source files directly** (`susp_v6_long.parquet` + `teacher_staff_long.parquet`)
2. **Do the merge at runtime** using `teacher_summarise_long()`
3. **Add diagnostics** to show what columns are available
4. **Fail gracefully** with actionable error messages

This approach is validated by the working script `21_weighted_teacher_diversity_by_quartile.R` and documented in `docs/protocols/PROTOCOL_TEACHER_DATA_MERGE.md`.
