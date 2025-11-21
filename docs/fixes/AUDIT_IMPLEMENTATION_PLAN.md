# Audit Implementation Plan: Missing Recommendations

**Created**: 2025-11-21
**Status**: DRAFT - Awaiting approval
**Priority**: HIGH (blocks publication)

---

## Executive Summary

This document provides a complete implementation plan for the 4 missing audit recommendations identified in the analytic script review. Each recommendation includes:
- Exact code to add
- File locations (line numbers)
- Testing procedures
- Expected outputs
- Risk mitigation

**Estimated implementation time**: 2-3 hours (including testing)

---

## Table of Contents

1. [Recommendation #1: Version Pinning](#recommendation-1-version-pinning)
2. [Recommendation #2: Reason Reconciliation](#recommendation-2-reason-reconciliation)
3. [Recommendation #3: Teacher Coverage Persistence](#recommendation-3-teacher-coverage-persistence)
4. [Recommendation #4: Environment Documentation](#recommendation-4-environment-documentation)
5. [Testing Protocol](#testing-protocol)
6. [Rollback Plan](#rollback-plan)

---

## Recommendation #1: Version Pinning

### Priority: 🔴 **CRITICAL** (High risk if not implemented)

### Files to Modify:
1. `Analysis/16_tail_concentration_analysis.R`
2. `Analysis/17_tail_by_grade-school_concentration_analysis.R`
3. `Analysis/17_tail_concentration_by_level.R`

### Problem:
Scripts can silently pair mismatched data versions (e.g., `susp_v6_long.parquet` with `susp_v5_features.parquet`), causing schema inconsistencies.

### Solution:
Add explicit version matching enforcement after file selection.

---

### Implementation: File 1 - `16_tail_concentration_analysis.R`

**Insert after line 127** (after `message("Using features: ...")`):

```r
## -------------------------------------------------------------------------
## Enforce version matching (Audit Recommendation #1)
## -------------------------------------------------------------------------

# Extract numeric versions from selected files
input_version_num <- stringr::str_extract(basename(INPUT_PATH), "(?<=v)[0-9]+")
feature_version_num <- stringr::str_extract(basename(FEATURE_PATH), "(?<=v)[0-9]+")

# Enforce exact version match
if (is.na(input_version_num) || is.na(feature_version_num)) {
  stop(
    "VERSION DETECTION FAILED:\n",
    "  Suspension file: ", basename(INPUT_PATH), " (version: ", input_version_num, ")\n",
    "  Features file: ", basename(FEATURE_PATH), " (version: ", feature_version_num, ")\n",
    "Cannot proceed without valid version numbers."
  )
}

if (input_version_num != feature_version_num) {
  stop(
    "VERSION MISMATCH DETECTED:\n",
    "  Suspension data: ", basename(INPUT_PATH), " (v", input_version_num, ")\n",
    "  Features data: ", basename(FEATURE_PATH), " (v", feature_version_num, ")\n",
    "\n",
    "These files must use the same version to ensure schema compatibility.\n",
    "\n",
    "To fix:\n",
    "  1. Check data-stage/ for matching versions\n",
    "  2. Re-run pipeline if needed: source('run_pipeline.R')\n",
    "  3. Ensure 22_build_v6_features.R completed successfully\n"
  )
}

message("✓ Version check passed: Both files are v", input_version_num)
```

**Why this location?**
Immediately after file selection ensures no downstream code runs with mismatched data.

**Expected console output**:
```
Using suspension data: susp_v6_long.parquet
Using features: susp_v6_features.parquet
✓ Version check passed: Both files are v6
```

**Error output (if mismatch)**:
```
Error: VERSION MISMATCH DETECTED:
  Suspension data: susp_v6_long.parquet (v6)
  Features data: susp_v5_features.parquet (v5)

These files must use the same version to ensure schema compatibility.

To fix:
  1. Check data-stage/ for matching versions
  2. Re-run pipeline if needed: source('run_pipeline.R')
  3. Ensure 22_build_v6_features.R completed successfully
```

---

### Implementation: File 2 - `17_tail_by_grade-school_concentration_analysis.R`

**Insert after line 132** (after `message("Using features: ...")`):

```r
## -------------------------------------------------------------------------
## Enforce version matching (Audit Recommendation #1)
## -------------------------------------------------------------------------

input_version_num <- stringr::str_extract(basename(INPUT_PATH), "(?<=v)[0-9]+")
feature_version_num <- stringr::str_extract(basename(FEATURE_PATH), "(?<=v)[0-9]+")

if (is.na(input_version_num) || is.na(feature_version_num)) {
  stop(
    "VERSION DETECTION FAILED:\n",
    "  Suspension file: ", basename(INPUT_PATH), " (version: ", input_version_num, ")\n",
    "  Features file: ", basename(FEATURE_PATH), " (version: ", feature_version_num, ")\n"
  )
}

if (input_version_num != feature_version_num) {
  stop(
    "VERSION MISMATCH: ", basename(INPUT_PATH), " (v", input_version_num, ") != ",
    basename(FEATURE_PATH), " (v", feature_version_num, ")\n",
    "Run source('run_pipeline.R') to regenerate matching versions."
  )
}

message("✓ Version check passed: v", input_version_num)
```

---

### Implementation: File 3 - `17_tail_concentration_by_level.R`

**Insert after line 106** (after `message("Using features: ...")`):

```r
## -------------------------------------------------------------------------
## Enforce version matching (Audit Recommendation #1)
## -------------------------------------------------------------------------

input_version_num <- stringr::str_extract(basename(INPUT_PATH), "(?<=v)[0-9]+")
feature_version_num <- stringr::str_extract(basename(FEATURE_PATH), "(?<=v)[0-9]+")

if (is.na(input_version_num) || is.na(feature_version_num)) {
  stop("VERSION DETECTION FAILED: Cannot extract version numbers from file names.")
}

if (input_version_num != feature_version_num) {
  stop(
    "VERSION MISMATCH: Suspension v", input_version_num,
    " incompatible with Features v", feature_version_num
  )
}

message("✓ Version check passed: v", input_version_num)
```

---

### Testing for Recommendation #1:

**Test 1: Normal operation (should pass)**
```r
# Ensure matching versions exist
source("run_pipeline.R")  # Creates v6_long and v6_features

# Run script - should succeed
source("Analysis/16_tail_concentration_analysis.R")
# Expected: "✓ Version check passed: Both files are v6"
```

**Test 2: Version mismatch (should fail)**
```r
# Temporarily rename to simulate mismatch
file.rename(
  "data-stage/susp_v6_features.parquet",
  "data-stage/susp_v5_features.parquet"
)

# Run script - should error
source("Analysis/16_tail_concentration_analysis.R")
# Expected: Error with clear message

# Restore
file.rename(
  "data-stage/susp_v5_features.parquet",
  "data-stage/susp_v6_features.parquet"
)
```

---

## Recommendation #2: Reason Reconciliation

### Priority: 🔴 **CRITICAL** (High risk of data quality issues)

### File to Modify:
`Analysis/02_black_rates_by_quartiles.R`

### Problem:
When deriving suspension counts from proportions (lines 140-170), no validation checks that the derived totals match `total_suspensions`. Rounding errors or missing categories can cause 5-10% undercounts.

### Solution:
Add reconciliation validation after deriving reason counts, emit warnings and diagnostic files when discrepancies exceed 1%.

---

### Implementation: `02_black_rates_by_quartiles.R`

**Create new helper function** (insert after line 67, before `create_total_rate_plot`):

```r
# --- 2b) Reason Reconciliation Helper (Audit Recommendation #2) -------------
#' Validate that derived reason counts sum to total_suspensions
#' @param plot_data Data frame with suspension_count and total_suspensions
#' @param group_var Grouping variable (e.g., black_prop_q_label)
#' @return Validation summary (invisibly), with side effect of writing audit file
validate_reason_totals <- function(plot_data, group_var) {
  gsym <- rlang::ensym(group_var)

  # Group by year × quartile and sum reason counts
  validation <- plot_data %>%
    group_by(academic_year, !!gsym) %>%
    summarise(
      # Sum of all reason-specific counts
      derived_total = sum(suspension_count, na.rm = TRUE),
      # Original total from data (should be same for all reasons in group)
      original_total = first(total_suspensions),
      n_reasons = n_distinct(reason),
      .groups = "drop"
    ) %>%
    mutate(
      # Calculate absolute and percentage difference
      abs_diff = abs(derived_total - original_total),
      pct_diff = if_else(
        original_total > 0,
        abs_diff / original_total,
        NA_real_
      ),
      # Flag significant discrepancies (>1%)
      is_discrepancy = !is.na(pct_diff) & pct_diff > 0.01
    )

  # Count and report issues
  n_issues <- sum(validation$is_discrepancy, na.rm = TRUE)

  if (n_issues > 0) {
    warning(
      "REASON RECONCILIATION ISSUES DETECTED:\n",
      "  ", n_issues, " year × quartile groups have reason totals differing from total_suspensions by >1%\n",
      "  Max discrepancy: ", scales::percent(max(validation$pct_diff, na.rm = TRUE), accuracy = 0.1), "\n",
      "  Audit file: outputs/data_audit/reason_reconciliation_issues.csv\n",
      "\n",
      "Possible causes:\n",
      "  - Rounding errors in proportion calculations\n",
      "  - Missing suspension reason categories\n",
      "  - Data suppression (asterisks converted to NA)\n",
      "\n",
      "Review audit file before using these rates in publications."
    )

    # Write detailed audit file
    dir.create(here::here("outputs", "data_audit"), showWarnings = FALSE, recursive = TRUE)

    issues <- validation %>%
      filter(is_discrepancy) %>%
      arrange(desc(pct_diff)) %>%
      mutate(
        pct_diff_label = scales::percent(pct_diff, accuracy = 0.01),
        abs_diff_label = scales::comma(abs_diff, accuracy = 1)
      ) %>%
      select(
        academic_year, quartile = !!gsym,
        original_total, derived_total, abs_diff_label, pct_diff_label,
        n_reasons
      )

    readr::write_csv(
      issues,
      here::here("outputs", "data_audit", "reason_reconciliation_issues.csv")
    )

    message("  Wrote ", nrow(issues), " discrepancy records to audit file")
  } else {
    message("✓ Reason reconciliation check passed: All totals within 1% of expected")
  }

  # Return validation summary invisibly for optional inspection
  invisible(validation)
}
```

**Modify `create_category_rate_plot` function** - add validation call after line 167:

Find this section (lines 140-167):
```r
  } else if (length(prop_cols) > 0) {
    # Derive counts from proportions × total_suspensions
    plot_data <- data %>%
      filter(!is.na(!!gsym)) %>%
      select(academic_year, !!gsym, total_suspensions, cumulative_enrollment, all_of(prop_cols)) %>%
      pivot_longer(all_of(prop_cols), names_to = "prop_name", values_to = "prop") %>%
      mutate(
        reason = sub("^prop_susp_", "", prop_name),
        reason_count = prop * total_suspensions
      ) %>%
      mutate(reason = dplyr::case_match(
        reason,
        "violent_incident_injury"    ~ "violent_injury",
        "violent_incident_no_injury" ~ "violent_no_injury",
        "illicit_drug_related"       ~ "illicit_drug",
        .default = reason
      )) %>%
      group_by(academic_year, !!gsym, reason) %>%
      summarise(
        suspension_count = sum(reason_count, na.rm = TRUE),
        total_enrollment = sum(cumulative_enrollment, na.rm = TRUE),
        .groups = "drop"
      ) %>%
      add_reason_label("reason") %>%
      mutate(
        reason_rate = if_else(total_enrollment > 0, suspension_count / total_enrollment, NA_real_),
        year_fct    = factor(academic_year, levels = year_levels)
      )
```

**Insert immediately after line 167** (before the closing `}` of the else-if block):
```r

    # Validate derived totals (Audit Recommendation #2)
    # Add total_suspensions to plot_data for validation
    plot_data_with_totals <- plot_data %>%
      left_join(
        data %>%
          distinct(academic_year, !!gsym, total_suspensions),
        by = c("academic_year", rlang::as_name(gsym))
      )

    validate_reason_totals(plot_data_with_totals, !!gsym)
```

---

### Expected Output (normal case):

**Console**:
```
Generating plots...
✓ Reason reconciliation check passed: All totals within 1% of expected
```

**No audit file created** (outputs/data_audit/ remains empty for this check)

---

### Expected Output (discrepancy detected):

**Console**:
```
Generating plots...
Warning: REASON RECONCILIATION ISSUES DETECTED:
  8 year × quartile groups have reason totals differing from total_suspensions by >1%
  Max discrepancy: 7.2%
  Audit file: outputs/data_audit/reason_reconciliation_issues.csv

Possible causes:
  - Rounding errors in proportion calculations
  - Missing suspension reason categories
  - Data suppression (asterisks converted to NA)

Review audit file before using these rates in publications.
  Wrote 8 discrepancy records to audit file
```

**Audit file created**: `outputs/data_audit/reason_reconciliation_issues.csv`
```csv
academic_year,quartile,original_total,derived_total,abs_diff_label,pct_diff_label,n_reasons
2023-24,Q4 (>50% Black),15243,14145,1098,7.20%,6
2022-23,Q4 (>50% Black),14892,13912,980,6.58%,6
2023-24,Q3 (25-50% Black),8234,7986,248,3.01%,6
```

---

### Testing for Recommendation #2:

**Test 1: Normal operation**
```r
source("Analysis/02_black_rates_by_quartiles.R")
# Expected: "✓ Reason reconciliation check passed"
```

**Test 2: Simulate discrepancy**
```r
# Temporarily modify v6 data to introduce mismatch
library(arrow); library(dplyr)
v6_test <- read_parquet("data-stage/susp_v6_long.parquet") %>%
  mutate(
    # Artificially reduce one reason proportion
    prop_susp_defiance_only = if_else(
      black_prop_q == 4,
      prop_susp_defiance_only * 0.9,  # 10% reduction
      prop_susp_defiance_only
    )
  )

write_parquet(v6_test, "data-stage/susp_v6_long_TEST.parquet")

# Modify script to use test file
# ... run script ...
# Expected: Warning with audit file

# Clean up
file.remove("data-stage/susp_v6_long_TEST.parquet")
```

---

## Recommendation #3: Teacher Coverage Persistence

### Priority: 🟡 **MEDIUM** (Improves transparency, not blocking)

### File to Modify:
`Analysis/18_merge_teacher_student.R`

### Problem:
Teacher data coverage is logged to console but not saved, preventing trend analysis and audit trails.

### Solution:
Persist coverage statistics to `outputs/data_audit/teacher_coverage_by_year.csv`.

---

### Implementation: `18_merge_teacher_student.R`

**Replace lines 64-84** with this enhanced version:

```r
teacher_cols <- grep("^teacher_", names(combined), value = TRUE)
if (length(teacher_cols)) {

  # --- Row-level coverage (all subgroups) ---
  coverage <- combined %>%
    mutate(has_teacher = if_any(all_of(teacher_cols), ~ !is.na(.x))) %>%
    summarise(
      total_rows = dplyr::n(),
      with_teacher   = sum(has_teacher, na.rm = TRUE)
    )
  message("[18] Teacher coverage: ", coverage$with_teacher, " of ", coverage$total_rows, " student subgroup rows.")

  # --- School-level coverage (unique campuses) ---
  school_coverage <- combined %>%
    distinct(cds_school, academic_year, .keep_all = TRUE) %>%
    mutate(has_teacher = if_any(all_of(teacher_cols), ~ !is.na(.x))) %>%
    summarise(
      unique_schools = dplyr::n(),
      schools_with_teacher = sum(has_teacher, na.rm = TRUE)
    )
  message("[18] Unique school coverage: ", school_coverage$schools_with_teacher, " of ", school_coverage$unique_schools, " campus-years.")

  # --- Year-by-year coverage report (Audit Recommendation #3) ---
  message("[18] Generating coverage audit report by year...")

  coverage_by_year <- combined %>%
    distinct(cds_school, academic_year, .keep_all = TRUE) %>%
    mutate(has_teacher = if_any(all_of(teacher_cols), ~ !is.na(.x))) %>%
    group_by(academic_year) %>%
    summarise(
      unique_schools = n(),
      schools_with_teacher = sum(has_teacher, na.rm = TRUE),
      schools_without_teacher = sum(!has_teacher, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    mutate(
      coverage_pct_schools = round(100 * schools_with_teacher / unique_schools, 1),
      # Add coverage quality tier
      coverage_tier = case_when(
        coverage_pct_schools >= 80 ~ "High (≥80%)",
        coverage_pct_schools >= 50 ~ "Moderate (50-79%)",
        coverage_pct_schools >= 20 ~ "Low (20-49%)",
        TRUE ~ "Very Low (<20%)"
      )
    ) %>%
    arrange(academic_year)

  # Create outputs/data_audit if needed
  dir.create(here::here("outputs", "data_audit"), showWarnings = FALSE, recursive = TRUE)

  # Write coverage report
  coverage_path <- here::here("outputs", "data_audit", "teacher_coverage_by_year.csv")
  readr::write_csv(coverage_by_year, coverage_path)

  message("[18] Coverage audit saved: ", coverage_path)
  message("[18] Coverage summary:")
  print(coverage_by_year %>% select(academic_year, unique_schools, coverage_pct_schools, coverage_tier))

  # Flag low-coverage years
  low_coverage_years <- coverage_by_year %>%
    filter(coverage_pct_schools < 50) %>%
    pull(academic_year)

  if (length(low_coverage_years) > 0) {
    warning(
      "LOW TEACHER COVERAGE (<50%) in years: ",
      paste(low_coverage_years, collapse = ", "),
      "\nUse caution when analyzing teacher diversity metrics for these years."
    )
  }

} else {
  warning("No teacher_* columns present after join.")
}
```

---

### Expected Output:

**Console**:
```
[18] Teacher coverage: 45632 of 48901 student subgroup rows.
[18] Unique school coverage: 8234 of 9012 campus-years.
[18] Generating coverage audit report by year...
[18] Coverage audit saved: /home/user/reach-suspensions/outputs/data_audit/teacher_coverage_by_year.csv
[18] Coverage summary:
# A tibble: 7 × 4
  academic_year unique_schools coverage_pct_schools coverage_tier
  <chr>                  <int>                <dbl> <chr>
1 2017-18                 1234                  23.4 Low (20-49%)
2 2018-19                 1289                  67.8 Moderate (50-79%)
3 2019-20                 1305                  82.1 High (≥80%)
4 2020-21                 1198                  79.3 Moderate (50-79%)
5 2021-22                 1342                  84.5 High (≥80%)
6 2022-23                 1389                  86.2 High (≥80%)
7 2023-24                 1455                  88.9 High (≥80%)

Warning: LOW TEACHER COVERAGE (<50%) in years: 2017-18
Use caution when analyzing teacher diversity metrics for these years.
```

**File created**: `outputs/data_audit/teacher_coverage_by_year.csv`
```csv
academic_year,unique_schools,schools_with_teacher,schools_without_teacher,coverage_pct_schools,coverage_tier
2017-18,1234,289,945,23.4,"Low (20-49%)"
2018-19,1289,874,415,67.8,"Moderate (50-79%)"
2019-20,1305,1071,234,82.1,"High (≥80%)"
2020-21,1198,950,248,79.3,"Moderate (50-79%)"
2021-22,1342,1134,208,84.5,"High (≥80%)"
2022-23,1389,1197,192,86.2,"High (≥80%)"
2023-24,1455,1293,162,88.9,"High (≥80%)"
```

---

### Testing for Recommendation #3:

**Test 1: Normal operation**
```r
source("Analysis/18_merge_teacher_student.R")

# Check file exists
stopifnot(file.exists("outputs/data_audit/teacher_coverage_by_year.csv"))

# Verify structure
library(readr)
cov <- read_csv("outputs/data_audit/teacher_coverage_by_year.csv", show_col_types = FALSE)
stopifnot(all(c("academic_year", "coverage_pct_schools", "coverage_tier") %in% names(cov)))

message("✓ Coverage report validated")
```

**Test 2: Check coverage trends**
```r
library(ggplot2)
cov <- read_csv("outputs/data_audit/teacher_coverage_by_year.csv")

ggplot(cov, aes(x = academic_year, y = coverage_pct_schools)) +
  geom_line(group = 1, linewidth = 1) +
  geom_point(size = 3) +
  geom_hline(yintercept = 50, linetype = "dashed", color = "red") +
  labs(
    title = "Teacher Data Coverage Over Time",
    subtitle = "Percentage of schools with any teacher demographic data",
    y = "Coverage (%)",
    x = "Academic Year"
  ) +
  theme_minimal()

ggsave("outputs/data_audit/teacher_coverage_trend.png", width = 10, height = 6, dpi = 300)
```

---

## Recommendation #4: Environment Documentation

### Priority: 🟢 **LOW** (Nice to have, not critical)

### File to Modify:
`Analysis/21_QUICKSTART.md`

### Problem:
Documentation doesn't explain optional environment variables for custom data paths.

### Solution:
Add environment variable section to prerequisites.

---

### Implementation: `21_QUICKSTART.md`

**Insert after line 32** (after R packages section):

```markdown

✅ **Environment variables** (optional):
```bash
# For custom data paths, copy and edit .Renviron:
cp .Renviron.example .Renviron

# Then edit .Renviron to set:
# RAW_PATH=/path/to/copy_CDE_suspensions_1718-2324_sc_race.xlsx
# OTH_RAW_PATH=/path/to/copy_CDE_suspensions_1718-2324_sc_oth.xlsx
# REACH_DATA_DIR=/custom/path/to/data-stage
```

**Why set these?**
- Use data files outside the default `data-raw/` directory
- Point to network drives or shared folders
- Keep multiple versions for testing

**Not needed if:** You're using default paths (`data-raw/` and `data-stage/`)

```

---

### Testing for Recommendation #4:

**Visual inspection only** - verify markdown renders correctly:

```bash
# View in terminal
cat Analysis/21_QUICKSTART.md | grep -A 10 "Environment variables"

# Or open in GitHub/RStudio to check formatting
```

---

## Testing Protocol

### Phase 1: Unit Tests (30 min)

**Test each recommendation independently:**

```r
# Test #1: Version pinning
source("Analysis/16_tail_concentration_analysis.R")
# Expected: "✓ Version check passed: Both files are v6"

source("Analysis/17_tail_by_grade-school_concentration_analysis.R")
# Expected: "✓ Version check passed: v6"

source("Analysis/17_tail_concentration_by_level.R")
# Expected: "✓ Version check passed: v6"

# Test #2: Reason reconciliation
source("Analysis/02_black_rates_by_quartiles.R")
# Expected: "✓ Reason reconciliation check passed" (or warning with audit file)

# Test #3: Teacher coverage
source("Analysis/18_merge_teacher_student.R")
stopifnot(file.exists("outputs/data_audit/teacher_coverage_by_year.csv"))

# Test #4: Documentation
# Visual check: cat Analysis/21_QUICKSTART.md
```

---

### Phase 2: Integration Test (30 min)

**Run full pipeline to ensure no breakage:**

```r
# Full pipeline from scratch
source("run_all.R")

# Check for new audit files
list.files("outputs/data_audit", pattern = "*.csv", full.names = TRUE)
# Expected:
#   outputs/data_audit/teacher_coverage_by_year.csv
#   outputs/data_audit/reason_reconciliation_issues.csv (if discrepancies exist)
```

---

### Phase 3: Error Simulation (20 min)

**Test that validations actually catch errors:**

```r
## Test version mismatch detection
# Rename to simulate v5
file.rename(
  "data-stage/susp_v6_features.parquet",
  "data-stage/susp_v5_features_TEMP.parquet"
)

# Should error
tryCatch(
  source("Analysis/16_tail_concentration_analysis.R"),
  error = function(e) {
    message("✓ Caught version mismatch error:")
    message(e$message)
  }
)

# Restore
file.rename(
  "data-stage/susp_v5_features_TEMP.parquet",
  "data-stage/susp_v6_features.parquet"
)

## Test reason reconciliation (manual data modification needed)
# See "Test 2: Simulate discrepancy" under Rec #2
```

---

### Phase 4: Validation Checklist (10 min)

- [ ] All scripts run without errors
- [ ] Version checks produce informative errors when mismatch occurs
- [ ] Reason reconciliation either passes or emits clear warning + audit file
- [ ] Teacher coverage report exists and contains all years
- [ ] Documentation renders correctly in markdown viewer
- [ ] No new files in git status (all outputs in gitignored directories)

---

## Rollback Plan

If issues arise during implementation:

### Option 1: Revert individual changes
```bash
# Revert specific file
git checkout HEAD -- Analysis/16_tail_concentration_analysis.R

# Or restore from backup (if created)
cp Analysis/16_tail_concentration_analysis.R.bak Analysis/16_tail_concentration_analysis.R
```

### Option 2: Full rollback to pre-implementation state
```bash
# Create backup branch first
git branch pre-audit-implementation

# After testing, if problems:
git reset --hard pre-audit-implementation
```

### Option 3: Cherry-pick working recommendations
```bash
# Implement only Rec #3 and #4 (low risk)
# Skip Rec #1 and #2 for further review
```

---

## Success Criteria

Implementation is **complete and approved** when:

1. ✅ All 4 code patches applied successfully
2. ✅ Full pipeline (`run_all.R`) completes without errors
3. ✅ Audit files generated in `outputs/data_audit/`:
   - `teacher_coverage_by_year.csv`
   - `reason_reconciliation_issues.csv` (if applicable)
4. ✅ Version mismatch errors are clear and actionable
5. ✅ No silent failures (all validations either pass or error/warn explicitly)
6. ✅ Documentation updated and renders correctly

---

## Timeline

| Phase | Duration | Owner | Status |
|-------|----------|-------|--------|
| Review plan | 15 min | User | ⏳ Pending |
| Approve plan | 5 min | User | ⏳ Pending |
| Implement Rec #1 (version pinning) | 30 min | Claude | ⏳ Pending |
| Implement Rec #2 (reason reconciliation) | 45 min | Claude | ⏳ Pending |
| Implement Rec #3 (teacher coverage) | 20 min | Claude | ⏳ Pending |
| Implement Rec #4 (documentation) | 5 min | Claude | ⏳ Pending |
| Unit tests | 30 min | Claude | ⏳ Pending |
| Integration test | 30 min | Claude | ⏳ Pending |
| **Total** | **~3 hours** | | |

---

## Questions for User

Before proceeding with implementation:

1. **Priority**: Implement all 4 recommendations, or start with critical ones (#1-#2) only?
2. **Backup**: Create `.bak` backup files before modifying scripts?
3. **Testing**: Run full integration test (`run_all.R`) as part of implementation, or separately?
4. **Git**: Create implementation branch (`audit-fixes`) or work on current branch?
5. **Threshold**: Is 1% the right threshold for reason reconciliation warnings, or prefer 0.5% or 2%?

---

## Next Steps

**Ready to proceed?** I can implement all 4 recommendations now. Just confirm:

```
Option A: Implement all 4 recommendations immediately
Option B: Implement critical only (#1-#2), defer #3-#4
Option C: Provide additional detail on specific recommendation
Option D: Modify the plan before implementing
```

**Command to start implementation:**
```
Ready when you are - just say "Implement all" or specify which recommendations.
```

---

**End of Implementation Plan**
