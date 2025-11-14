# Analysis/22_build_teacher_race_shares.R
# Build wide-format teacher features file with proper racial diversity shares
# from the teacher demographic data, then merge with student suspension data.
#
# This script creates: susp_v6_teacher_features.parquet
# Input files:
#   - teacher_staff_long.parquet (teacher demographics by race)
#   - susp_v6_long.parquet (student suspension data by race)

suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(arrow)
  library(here)
  library(janitor)
})

try(here::i_am("Analysis/22_build_teacher_race_shares.R"), silent = TRUE)

source(here("R", "utils_keys_filters.R"))
source(here("R", "teacher_processing.R"))

TEACHER_PATH <- here("data-stage", "teacher_staff_long.parquet")
STUDENT_PATH <- here("data-stage", "susp_v6_long.parquet")
OUT_PATH <- here("data-stage", "susp_v6_teacher_features.parquet")

message("[22] ========================================")
message("[22] Building Teacher Race Share Features")
message("[22] ========================================\n")

# ============================================================================
# STEP 1: Load and summarize teacher demographics
# ============================================================================

if (!file.exists(TEACHER_PATH)) {
  stop("Missing teacher data: ", TEACHER_PATH, "\nRun R/01c_ingest_teacher_demographics.R first.")
}

message("[22] Loading teacher demographics...")
teacher_long <- read_parquet(TEACHER_PATH) %>%
  clean_names() %>%
  build_keys()

message("[22] Teacher data: ", format(nrow(teacher_long), big.mark = ","), " rows")
message("[22] Columns: ", paste(head(names(teacher_long), 10), collapse = ", "), "...\n")

# Summarize to wide format with race and gender shares
message("[22] Computing teacher race and gender shares by school-year...")
teacher_summary <- teacher_summarise_long(teacher_long)

message("[22] Teacher summary: ", format(nrow(teacher_summary), big.mark = ","), " rows")

# Verify we have race share columns
race_share_cols <- grep("african_american|asian|hispanic|white|filipino",
                        names(teacher_summary), value = TRUE, ignore.case = TRUE)
message("[22] Found ", length(race_share_cols), " teacher race-related columns")

if (length(race_share_cols) == 0) {
  stop("ERROR: No teacher race columns found in summary. Check teacher_summarise_long() function.")
}

# Sample of race columns
message("[22] Sample race columns:")
message("     ", paste(head(race_share_cols, 5), collapse = "\n     "))

# ============================================================================
# STEP 2: Load student suspension data
# ============================================================================

if (!file.exists(STUDENT_PATH)) {
  stop("Missing student data: ", STUDENT_PATH, "\nRun run_pipeline.R first.")
}

message("\n[22] Loading student suspension data...")
student_long <- read_parquet(STUDENT_PATH) %>%
  clean_names() %>%
  build_keys()

message("[22] Student data: ", format(nrow(student_long), big.mark = ","), " rows")

# Check for student_group column (race/ethnicity of students)
if ("reporting_category" %in% names(student_long)) {
  # Rename to student_group for clarity
  student_long <- student_long %>%
    rename(student_group = reporting_category)
  message("[22] Renamed 'reporting_category' to 'student_group'")
}

if (!"student_group" %in% names(student_long)) {
  warning("[22] WARNING: No 'student_group' column found. Cannot create race-specific features.")
  student_long <- student_long %>%
    mutate(student_group = "All Students")
}

message("[22] Student groups: ", paste(sort(unique(student_long$student_group)), collapse = ", "))

# ============================================================================
# STEP 3: Merge teacher and student data
# ============================================================================

message("\n[22] Merging teacher demographics with student suspension data...")

# Join keys: academic_year + cds_school
join_keys <- c("academic_year", "cds_school")

# LEFT JOIN: keep all student data, attach teacher data where available
combined <- student_long %>%
  left_join(teacher_summary, by = join_keys, relationship = "many-to-one")

message("[22] Merged data: ", format(nrow(combined), big.mark = ","), " rows")

# Check merge coverage
teacher_cols <- grep("^teacher_", names(combined), value = TRUE)
if (length(teacher_cols) > 0) {
  coverage <- combined %>%
    mutate(has_teacher = if_any(all_of(teacher_cols), ~!is.na(.x))) %>%
    summarise(
      total_rows = n(),
      with_teacher = sum(has_teacher, na.rm = TRUE),
      pct = round(100 * with_teacher / total_rows, 1)
    )

  message("[22] Teacher coverage: ", format(coverage$with_teacher, big.mark = ","),
          " / ", format(coverage$total_rows, big.mark = ","),
          " rows (", coverage$pct, "%)")

  # School-level coverage
  school_cov <- combined %>%
    distinct(cds_school, academic_year, .keep_all = TRUE) %>%
    mutate(has_teacher = if_any(all_of(teacher_cols), ~!is.na(.x))) %>%
    summarise(
      schools = n(),
      with_teacher = sum(has_teacher, na.rm = TRUE),
      pct = round(100 * with_teacher / schools, 1)
    )

  message("[22] School coverage: ", format(school_cov$with_teacher, big.mark = ","),
          " / ", format(school_cov$schools, big.mark = ","),
          " campus-years (", school_cov$pct, "%)")
}

# ============================================================================
# STEP 4: Clean and validate
# ============================================================================

message("\n[22] Cleaning and validating...")

# Sanitize NaN and Inf values in teacher columns
combined <- combined %>%
  mutate(across(all_of(teacher_cols), ~{
    out <- .x
    out[is.nan(out)] <- NA_real_
    out[is.infinite(out)] <- NA_real_
    out
  }))

# Verify race share columns present
final_race_cols <- grep("african_american|asian|hispanic|white|filipino",
                        teacher_cols, value = TRUE, ignore.case = TRUE)
message("[22] Final dataset has ", length(final_race_cols), " teacher race-related columns")

if (length(final_race_cols) == 0) {
  stop("ERROR: Teacher race columns missing from final dataset!")
}

# ============================================================================
# STEP 5: Write output
# ============================================================================

message("\n[22] Writing output: ", OUT_PATH)
write_parquet(combined, OUT_PATH)

message("[22] SUCCESS!")
message("[22] Output: ", format(nrow(combined), big.mark = ","), " rows x ",
        ncol(combined), " columns")
message("[22] File size: ", format(file.info(OUT_PATH)$size / 1024^2, digits = 1), " MB")

# ============================================================================
# STEP 6: Generate diagnostic report
# ============================================================================

message("\n[22] ========================================")
message("[22] Diagnostic Summary")
message("[22] ========================================")

message("\nKey dimensions:")
message("  - Unique schools: ", n_distinct(combined$cds_school))
message("  - Academic years: ", paste(sort(unique(combined$academic_year)), collapse = ", "))
message("  - Student groups: ", paste(sort(unique(combined$student_group)), collapse = ", "))

message("\nTeacher race columns available:")
for (col in head(final_race_cols, 10)) {
  non_missing <- sum(!is.na(combined[[col]]))
  pct <- round(100 * non_missing / nrow(combined), 1)
  message(sprintf("  %-60s %8d rows (%5.1f%%)", col, non_missing, pct))
}

if (length(final_race_cols) > 10) {
  message("  ... and ", length(final_race_cols) - 10, " more race-related columns")
}

message("\n[22] File ready for regression analysis!")
message("[22] Use this file in Analysis/21_teacher_diversity_regression.R")

invisible(TRUE)
