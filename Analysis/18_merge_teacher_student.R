# Analysis/18_merge_teacher_student.R
# Merge teacher demographic summaries with suspension features.

suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(stringr)
  library(janitor)
  library(arrow)
  library(here)
})

try(here::i_am("Analysis/18_merge_teacher_student.R"), silent = TRUE)

source(here("R", "utils_keys_filters.R"))
source(here("R", "teacher_processing.R"))

TEACHER_PATH <- here("data-stage", "teacher_staff_long.parquet")
V6_PATH      <- here("data-stage", "susp_v6_long.parquet")  # CHANGED: Use long format for race-specific data
OUT_PATH     <- here("data-stage", "susp_v6_teacher_long.parquet")  # CHANGED: Output reflects long format

if (!file.exists(TEACHER_PATH)) {
  stop("Missing teacher parquet: ", TEACHER_PATH, "\nRun R/01c_ingest_teacher_demographics.R first.")
}
if (!file.exists(V6_PATH)) {
  stop("Missing v6 long parquet: ", V6_PATH, "\nRun run_pipeline.R first.")
}

message("[18] Loading teacher long parquet ...")
teacher_long <- arrow::read_parquet(TEACHER_PATH) %>%
  janitor::clean_names() %>%
  build_keys()

message("[18] Summarising teacher demographics ...")
teacher_summary <- teacher_summarise_long(teacher_long)

total_cols <- grep("^teacher_.*_total$", names(teacher_summary), value = TRUE)
if (!length(total_cols)) {
  warning("Teacher summary has no numeric totals; downstream joins may be sparse.")
}

teacher_summary <- teacher_summary %>%
  mutate(across(where(is.numeric), ~ {
    out <- .x
    out[is.nan(out)] <- NA_real_
    dplyr::na_if(out, Inf)
  }))

teacher_summary <- assert_unique_campus(teacher_summary, campus_col = "cds_school", year_col = "academic_year")

message("[18] Loading suspension v6 long (race-specific data) ...")
v6 <- arrow::read_parquet(V6_PATH) %>%
  janitor::clean_names() %>%
  build_keys()

# Note: v6_long has multiple rows per school-year (one per race/subgroup), so we don't assert unique campus

join_keys <- c("academic_year", "cds_school")
message("[18] Joining teacher metrics onto v6 long (many student race rows to one teacher summary) ...")
combined <- v6 %>%
  left_join(teacher_summary, by = join_keys, relationship = "many-to-one")

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

arrow::write_parquet(combined, OUT_PATH)
message("[18] Wrote ", OUT_PATH, " (rows: ", nrow(combined), ")")

invisible(TRUE)
