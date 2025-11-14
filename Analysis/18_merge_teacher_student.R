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
  coverage <- combined %>%
    mutate(has_teacher = if_any(all_of(teacher_cols), ~ !is.na(.x))) %>%
    summarise(
      total_rows = dplyr::n(),
      with_teacher   = sum(has_teacher, na.rm = TRUE)
    )
  message("[18] Teacher coverage: ", coverage$with_teacher, " of ", coverage$total_rows, " student subgroup rows.")

  # Also report unique school coverage
  school_coverage <- combined %>%
    distinct(cds_school, academic_year, .keep_all = TRUE) %>%
    mutate(has_teacher = if_any(all_of(teacher_cols), ~ !is.na(.x))) %>%
    summarise(
      unique_schools = dplyr::n(),
      schools_with_teacher = sum(has_teacher, na.rm = TRUE)
    )
  message("[18] Unique school coverage: ", school_coverage$schools_with_teacher, " of ", school_coverage$unique_schools, " campus-years.")
} else {
  warning("No teacher_* columns present after join.")
}

arrow::write_parquet(combined, OUT_PATH)
message("[18] Wrote ", OUT_PATH, " (rows: ", nrow(combined), ")")

invisible(TRUE)
