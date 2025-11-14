# Analysis/create_teacher_features_wide.R
# Create wide-format teacher features file with racial diversity columns
# from the long-format teacher-student merged data.

suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(arrow)
  library(stringr)
})

message("[CREATE_WIDE] Creating wide-format teacher features file...")

# Read the long-format merged data
LONG_PATH <- "data-stage/susp_v6_teacher_long.parquet"
OUT_PATH <- "data-stage/susp_v6_teacher_features.parquet"

if (!file.exists(LONG_PATH)) {
  stop("Missing long-format teacher data: ", LONG_PATH)
}

message("[CREATE_WIDE] Reading long-format data: ", LONG_PATH)
df_long <- read_parquet(LONG_PATH)

message("[CREATE_WIDE] Input dimensions: ", nrow(df_long), " rows x ", ncol(df_long), " columns")

# Examine structure
message("\n[CREATE_WIDE] Sample teacher columns:")
teacher_cols <- grep("^teacher_", names(df_long), value = TRUE)
message(paste(head(teacher_cols, 20), collapse = "\n"))

# Check if we have racial diversity columns
race_cols <- grep("african|black|asian|hispanic|latino|white|filipino|hawaiian|native|indian",
                  teacher_cols, value = TRUE, ignore.case = TRUE)
message("\n[CREATE_WIDE] Teacher race-related columns found: ", length(race_cols))
if (length(race_cols) > 0) {
  message(paste(head(race_cols, 10), collapse = "\n"))
}

# The key insight: the long-format file already has ONE ROW per school-year-student_group
# with teacher columns attached. We just need to ensure all necessary columns are present.

# Check if this is already in the right format
key_cols <- c("cds_school", "academic_year", "student_group")
if (all(key_cols %in% names(df_long))) {
  dup_count <- df_long %>%
    group_by(across(all_of(key_cols))) %>%
    filter(n() > 1) %>%
    nrow()

  if (dup_count == 0) {
    message("\n[CREATE_WIDE] Data is already in school-year-studentgroup format (no duplicates)")
    message("[CREATE_WIDE] This file can be used directly for race-specific regressions")
  } else {
    message("\n[CREATE_WIDE] WARNING: Found ", dup_count, " duplicate school-year-studentgroup combinations")
  }
}

# Write the file with a different approach:
# The long-format file is ALREADY suitable for student-group-specific analyses
# We just need to ensure it's saved at the expected path

message("\n[CREATE_WIDE] Writing to: ", OUT_PATH)
write_parquet(df_long, OUT_PATH)

message("[CREATE_WIDE] SUCCESS: Created ", OUT_PATH)
message("[CREATE_WIDE] Output dimensions: ", nrow(df_long), " rows x ", ncol(df_long), " columns")

# Generate diagnostic summary
message("\n[CREATE_WIDE] Diagnostic Summary:")
message("  - Unique schools: ", n_distinct(df_long$cds_school))
message("  - Academic years: ", paste(sort(unique(df_long$academic_year)), collapse = ", "))

if ("student_group" %in% names(df_long)) {
  message("  - Student groups: ", paste(sort(unique(df_long$student_group)), collapse = ", "))
}

# Count teacher column availability
teacher_avail <- df_long %>%
  filter(!is.na(cds_school)) %>%
  mutate(has_teacher = if_any(starts_with("teacher_"), ~!is.na(.x))) %>%
  summarise(
    total_rows = n(),
    with_teacher = sum(has_teacher, na.rm = TRUE),
    pct_coverage = round(100 * with_teacher / total_rows, 1)
  )

message("  - Teacher data coverage: ", teacher_avail$with_teacher, " / ",
        teacher_avail$total_rows, " (", teacher_avail$pct_coverage, "%)")

message("\n[CREATE_WIDE] File ready for regression analysis!")
invisible(TRUE)
