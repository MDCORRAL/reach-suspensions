# Quick diagnostic to check data structure before power analysis
# Run this first to see if data is suitable for power analysis

library(here)
library(arrow)
library(dplyr)

message("Loading data...")
df <- read_parquet(here("data-stage", "susp_v6_teacher_features.parquet"))

message("\n=== DATA STRUCTURE ===")
message("Total rows: ", format(nrow(df), big.mark = ","))
message("Total columns: ", ncol(df))

message("\n=== KEY COLUMNS ===")
message("Has student_group: ", "student_group" %in% names(df))
message("Has total_suspensions: ", "total_suspensions" %in% names(df))
message("Has cumulative_enrollment: ", "cumulative_enrollment" %in% names(df))

message("\n=== STUDENT GROUPS ===")
if ("student_group" %in% names(df)) {
  group_counts <- df %>% count(student_group, sort = TRUE)
  print(group_counts)
} else {
  message("No student_group column found!")
}

message("\n=== TEACHER COLUMNS ===")
teacher_cols <- grep("^teacher_", names(df), value = TRUE)
message("Total teacher columns: ", length(teacher_cols))

teacher_share_cols <- grep("_share$", teacher_cols, value = TRUE)
message("Teacher share columns: ", length(teacher_share_cols))

message("\nFirst 20 teacher share columns:")
print(head(teacher_share_cols, 20))

message("\n=== SAMPLE AGGREGATION TEST ===")
message("Testing aggregation on small subset (1000 rows)...")

test_agg <- df %>%
  head(1000) %>%
  group_by(cds_school, academic_year, student_group) %>%
  summarise(
    total_suspensions = sum(total_suspensions, na.rm = TRUE),
    cumulative_enrollment = first(cumulative_enrollment),
    n = n(),
    .groups = "drop"
  )

message("Test aggregation successful!")
message("  Input rows: 1,000")
message("  Output rows: ", nrow(test_agg))
message("  Reduction factor: ", round(1000 / nrow(test_agg), 1), "x")

message("\n=== EXPECTED FULL AGGREGATION ===")
message("If pattern holds, full data would aggregate to approximately:")
message("  ", format(round(nrow(df) / (1000 / nrow(test_agg))), big.mark = ","), " rows")

message("\n=== RECOMMENDATION ===")
reduction_factor <- 1000 / nrow(test_agg)
expected_final <- nrow(df) / reduction_factor

if (expected_final < 100000) {
  message("✓ Data should aggregate efficiently (< 100k rows)")
  message("  You can proceed with the full power analysis script.")
} else if (expected_final < 500000) {
  message("⚠ Data will take some time to aggregate (",
          format(round(expected_final), big.mark = ","), " expected rows)")
  message("  Be patient - aggregation may take 1-2 minutes.")
} else {
  message("⚠ Data aggregation may be slow (",
          format(round(expected_final), big.mark = ","), " expected rows)")
  message("  Consider filtering to recent years only (e.g., 2018-19 onwards)")
}
