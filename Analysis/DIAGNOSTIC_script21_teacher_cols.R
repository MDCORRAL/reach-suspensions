# Quick diagnostic: Does script 21 have the same teacher share issue?

suppressPackageStartupMessages({
  library(dplyr)
  library(arrow)
  library(here)
  library(janitor)
})

try(here::i_am("Analysis/DIAGNOSTIC_script21_teacher_cols.R"), silent = TRUE)

source(here("R", "utils_keys_filters.R"))
source(here("R", "teacher_processing.R"))

message("=== DIAGNOSTIC: Script 21 Teacher Columns ===\n")

# Replicate what script 21 does
TEACHER_PATH <- here("data-stage", "teacher_staff_long.parquet")

if (!file.exists(TEACHER_PATH)) {
  stop("Missing teacher data")
}

message("Step 1: Load and summarize teacher data (like script 21 does)")
message("=============================================================\n")

teacher_long <- read_parquet(TEACHER_PATH) %>%
  clean_names() %>%
  build_keys()

message("Raw teacher data columns:")
message("  ", paste(names(teacher_long)[1:10], collapse = ", "), "...")

teacher_summary <- teacher_summarise_long(teacher_long)

message("\n>>> After teacher_summarise_long():")
message("  Total columns: ", ncol(teacher_summary))

# Check for the columns script 21 would find
teacher_race_cols <- grep(
  "^teacher_staff_count_(african|american_indian|asian|filipino|hispanic|pacific|white|two_or_more|not_reported)",
  names(teacher_summary),
  value = TRUE,
  perl = TRUE
)

# Exclude _share columns
teacher_race_cols <- grep("_share$", teacher_race_cols, value = TRUE, invert = TRUE, perl = TRUE)

message("\nColumns script 21 would use:")
message("  Found ", length(teacher_race_cols), " columns")
if (length(teacher_race_cols) > 0) {
  for (col in teacher_race_cols[1:min(10, length(teacher_race_cols))]) {
    message("    ", col)
  }
}

# Check if these are counts or shares
message("\nStep 2: Test with one school")
message("============================\n")

# Get one school
sample_school <- teacher_summary %>% slice(1)

if (nrow(sample_school) > 0) {
  message("Sample school: ", sample_school$cds_school, " (", sample_school$academic_year, ")")

  # Find total
  total_col <- "teacher_staff_count_total"
  if (total_col %in% names(sample_school)) {
    total_val <- sample_school[[total_col]]
    message("  Total staff: ", total_val)

    # Sum the race columns
    if (length(teacher_race_cols) > 0) {
      race_sum <- 0
      for (col in teacher_race_cols) {
        if (col %in% names(sample_school)) {
          val <- sample_school[[col]]
          race_sum <- race_sum + val
        }
      }

      message("  Sum of race columns: ", race_sum)
      message("  Ratio (sum/total): ", round(race_sum / total_val, 2))

      if (race_sum > total_val * 1.5) {
        message("\n  ⚠️  WARNING: Sum > total! These might be percentages!")
      } else {
        message("\n  ✓ Looks OK - these appear to be raw counts")
      }
    }
  }
}

# Check what columns exist with "total" prefix
message("\nStep 3: Check for 'teacher_total_' prefix columns")
message("===================================================\n")

total_prefix_cols <- grep("^teacher_total_", names(teacher_summary), value = TRUE)
message("Columns starting with 'teacher_total_': ", length(total_prefix_cols))
if (length(total_prefix_cols) > 0) {
  for (col in total_prefix_cols[1:min(10, length(total_prefix_cols))]) {
    message("  ", col)
  }
}

message("\n=== DIAGNOSTIC COMPLETE ===")
message("\nConclusion:")
message("If script 21 uses columns from teacher_summarise_long() directly,")
message("and those columns contain raw counts (not percentages), then")
message("script 21 is probably OK.")
message("\nBut if script 21 loads from susp_v6_teacher_features.parquet,")
message("it might have the same issue as script 23.")
