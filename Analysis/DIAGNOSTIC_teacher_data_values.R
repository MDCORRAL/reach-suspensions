# DIAGNOSTIC: Check what values are actually in teacher columns
# Purpose: Investigate why graphs show percentages far beyond 100%

suppressPackageStartupMessages({
  library(dplyr)
  library(arrow)
  library(here)
  library(janitor)
})

try(here::i_am("Analysis/DIAGNOSTIC_teacher_data_values.R"), silent = TRUE)

source(here("R", "utils_keys_filters.R"))
source(here("R", "teacher_processing.R"))
source(here("R", "00_paths.R"))

message("=== DIAGNOSTIC: Teacher Data Values ===\n")

# Load the same way script 23 does
V6_LONG_PATH <- here("data-stage", "susp_v6_long.parquet")
TEACHER_PATH <- here("data-stage", "teacher_staff_long.parquet")
FEATURES_PATH <- here("data-stage", "susp_v6_features.parquet")

# Load student data
df_students_raw <- read_parquet(V6_LONG_PATH) %>%
  clean_names() %>%
  build_keys() %>%
  filter(
    aggregate_level == "S" | tolower(aggregate_level) == "school",
    !school_code %in% SPECIAL_SCHOOL_CODES
  )

df_students <- df_students_raw %>%
  filter(
    category_type == "Race/Ethnicity",
    canon_race_label(subgroup) == "All Students"
  ) %>%
  distinct(cds_school, academic_year, .keep_all = TRUE)

# Load and summarize teacher data
teacher_long <- read_parquet(TEACHER_PATH) %>%
  clean_names() %>%
  build_keys()

teacher_summary <- teacher_summarise_long(teacher_long)

# Join
df <- df_students %>%
  left_join(teacher_summary, by = c("academic_year", "cds_school"), relationship = "one-to-one")

# Load features
features <- read_parquet(FEATURES_PATH) %>%
  clean_names() %>%
  build_keys() %>%
  select(cds_school, academic_year, is_traditional, black_share, white_share, hispanic_share)

df <- df %>%
  left_join(features, by = c("cds_school", "academic_year"))

# Filter to Q4 traditional schools (same as script 23)
df_q4 <- df %>%
  filter(
    is_traditional == TRUE,
    !is.na(black_prop_q),
    black_prop_q == 4
  )

message("Total Q4 school-years: ", nrow(df_q4))
message("Schools with teacher data: ", sum(!is.na(df_q4$teacher_staff_count_total)))
message("\n=== CHECKING COLUMN VALUES ===\n")

# Check a few specific schools to see what values are in the columns
sample_schools <- df_q4 %>%
  filter(!is.na(teacher_staff_count_total)) %>%
  slice(1:5) %>%
  select(
    cds_school, academic_year, school_name,
    # Totals
    teacher_staff_count_total,
    teacher_staff_count_total_by_type_teachers,
    teacher_staff_count_total_by_type_administrators,
    # Race counts (should be integers or small numbers)
    teacher_staff_count_african_american,
    teacher_staff_count_white,
    # Race counts by type (should be integers or small numbers)
    teacher_staff_count_by_type_teachers_african_american,
    teacher_staff_count_by_type_teachers_white,
    teacher_staff_count_by_type_administrators_african_american,
    teacher_staff_count_by_type_administrators_white,
    # Race shares (should be 0-1)
    teacher_staff_count_african_american_share,
    teacher_staff_count_white_share,
    # Race shares by type (should be 0-1)
    teacher_staff_count_by_type_teachers_african_american_share,
    teacher_staff_count_by_type_teachers_white_share,
    teacher_staff_count_by_type_administrators_african_american_share,
    teacher_staff_count_by_type_administrators_white_share
  )

print(sample_schools)

message("\n=== VALUE RANGES ===\n")

# Check ranges of key columns
check_cols <- c(
  "teacher_staff_count_total",
  "teacher_staff_count_total_by_type_teachers",
  "teacher_staff_count_total_by_type_administrators",
  "teacher_staff_count_african_american",
  "teacher_staff_count_white",
  "teacher_staff_count_by_type_teachers_african_american",
  "teacher_staff_count_by_type_teachers_white",
  "teacher_staff_count_by_type_administrators_african_american",
  "teacher_staff_count_by_type_administrators_white"
)

for (col in check_cols) {
  if (col %in% names(df_q4)) {
    vals <- df_q4[[col]][!is.na(df_q4[[col]])]
    if (length(vals) > 0) {
      message(sprintf("%-60s Min: %8.2f  Max: %8.2f  Mean: %8.2f",
                      col, min(vals), max(vals), mean(vals)))
    }
  }
}

message("\n=== SHARE RANGES (should be 0-1) ===\n")

share_cols <- c(
  "teacher_staff_count_african_american_share",
  "teacher_staff_count_white_share",
  "teacher_staff_count_by_type_teachers_african_american_share",
  "teacher_staff_count_by_type_teachers_white_share",
  "teacher_staff_count_by_type_administrators_african_american_share",
  "teacher_staff_count_by_type_administrators_white_share"
)

for (col in share_cols) {
  if (col %in% names(df_q4)) {
    vals <- df_q4[[col]][!is.na(df_q4[[col]])]
    if (length(vals) > 0) {
      message(sprintf("%-70s Min: %6.4f  Max: %6.4f  Mean: %6.4f",
                      col, min(vals), max(vals), mean(vals)))
      # Flag values > 1
      if (max(vals) > 1) {
        message("  *** WARNING: Values exceed 1.0! (", sum(vals > 1), " schools)")
      }
    }
  }
}

message("\n=== AGGREGATION TEST (mimic script 23 by_level_stats) ===\n")

# Replicate the exact calculation from script 23
by_level_test <- df_q4 %>%
  filter(!is.na(teacher_staff_count_total), !is.na(school_level)) %>%
  group_by(school_level) %>%
  summarise(
    n_schools = n(),
    total_teachers = sum(teacher_staff_count_total_by_type_teachers, na.rm = TRUE),
    total_administrators = sum(teacher_staff_count_total_by_type_administrators, na.rm = TRUE),

    # Sum the counts
    teachers_african_american = sum(teacher_staff_count_by_type_teachers_african_american, na.rm = TRUE),
    teachers_white = sum(teacher_staff_count_by_type_teachers_white, na.rm = TRUE),
    admins_african_american = sum(teacher_staff_count_by_type_administrators_african_american, na.rm = TRUE),
    admins_white = sum(teacher_staff_count_by_type_administrators_white, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    # Calculate shares
    teachers_african_american_share = teachers_african_american / total_teachers,
    teachers_white_share = teachers_white / total_teachers,
    admins_african_american_share = admins_african_american / total_administrators,
    admins_white_share = admins_white / total_administrators
  )

print(by_level_test)

message("\n=== PROBLEMATIC SHARES ===")
if (any(by_level_test$admins_white_share > 1, na.rm = TRUE)) {
  message("*** FOUND THE PROBLEM! ***")
  message("White admin shares > 1.0:")
  print(by_level_test %>%
          filter(admins_white_share > 1) %>%
          select(school_level, total_administrators, admins_white, admins_white_share))
}

message("\n=== CHECKING RAW TEACHER_LONG DATA ===\n")
# Check the raw teacher data before summarization
teacher_long_sample <- teacher_long %>%
  filter(academic_year == "2023-24") %>%
  slice(1:10)

message("Sample of raw teacher_long data:")
print(teacher_long_sample)

message("\nColumn names in teacher_long:")
print(names(teacher_long))

message("\n=== Done ===")
