# Diagnostic script to investigate teacher share calculation issues
# Purpose: Check what's actually in the teacher data columns

suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(arrow)
  library(here)
  library(janitor)
})

try(here::i_am("Analysis/DIAGNOSTIC_teacher_shares.R"), silent = TRUE)

source(here("R", "utils_keys_filters.R"))
source(here("R", "00_paths.R"))

message("=== DIAGNOSTIC: Teacher Share Calculation ===\n")

# Load the merged teacher-features data
TEACHER_DATA_PATH <- here("data-stage", "susp_v6_teacher_features.parquet")
FEATURES_PATH <- here("data-stage", "susp_v6_features.parquet")

df <- read_parquet(TEACHER_DATA_PATH) %>%
  clean_names() %>%
  build_keys()

features <- read_parquet(FEATURES_PATH) %>%
  clean_names() %>%
  build_keys() %>%
  select(cds_school, academic_year, is_traditional, black_share, white_share, hispanic_share)

df <- df %>%
  left_join(features, by = c("cds_school", "academic_year"))

message("Step 1: Check column names")
message("=============================\n")

# Find all teacher-related columns
teacher_cols <- names(df)[grepl("^teacher_", names(df))]
message("Found ", length(teacher_cols), " teacher-related columns\n")

# Show columns related to counts
count_cols <- teacher_cols[grepl("count", teacher_cols)]
message("Count-related columns (", length(count_cols), "):")
for (col in count_cols[1:min(20, length(count_cols))]) {
  message("  ", col)
}
if (length(count_cols) > 20) message("  ... and ", length(count_cols) - 20, " more")

# Show columns related to shares
share_cols <- teacher_cols[grepl("share", teacher_cols)]
message("\nShare-related columns (", length(share_cols), "):")
for (col in share_cols[1:min(20, length(share_cols))]) {
  message("  ", col)
}
if (length(share_cols) > 20) message("  ... and ", length(share_cols) - 20, " more")

message("\n\nStep 2: Sample one school's data")
message("==================================\n")

# Pick a school with teacher data
sample_school <- df %>%
  filter(!is.na(teacher_staff_count_total)) %>%
  filter(!is.na(black_prop_q), black_prop_q == 4, is_traditional == TRUE) %>%
  filter(reporting_category == "All Students") %>%
  head(1)

if (nrow(sample_school) > 0) {
  message("School: ", sample_school$school_name)
  message("Academic Year: ", sample_school$academic_year)
  message("CDS: ", sample_school$cds_school)

  # Check key teacher columns
  cols_to_check <- c(
    "teacher_staff_count_total",
    "teacher_staff_count_total_by_type_teachers",
    "teacher_staff_count_total_by_type_administrators",
    "teacher_staff_count_by_type_teachers_african_american",
    "teacher_staff_count_by_type_teachers_white",
    "teacher_staff_count_by_type_teachers_hispanic_or_latino",
    "teacher_staff_count_by_type_teachers_asian",
    "teacher_staff_count_by_type_teachers_african_american_share",
    "teacher_staff_count_by_type_teachers_white_share",
    "teacher_staff_count_by_type_teachers_hispanic_or_latino_share",
    "teacher_staff_count_by_type_teachers_asian_share"
  )

  message("\nKey columns for this school:")
  for (col in cols_to_check) {
    if (col %in% names(sample_school)) {
      val <- sample_school[[col]]
      message(sprintf("  %-70s = %s", col, format(val, digits = 4)))
    } else {
      message(sprintf("  %-70s = MISSING", col))
    }
  }
}

message("\n\nStep 3: Aggregate like script 23 does (2019-20)")
message("================================================\n")

# Filter like script 23
reporting_col <- if ("reporting_category" %in% names(df)) {
  "reporting_category"
} else if ("student_group" %in% names(df)) {
  "student_group"
} else {
  NA_character_
}

school_summary <- df %>%
  filter(
    is_traditional == TRUE,
    !is.na(black_prop_q),
    black_prop_q == 4
  ) %>%
  {
    if (!is.na(reporting_col) && reporting_col %in% names(.)) {
      filter(., !!sym(reporting_col) %in% c("All Students", "TA", "Total"))
    } else {
      .
    }
  } %>%
  distinct(academic_year, cds_school, .keep_all = TRUE)

message("Schools in Q4 Black enrollment: ", nrow(school_summary))

# Now aggregate for 2019-20 like the script does
year_2019_20 <- school_summary %>%
  filter(academic_year == "2019-20", !is.na(teacher_staff_count_total))

if (nrow(year_2019_20) > 0) {
  message("\nSchools in 2019-20 with teacher data: ", nrow(year_2019_20))

  # Calculate totals
  agg <- year_2019_20 %>%
    summarise(
      # Count variables
      total_teachers = sum(teacher_staff_count_total_by_type_teachers, na.rm = TRUE),
      teachers_african_american = sum(teacher_staff_count_by_type_teachers_african_american, na.rm = TRUE),
      teachers_white = sum(teacher_staff_count_by_type_teachers_white, na.rm = TRUE),
      teachers_hispanic = sum(teacher_staff_count_by_type_teachers_hispanic_or_latino, na.rm = TRUE),
      teachers_asian = sum(teacher_staff_count_by_type_teachers_asian, na.rm = TRUE)
    )

  message("\n--- Aggregated COUNTS (summed across schools) ---")
  message(sprintf("  Total teachers: %s", format(agg$total_teachers, big.mark = ",")))
  message(sprintf("  African American: %s", format(agg$teachers_african_american, big.mark = ",")))
  message(sprintf("  White: %s", format(agg$teachers_white, big.mark = ",")))
  message(sprintf("  Hispanic/Latino: %s", format(agg$teachers_hispanic, big.mark = ",")))
  message(sprintf("  Asian: %s", format(agg$teachers_asian, big.mark = ",")))

  # Calculate shares
  agg <- agg %>%
    mutate(
      teachers_african_american_share = teachers_african_american / total_teachers,
      teachers_white_share = teachers_white / total_teachers,
      teachers_hispanic_share = teachers_hispanic / total_teachers,
      teachers_asian_share = teachers_asian / total_teachers
    )

  message("\n--- Calculated SHARES (count / total) ---")
  message(sprintf("  African American: %.1f%%", agg$teachers_african_american_share * 100))
  message(sprintf("  White: %.1f%%", agg$teachers_white_share * 100))
  message(sprintf("  Hispanic/Latino: %.1f%%", agg$teachers_hispanic_share * 100))
  message(sprintf("  Asian: %.1f%%", agg$teachers_asian_share * 100))

  # Check if these sum to reasonable value
  total_share <- agg$teachers_african_american_share +
    agg$teachers_white_share +
    agg$teachers_hispanic_share +
    agg$teachers_asian_share

  message(sprintf("\n  Sum of these 4 shares: %.1f%%", total_share * 100))
  if (total_share > 1.1 || total_share < 0.5) {
    message("  ⚠️  WARNING: Shares don't sum to ~100%! There's a problem!")
  } else {
    message("  ✓  Shares sum to reasonable value (other races account for remainder)")
  }

  # Check if the count columns might actually be shares
  message("\n\n--- Checking if 'count' columns might actually be shares ---")
  sample_values <- year_2019_20 %>%
    select(school_name,
           teacher_staff_count_by_type_teachers_white,
           teacher_staff_count_total_by_type_teachers) %>%
    head(10)

  print(sample_values)

  # Look for values between 0 and 1
  white_values <- year_2019_20$teacher_staff_count_by_type_teachers_white
  white_values <- white_values[!is.na(white_values)]

  if (length(white_values) > 0) {
    pct_between_0_and_1 <- mean(white_values >= 0 & white_values <= 1) * 100
    message(sprintf("\n  %.1f%% of 'white teacher count' values are between 0 and 1", pct_between_0_and_1))
    if (pct_between_0_and_1 > 50) {
      message("  ⚠️  LIKELY PROBLEM: These 'count' columns appear to be shares, not counts!")
    }
  }

} else {
  message("\nNo schools found in 2019-20")
}

message("\n\nStep 4: Check what other race categories exist")
message("==============================================\n")

# Look for all race-related teacher columns
all_race_cols <- grep("teacher_staff_count_by_type_teachers_[^_]+$",
                      names(df), value = TRUE, perl = TRUE)
all_race_cols <- setdiff(all_race_cols,
                        grep("share|total|gender", all_race_cols, value = TRUE))

message("Race categories in teacher data:")
for (col in all_race_cols) {
  race_name <- gsub("teacher_staff_count_by_type_teachers_", "", col)
  non_zero <- sum(!is.na(df[[col]]) & df[[col]] > 0, na.rm = TRUE)
  message(sprintf("  %-40s: %6d schools with data", race_name, non_zero))
}

message("\n=== DIAGNOSTIC COMPLETE ===")
