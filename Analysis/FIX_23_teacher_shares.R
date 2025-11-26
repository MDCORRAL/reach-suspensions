# FIX for Analysis/23_teacher_demographics_q4_black_enrollment.R
# Purpose: Audit and fix teacher share calculations

suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(arrow)
  library(here)
  library(janitor)
  library(readr)
})

try(here::i_am("Analysis/FIX_23_teacher_shares.R"), silent = TRUE)

source(here("R", "utils_keys_filters.R"))
source(here("R", "00_paths.R"))

message("=== FIX: Auditing Teacher Share Calculations ===\n")

# Load data
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

message("Step 1: Check which teacher columns exist")
message("==========================================\n")

teacher_cols <- names(df)[grepl("^teacher_", names(df))]
message("Total teacher columns: ", length(teacher_cols))

# Check for count vs share columns
count_cols <- teacher_cols[!grepl("_share$", teacher_cols)]
share_cols <- teacher_cols[grepl("_share$", teacher_cols)]

message("  Count columns: ", length(count_cols))
message("  Share columns: ", length(share_cols))

# Specifically check for the columns script 23 is trying to use
cols_script23_needs <- c(
  "teacher_staff_count_total",
  "teacher_staff_count_total_by_type_teachers",
  "teacher_staff_count_total_by_type_administrators",
  "teacher_staff_count_by_type_teachers_african_american",
  "teacher_staff_count_by_type_teachers_white",
  "teacher_staff_count_by_type_teachers_hispanic_or_latino",
  "teacher_staff_count_by_type_teachers_asian",
  "teacher_staff_count_by_type_administrators_african_american",
  "teacher_staff_count_by_type_administrators_white",
  "teacher_staff_count_by_type_administrators_hispanic_or_latino",
  "teacher_staff_count_by_type_administrators_asian"
)

message("\nChecking if script 23's expected columns exist:")
for (col in cols_script23_needs) {
  exists <- col %in% names(df)
  message(sprintf("  %-70s: %s", col, ifelse(exists, "✓ EXISTS", "✗ MISSING")))

  if (!exists) {
    # Check if a similar column exists
    similar <- grep(gsub("_", ".*", col), names(df), value = TRUE)
    if (length(similar) > 0) {
      message(sprintf("    Similar columns found: %s", paste(similar[1:min(3, length(similar))], collapse = ", ")))
    }
  }
}

# Check what columns DO exist for teachers
message("\nTeacher-specific columns that DO exist:")
teacher_specific_cols <- grep("by_type_teachers", names(df), value = TRUE)
for (col in teacher_specific_cols[1:min(30, length(teacher_specific_cols))]) {
  # Check if it's count or share
  is_share <- grepl("_share$", col)
  type <- ifelse(is_share, "SHARE", "COUNT")
  message(sprintf("  [%5s] %s", type, col))
}

message("\n\nStep 2: Test the calculation for one year")
message("==========================================\n")

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

# Focus on 2019-20
year_sample <- school_summary %>%
  filter(academic_year == "2019-20", !is.na(teacher_staff_count_total))

message("Schools in 2019-20 Q4 sample: ", nrow(year_sample))

if (nrow(year_sample) > 0) {
  # Sample one school to inspect
  one_school <- year_sample %>% slice(1)

  message("\nSample school: ", one_school$school_name)
  message("  teacher_staff_count_total_by_type_teachers = ", one_school$teacher_staff_count_total_by_type_teachers)

  # Check if the race columns are counts or shares
  race_cols_to_check <- c(
    "teacher_staff_count_by_type_teachers_african_american",
    "teacher_staff_count_by_type_teachers_white",
    "teacher_staff_count_by_type_teachers_hispanic_or_latino",
    "teacher_staff_count_by_type_teachers_asian"
  )

  message("\n  Race breakdowns (should be COUNTS, not shares):")
  total_check <- 0
  for (col in race_cols_to_check) {
    if (col %in% names(one_school)) {
      val <- one_school[[col]]
      total_check <- total_check + val
      message(sprintf("    %-60s = %8.2f", col, val))
    } else {
      message(sprintf("    %-60s = MISSING", col))
    }
  }

  message(sprintf("\n  Sum of these 4 races: %.2f", total_check))
  message(sprintf("  Total teachers at this school: %.2f", one_school$teacher_staff_count_total_by_type_teachers))

  if (total_check > one_school$teacher_staff_count_total_by_type_teachers * 1.1) {
    message("  ⚠️  WARNING: Sum of races > total teachers! These might be shares (0-1) not counts!")
  } else if (total_check > 4) {
    message("  ✓ These appear to be counts (values > 1)")
  } else if (total_check <= 4 && total_check > 0) {
    message("  ⚠️  POSSIBLE PROBLEM: Values are all ≤ 1, might be shares!")
  }

  # Now check what happens when we aggregate
  message("\n\nAggregating across all ", nrow(year_sample), " schools in 2019-20:")

  agg <- year_sample %>%
    summarise(
      total_teachers = sum(teacher_staff_count_total_by_type_teachers, na.rm = TRUE),
      teachers_african_american = sum(teacher_staff_count_by_type_teachers_african_american, na.rm = TRUE),
      teachers_white = sum(teacher_staff_count_by_type_teachers_white, na.rm = TRUE),
      teachers_hispanic = sum(teacher_staff_count_by_type_teachers_hispanic_or_latino, na.rm = TRUE),
      teachers_asian = sum(teacher_staff_count_by_type_teachers_asian, na.rm = TRUE)
    )

  message(sprintf("  Total teachers: %s", format(agg$total_teachers, big.mark = ",")))
  message(sprintf("  African American: %s", format(agg$teachers_african_american, big.mark = ",")))
  message(sprintf("  White: %s", format(agg$teachers_white, big.mark = ",")))
  message(sprintf("  Hispanic/Latino: %s", format(agg$teachers_hispanic, big.mark = ",")))
  message(sprintf("  Asian: %s", format(agg$teachers_asian, big.mark = ",")))

  sum_races <- agg$teachers_african_american + agg$teachers_white + agg$teachers_hispanic + agg$teachers_asian
  message(sprintf("\n  Sum of 4 races: %s", format(sum_races, big.mark = ",")))
  message(sprintf("  Ratio to total: %.2f", sum_races / agg$total_teachers))

  if (sum_races > agg$total_teachers) {
    message("  ⚠️  CRITICAL ERROR: Sum of races > total! Data is corrupted or shares are being summed!")
  }

  # Calculate shares
  agg <- agg %>%
    mutate(
      african_american_pct = teachers_african_american / total_teachers * 100,
      white_pct = teachers_white / total_teachers * 100,
      hispanic_pct = teachers_hispanic / total_teachers * 100,
      asian_pct = teachers_asian / total_teachers * 100
    )

  message("\n  Calculated percentages:")
  message(sprintf("    African American: %.1f%%", agg$african_american_pct))
  message(sprintf("    White: %.1f%%", agg$white_pct))
  message(sprintf("    Hispanic/Latino: %.1f%%", agg$hispanic_pct))
  message(sprintf("    Asian: %.1f%%", agg$asian_pct))
  message(sprintf("    Sum: %.1f%%", agg$african_american_pct + agg$white_pct + agg$hispanic_pct + agg$asian_pct))

  if (agg$white_pct > 95) {
    message("\n  ⚠️  WHITE TEACHERS AT ", round(agg$white_pct, 1), "%! This matches the user's reported issue!")
    message("      This strongly suggests the count columns contain shares, not counts!")
  }
}

message("\n\n=== DIAGNOSIS COMPLETE ===")
message("\nNext steps:")
message("1. Check the output above to confirm if count columns actually contain shares")
message("2. If yes, fix script 23 to use the correct columns")
message("3. Or fix the teacher_processing.R or data merge to ensure counts are preserved")
