# Analysis/DIAGNOSTIC_q4_suspension_data.R
# Diagnostic script to investigate missing suspension data in Q4 Black enrollment schools

suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(arrow)
  library(here)
  library(janitor)
  library(readr)
})

try(here::i_am("Analysis/DIAGNOSTIC_q4_suspension_data.R"), silent = TRUE)

source(here("R", "utils_keys_filters.R"))
source(here("R", "00_paths.R"))

message("=== DIAGNOSTIC: Q4 Suspension Data Investigation ===\n")

# ============================================================================
# STEP 1: Check original v6_long data
# ============================================================================

message("STEP 1: Checking susp_v6_long.parquet...")
v6_long <- read_parquet(file.path(dp_stage, "susp_v6_long.parquet")) %>%
  clean_names() %>%
  build_keys()

message("  Total rows: ", format(nrow(v6_long), big.mark = ","))
message("  Unique schools: ", n_distinct(v6_long$cds_school))

# Check for Q4 schools
q4_v6 <- v6_long %>%
  filter(!is.na(black_prop_q), black_prop_q == 4)

message("\n  Q4 schools in v6_long:")
message("    Total rows: ", format(nrow(q4_v6), big.mark = ","))
message("    Unique schools: ", n_distinct(q4_v6$cds_school))

# Check suspension columns
susp_cols <- names(v6_long)[grepl("suspension|susp", names(v6_long), ignore.case = TRUE)]
message("\n  Suspension columns: ", paste(susp_cols, collapse = ", "))

# Check data availability
message("\n  Suspension data availability in Q4 schools:")
for (col in susp_cols) {
  if (col %in% names(q4_v6)) {
    non_na <- sum(!is.na(q4_v6[[col]]))
    pct <- round(100 * non_na / nrow(q4_v6), 1)
    message(sprintf("    %-40s: %8d / %8d (%5.1f%%)", col, non_na, nrow(q4_v6), pct))
  }
}

# Sample Q4 schools with missing suspension data
message("\n  Sample Q4 schools with missing suspension data:")
missing_susp_v6 <- q4_v6 %>%
  filter(is.na(total_suspensions)) %>%
  select(academic_year, cds_school, school_name, reporting_category,
         cumulative_enrollment, black_share, black_prop_q,
         total_suspensions, suspension_rate_percent_total) %>%
  head(10)

print(missing_susp_v6)

# ============================================================================
# STEP 2: Check v6_features data
# ============================================================================

message("\n\nSTEP 2: Checking susp_v6_features.parquet...")
v6_features <- read_parquet(file.path(dp_stage, "susp_v6_features.parquet")) %>%
  clean_names() %>%
  build_keys()

message("  Total rows: ", format(nrow(v6_features), big.mark = ","))
message("  Unique schools: ", n_distinct(v6_features$cds_school))

# Check for is_traditional flag
message("  is_traditional coverage: ", sum(!is.na(v6_features$is_traditional)), " / ", nrow(v6_features))

q4_features <- v6_features %>%
  filter(!is.na(black_prop_q), black_prop_q == 4)

message("\n  Q4 schools in v6_features:")
message("    Total rows: ", format(nrow(q4_features), big.mark = ","))
message("    Unique schools: ", n_distinct(q4_features$cds_school))

# Check suspension data in features
susp_cols_feat <- names(v6_features)[grepl("suspension|susp", names(v6_features), ignore.case = TRUE)]
message("\n  Suspension columns in features: ", paste(susp_cols_feat, collapse = ", "))

message("\n  Suspension data availability in Q4 features:")
for (col in susp_cols_feat) {
  if (col %in% names(q4_features)) {
    non_na <- sum(!is.na(q4_features[[col]]))
    pct <- round(100 * non_na / nrow(q4_features), 1)
    message(sprintf("    %-40s: %8d / %8d (%5.1f%%)", col, non_na, nrow(q4_features), pct))
  }
}

# ============================================================================
# STEP 3: Check merged teacher-features data
# ============================================================================

message("\n\nSTEP 3: Checking susp_v6_teacher_features.parquet...")
teacher_features <- read_parquet(file.path(dp_stage, "susp_v6_teacher_features.parquet")) %>%
  clean_names() %>%
  build_keys()

message("  Total rows: ", format(nrow(teacher_features), big.mark = ","))
message("  Unique schools: ", n_distinct(teacher_features$cds_school))

q4_teacher <- teacher_features %>%
  filter(!is.na(black_prop_q), black_prop_q == 4)

message("\n  Q4 schools in teacher_features:")
message("    Total rows: ", format(nrow(q4_teacher), big.mark = ","))
message("    Unique schools: ", n_distinct(q4_teacher$cds_school))

# Check suspension data
susp_cols_teacher <- names(teacher_features)[grepl("suspension|susp", names(teacher_features), ignore.case = TRUE)]
message("\n  Suspension columns in teacher_features: ", paste(susp_cols_teacher, collapse = ", "))

message("\n  Suspension data availability in Q4 teacher_features:")
for (col in susp_cols_teacher) {
  if (col %in% names(q4_teacher)) {
    non_na <- sum(!is.na(q4_teacher[[col]]))
    pct <- round(100 * non_na / nrow(q4_teacher), 1)
    message(sprintf("    %-40s: %8d / %8d (%5.1f%%)", col, non_na, nrow(q4_teacher), pct))
  }
}

# Sample Q4 schools with missing suspension data
message("\n  Sample Q4 schools with missing suspension data in teacher_features:")
missing_susp_teacher <- q4_teacher %>%
  filter(is.na(total_suspensions)) %>%
  select(academic_year, cds_school, school_name, reporting_category,
         cumulative_enrollment, black_share, black_prop_q,
         total_suspensions, suspension_rate_percent_total) %>%
  head(10)

print(missing_susp_teacher)

# ============================================================================
# STEP 4: Check what happens after is_traditional filter
# ============================================================================

message("\n\nSTEP 4: Checking traditional schools filter impact...")

# Load features to get is_traditional
features_for_join <- read_parquet(file.path(dp_stage, "susp_v6_features.parquet")) %>%
  clean_names() %>%
  build_keys() %>%
  select(cds_school, academic_year, is_traditional, black_share, white_share, hispanic_share)

# Join to teacher_features (mimicking script 23)
teacher_with_trad <- teacher_features %>%
  left_join(features_for_join, by = c("cds_school", "academic_year"))

# Filter like script 23
q4_traditional <- teacher_with_trad %>%
  filter(
    is_traditional == TRUE,
    !is.na(black_prop_q),
    black_prop_q == 4
  ) %>%
  distinct(academic_year, cds_school, .keep_all = TRUE)

message("  Q4 traditional schools (one row per school-year):")
message("    Total rows: ", format(nrow(q4_traditional), big.mark = ","))
message("    Unique schools: ", n_distinct(q4_traditional$cds_school))

# Check suspension data after all filters
message("\n  Suspension data after traditional filter:")
message("    With total_suspensions: ", sum(!is.na(q4_traditional$total_suspensions)))
message("    With suspension_rate_percent_total: ", sum(!is.na(q4_traditional$suspension_rate_percent_total)))
message("    Missing both: ", sum(is.na(q4_traditional$total_suspensions) & is.na(q4_traditional$suspension_rate_percent_total)))

# ============================================================================
# STEP 5: Detailed analysis of missing data patterns
# ============================================================================

message("\n\nSTEP 5: Analyzing patterns in missing suspension data...")

# By year
by_year <- q4_traditional %>%
  group_by(academic_year) %>%
  summarise(
    total_schools = n(),
    with_susp_data = sum(!is.na(total_suspensions)),
    pct_with_data = round(100 * with_susp_data / total_schools, 1)
  )

message("\n  By academic year:")
print(by_year)

# By school level
if ("school_level" %in% names(q4_traditional)) {
  by_level <- q4_traditional %>%
    filter(!is.na(school_level)) %>%
    group_by(school_level) %>%
    summarise(
      total_schools = n(),
      with_susp_data = sum(!is.na(total_suspensions)),
      pct_with_data = round(100 * with_susp_data / total_schools, 1)
    )

  message("\n  By school level:")
  print(by_level)
}

# By enrollment size
enrollment_analysis <- q4_traditional %>%
  mutate(
    enrollment_category = case_when(
      is.na(cumulative_enrollment) ~ "Missing enrollment",
      cumulative_enrollment < 100 ~ "< 100",
      cumulative_enrollment < 500 ~ "100-499",
      cumulative_enrollment < 1000 ~ "500-999",
      TRUE ~ "1000+"
    )
  ) %>%
  group_by(enrollment_category) %>%
  summarise(
    total_schools = n(),
    with_susp_data = sum(!is.na(total_suspensions)),
    pct_with_data = round(100 * with_susp_data / total_schools, 1)
  )

message("\n  By enrollment size:")
print(enrollment_analysis)

# ============================================================================
# STEP 6: Check if it's a race-specific issue
# ============================================================================

message("\n\nSTEP 6: Checking if it's a race/reporting_category issue...")

# Check if certain reporting categories have more missing data
if ("reporting_category" %in% names(teacher_features)) {
  by_race <- q4_teacher %>%
    filter(!is.na(reporting_category)) %>%
    group_by(reporting_category) %>%
    summarise(
      total_rows = n(),
      with_susp_data = sum(!is.na(total_suspensions)),
      pct_with_data = round(100 * with_susp_data / total_rows, 1)
    )

  message("\n  By reporting category:")
  print(by_race)
}

# ============================================================================
# STEP 7: Write diagnostic outputs
# ============================================================================

message("\n\nSTEP 7: Writing diagnostic outputs...")

# Export schools with missing data for manual review
missing_data_export <- q4_traditional %>%
  filter(is.na(total_suspensions)) %>%
  select(academic_year, cds_school, county_name, district_name, school_name,
         school_level, locale_simple, cumulative_enrollment, black_share, black_prop_q,
         total_suspensions, suspension_rate_percent_total,
         starts_with("teacher_staff_count_total")) %>%
  arrange(desc(cumulative_enrollment))

out_path <- file.path(dp_out, "tables", "DIAGNOSTIC_q4_missing_suspension_data.csv")
write_csv(missing_data_export, out_path)
message("  Wrote: ", out_path)
message("  Schools with missing suspension data: ", nrow(missing_data_export))

message("\n=== DIAGNOSTIC COMPLETE ===")
