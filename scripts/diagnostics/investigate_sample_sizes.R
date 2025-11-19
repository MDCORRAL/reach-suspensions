# scripts/diagnostics/investigate_sample_sizes.R
# Investigate sample sizes in teacher diversity regression

suppressPackageStartupMessages({
  library(dplyr)
  library(arrow)
  library(here)
})

TEACHER_FEATURES_PATH <- here("data-stage", "susp_v6_teacher_features.parquet")

cat(strrep("=", 70), "\n")
cat("INVESTIGATING REGRESSION SAMPLE SIZES\n")
cat(strrep("=", 70), "\n\n")

# Load data
cat("Loading data:", basename(TEACHER_FEATURES_PATH), "\n")
df <- read_parquet(TEACHER_FEATURES_PATH)

cat("\n1. OVERALL DATA STRUCTURE\n")
cat(strrep("-", 70), "\n")
cat("Total rows:", format(nrow(df), big.mark = ","), "\n")
cat("Total columns:", ncol(df), "\n\n")

# Check years
cat("2. ACADEMIC YEARS INCLUDED\n")
cat(strrep("-", 70), "\n")
years <- sort(unique(df$academic_year))
cat("Years:", paste(years, collapse = ", "), "\n")
cat("Number of years:", length(years), "\n\n")

# Check what each row represents
cat("3. WHAT EACH ROW REPRESENTS\n")
cat(strrep("-", 70), "\n")
cat("Sample of first 10 rows (key columns):\n")
print(head(df %>% select(academic_year, cds_school, student_group, cumulative_enrollment), 10))
cat("\n")

# Check student groups
cat("4. OBSERVATIONS BY STUDENT GROUP (BEFORE FILTERING)\n")
cat(strrep("-", 70), "\n")
group_counts <- df %>%
  count(student_group, sort = TRUE, name = "total_rows")
print(group_counts)
cat("\n")

# Now simulate the regression filtering to see what happens
cat("5. SIMULATING REGRESSION FILTERS\n")
cat("-" %% 70, "\n\n")

# Define student groups used in regression
ALLOWED_RACE_GROUPS <- c(
  "Black/African American", "White", "Hispanic/Latino",
  "American Indian/Alaska Native", "Asian", "Filipino",
  "Native Hawaiian/Pacific Islander", "Two or More Races"
)

for (group_name in ALLOWED_RACE_GROUPS) {
  cat("Student Group:", group_name, "\n")

  # Filter to this group
  group_df <- df %>% filter(student_group == group_name, !is.na(student_group))

  cat("  - Initial rows for", group_name, ":", format(nrow(group_df), big.mark = ","), "\n")

  # Count by year
  cat("  - Breakdown by year:\n")
  year_counts <- group_df %>%
    count(academic_year, name = "n_rows") %>%
    arrange(academic_year)
  for (i in 1:nrow(year_counts)) {
    cat(sprintf("      %s: %s rows\n",
                year_counts$academic_year[i],
                format(year_counts$n_rows[i], big.mark = ",")))
  }

  # Now apply regression filters
  outcome_col <- "suspension_rate_percent_total"

  # Check for teacher race share columns
  race_share_pattern <- "teacher.*_(african_american|asian|hispanic|white|filipino|american_indian|native_hawaiian|pacific_islander|two_or_more).*_share$"
  race_share_cols <- grep(race_share_pattern, names(group_df), value = TRUE, ignore.case = TRUE)

  # Identify non-white columns
  white_cols <- grep("_white_share$", race_share_cols, value = TRUE, ignore.case = TRUE)
  white_cols <- white_cols[!grepl("non_white", white_cols, ignore.case = TRUE)]
  not_reported_cols <- grep("_(not_reported|unknown)_share$", race_share_cols, value = TRUE, ignore.case = TRUE)
  non_white_cols <- setdiff(race_share_cols, c(white_cols, not_reported_cols))

  # Compute teacher non-white share
  if (length(non_white_cols) > 0) {
    mat <- sapply(non_white_cols, function(col) as.numeric(group_df[[col]]))
    if (!is.matrix(mat)) mat <- matrix(mat, ncol = 1)
    teacher_non_white_share <- rowSums(mat, na.rm = TRUE)
    all_missing <- apply(is.na(mat), 1, all)
    teacher_non_white_share[all_missing] <- NA_real_
  } else {
    teacher_non_white_share <- rep(NA_real_, nrow(group_df))
  }

  # Compute admin non-white share (same logic but for administrators)
  admin_race_share_pattern <- "teacher.*by_type_administrators.*_(african_american|asian|hispanic|white|filipino|american_indian|native_hawaiian|pacific_islander|two_or_more).*_share$"
  admin_race_share_cols <- grep(admin_race_share_pattern, names(group_df), value = TRUE, ignore.case = TRUE)

  admin_white_cols <- grep("_white_share$", admin_race_share_cols, value = TRUE, ignore.case = TRUE)
  admin_white_cols <- admin_white_cols[!grepl("non_white", admin_white_cols, ignore.case = TRUE)]
  admin_not_reported_cols <- grep("_(not_reported|unknown)_share$", admin_race_share_cols, value = TRUE, ignore.case = TRUE)
  admin_non_white_cols <- setdiff(admin_race_share_cols, c(admin_white_cols, admin_not_reported_cols))

  if (length(admin_non_white_cols) > 0) {
    admin_mat <- sapply(admin_non_white_cols, function(col) as.numeric(group_df[[col]]))
    if (!is.matrix(admin_mat)) admin_mat <- matrix(admin_mat, ncol = 1)
    admin_non_white_share <- rowSums(admin_mat, na.rm = TRUE)
    admin_all_missing <- apply(is.na(admin_mat), 1, all)
    admin_non_white_share[admin_all_missing] <- NA_real_
  } else {
    admin_non_white_share <- rep(NA_real_, nrow(group_df))
  }

  # Get suspension rate and enrollment
  suspension_rate <- suppressWarnings(as.numeric(group_df[[outcome_col]]) / 100)
  enrollment <- suppressWarnings(as.numeric(group_df$cumulative_enrollment))

  # Apply filters
  keep <- !is.na(suspension_rate) &
    !is.na(teacher_non_white_share) &
    !is.na(admin_non_white_share) &
    !is.na(enrollment) &
    enrollment > 0

  filtered_df <- group_df[keep, ]

  cat(sprintf("  - After complete case filtering: %s rows\n",
              format(sum(keep), big.mark = ",")))

  # Check how many unique schools
  unique_schools <- n_distinct(filtered_df$cds_school)
  cat(sprintf("  - Unique schools: %s\n", format(unique_schools, big.mark = ",")))

  # Check school-years
  unique_school_years <- filtered_df %>%
    distinct(cds_school, academic_year) %>%
    nrow()
  cat(sprintf("  - Unique school-year combinations: %s\n",
              format(unique_school_years, big.mark = ",")))

  # This should equal the regression sample size
  cat(sprintf("  - FINAL SAMPLE SIZE (= regression N): %s\n\n",
              format(nrow(filtered_df), big.mark = ",")))
}

cat("\n6. KEY INSIGHT: WHY SAMPLE SIZES DIFFER BY RACE\n")
cat(strrep("-", 70), "\n")
cat("The sample size for each racial group represents the number of\n")
cat("school-year-race observations with complete data, which includes:\n\n")
cat("  1. A school has students from that racial group in that year\n")
cat("  2. Non-missing suspension rate for that group\n")
cat("  3. Non-missing teacher racial diversity data\n")
cat("  4. Non-missing administrator racial diversity data\n")
cat("  5. Positive enrollment for that group\n\n")
cat("Different racial groups have different sample sizes because:\n\n")
cat("  - White and Hispanic/Latino students are present in more schools\n")
cat("    (larger geographic distribution)\n")
cat("  - Smaller groups like American Indian/Alaska Native or\n")
cat("    Native Hawaiian/Pacific Islander are only present in\n")
cat("    specific schools\n")
cat("  - Each observation is a SCHOOL-YEAR-RACE combination,\n")
cat("    NOT a unique school\n\n")
cat(strrep("=", 70), "\n")
