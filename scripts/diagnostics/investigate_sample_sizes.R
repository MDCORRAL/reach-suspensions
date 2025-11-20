# scripts/diagnostics/investigate_sample_sizes.R
# Investigate sample sizes in teacher diversity regression

suppressPackageStartupMessages({
  library(dplyr)
  library(arrow)
  library(here)
})

source(here("R", "utils_keys_filters.R"))

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

# Define race group codes (as they appear in the data)
# Based on R/utils_keys_filters.R:
RACE_GROUP_CODES <- list(
  RB = "Black/African American",
  RW = "White",
  RH = "Hispanic/Latino",
  RI = "American Indian/Alaska Native",
  RA = "Asian",
  RF = "Filipino",
  RP = "Native Hawaiian/Pacific Islander",
  RT = "Two or More Races"
)

cat("5. SIMULATING REGRESSION FILTERS\n")
cat(strrep("-", 70), "\n\n")

# Helper function to safely compute sum of shares
safe_sum_shares <- function(df, col_names) {
  if (length(col_names) == 0) {
    return(rep(NA_real_, nrow(df)))
  }

  # Create matrix of share values
  mat <- sapply(col_names, function(col) {
    if (col %in% names(df)) {
      suppressWarnings(as.numeric(df[[col]]))
    } else {
      rep(NA_real_, nrow(df))
    }
  })

  # Handle case where sapply doesn't return a matrix
  if (!is.matrix(mat)) {
    mat <- matrix(mat, ncol = length(col_names))
  }

  # Handle case where there are 0 rows (creates 0x0 or 0xN matrix)
  if (nrow(mat) == 0) {
    return(numeric(0))
  }

  # Sum across rows, handling NAs
  result <- rowSums(mat, na.rm = TRUE)

  # Set to NA if all columns were NA
  all_missing <- apply(is.na(mat), 1, all)
  result[all_missing] <- NA_real_

  return(result)
}

# Iterate through each race group
for (code in names(RACE_GROUP_CODES)) {
  group_name <- RACE_GROUP_CODES[[code]]
  cat("Student Group:", code, "(", group_name, ")\n")

  # Filter to this group
  group_df <- df %>% filter(student_group == code)

  cat(sprintf("  - Initial rows for %s: %s\n",
              code,
              format(nrow(group_df), big.mark = ",")))

  # If no rows, skip
  if (nrow(group_df) == 0) {
    cat("  - SKIPPING: No data for this group\n\n")
    next
  }

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
  teacher_non_white_share <- safe_sum_shares(group_df, non_white_cols)

  # Compute admin non-white share (same logic but for administrators)
  admin_race_share_pattern <- "teacher.*by_type_administrators.*_(african_american|asian|hispanic|white|filipino|american_indian|native_hawaiian|pacific_islander|two_or_more).*_share$"
  admin_race_share_cols <- grep(admin_race_share_pattern, names(group_df), value = TRUE, ignore.case = TRUE)

  admin_white_cols <- grep("_white_share$", admin_race_share_cols, value = TRUE, ignore.case = TRUE)
  admin_white_cols <- admin_white_cols[!grepl("non_white", admin_white_cols, ignore.case = TRUE)]
  admin_not_reported_cols <- grep("_(not_reported|unknown)_share$", admin_race_share_cols, value = TRUE, ignore.case = TRUE)
  admin_non_white_cols <- setdiff(admin_race_share_cols, c(admin_white_cols, admin_not_reported_cols))

  admin_non_white_share <- safe_sum_shares(group_df, admin_non_white_cols)

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

  # Check filtering breakdown
  cat("  - Filter breakdown:\n")
  cat(sprintf("      Missing suspension_rate: %s\n",
              format(sum(is.na(suspension_rate)), big.mark = ",")))
  cat(sprintf("      Missing teacher_non_white_share: %s\n",
              format(sum(is.na(teacher_non_white_share)), big.mark = ",")))
  cat(sprintf("      Missing admin_non_white_share: %s\n",
              format(sum(is.na(admin_non_white_share)), big.mark = ",")))
  cat(sprintf("      Missing or zero enrollment: %s\n",
              format(sum(is.na(enrollment) | enrollment == 0), big.mark = ",")))

  # Check how many unique schools
  if (nrow(filtered_df) > 0) {
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
  } else {
    cat("  - NO OBSERVATIONS AFTER FILTERING\n\n")
  }
}

cat("\n")
cat(strrep("=", 70), "\n")
cat("SUMMARY\n")
cat(strrep("=", 70), "\n\n")

cat("This script simulates the filtering logic used in the regression analysis.\n")
cat("The 'FINAL SAMPLE SIZE' for each race group should match the N reported\n")
cat("in the regression output for that group.\n\n")

cat("If sample sizes are unexpectedly small, check:\n")
cat("  1. Teacher data coverage (missing teacher_non_white_share)\n")
cat("  2. Admin data coverage (missing admin_non_white_share)\n")
cat("  3. Suspension rate missingness\n")
cat("  4. Schools with zero or missing enrollment\n\n")
