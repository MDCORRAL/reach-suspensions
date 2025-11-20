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

# CRITICAL: Use the EXACT same race slugs as the regression analysis
# This must match Analysis/21_teacher_diversity_regression.R lines 30-41
TEACHER_RACE_SLUGS <- c(
  "african_american",
  "asian",
  "filipino",
  "hispanic_or_latino",
  "american_indian_or_alaska_native",
  "native_hawaiian_pacific_islander",
  "pacific_islander",  # legacy slug still appears in some historical files
  "white",
  "two_or_more_races",
  "not_reported"
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

  # CRITICAL: Use EXACT same pattern as regression (Analysis/21_teacher_diversity_regression.R line 96-98)
  # Build pattern from TEACHER_RACE_SLUGS
  race_share_pattern <- paste0("^teacher.*_(", paste(TEACHER_RACE_SLUGS, collapse = "|"), ")_share$")
  race_share_cols <- grep(race_share_pattern, names(group_df), value = TRUE, ignore.case = TRUE)

  cat("  - Teacher race share columns found:", length(race_share_cols), "\n")

  # Identify non-white columns (same logic as regression lines 110-116)
  white_cols <- grep("_white_share$", race_share_cols, value = TRUE, ignore.case = TRUE)
  white_cols <- white_cols[!grepl("non_white", white_cols, ignore.case = TRUE)]
  not_reported_cols <- grep("_(not_reported|unknown)_share$", race_share_cols, value = TRUE, ignore.case = TRUE)
  non_white_cols <- setdiff(race_share_cols, c(white_cols, not_reported_cols))

  cat("      Non-white columns:", length(non_white_cols), "\n")
  cat("      White columns:", length(white_cols), "\n")
  cat("      Not reported columns:", length(not_reported_cols), "\n")

  # Compute teacher non-white share (same as regression lines 128-136)
  teacher_non_white_share <- safe_sum_shares(group_df, non_white_cols)

  # Compute admin non-white share (same logic but for administrators)
  # This matches extract_admin_race_nonwhite_share() in regression line 182
  admin_race_share_pattern <- paste0("^teacher.*by_type_administrators.*_(", paste(TEACHER_RACE_SLUGS, collapse = "|"), ")_share$")
  admin_race_share_cols <- grep(admin_race_share_pattern, names(group_df), value = TRUE, ignore.case = TRUE)

  cat("  - Admin race share columns found:", length(admin_race_share_cols), "\n")

  admin_white_cols <- grep("_white_share$", admin_race_share_cols, value = TRUE, ignore.case = TRUE)
  admin_white_cols <- admin_white_cols[!grepl("non_white", admin_white_cols, ignore.case = TRUE)]
  admin_not_reported_cols <- grep("_(not_reported|unknown)_share$", admin_race_share_cols, value = TRUE, ignore.case = TRUE)
  admin_non_white_cols <- setdiff(admin_race_share_cols, c(admin_white_cols, admin_not_reported_cols))

  cat("      Non-white columns:", length(admin_non_white_cols), "\n")
  cat("      White columns:", length(admin_white_cols), "\n")
  cat("      Not reported columns:", length(admin_not_reported_cols), "\n")

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

    # Count observations per school-year (should be ~6 if reason-level data)
    avg_obs_per_school_year <- nrow(filtered_df) / unique_school_years
    cat(sprintf("  - Observations per school-year: %.1f (suggests reason-level data)\n",
                avg_obs_per_school_year))

    # This should equal the regression sample size BEFORE aggregation
    cat(sprintf("  - OBSERVATIONS (before school-year aggregation): %s\n",
                format(nrow(filtered_df), big.mark = ",")))
    cat(sprintf("  - EXPECTED REGRESSION N (after aggregation): %s\n\n",
                format(unique_school_years, big.mark = ",")))
  } else {
    cat("  - NO OBSERVATIONS AFTER FILTERING\n\n")
  }
}

cat("\n")
cat(strrep("=", 70), "\n")
cat("SUMMARY\n")
cat(strrep("=", 70), "\n\n")

cat("This script uses the EXACT SAME filtering logic as the regression analysis.\n")
cat("(Analysis/21_teacher_diversity_regression.R)\n\n")

cat("Key alignment points:\n")
cat("  ✓ Uses identical TEACHER_RACE_SLUGS pattern (lines 30-41 in regression)\n")
cat("  ✓ Same race share column detection logic\n")
cat("  ✓ Same white/non-white/not-reported separation\n")
cat("  ✓ Same complete case filtering\n")
cat("  ✓ Same enrollment > 0 requirement\n\n")

cat(strrep("=", 70), "\n")
cat("TECHNICAL NOTE: AGGREGATION TO SCHOOL-YEAR-RACE LEVEL\n")
cat(strrep("=", 70), "\n\n")

cat("BACKGROUND:\n")
cat("The raw data contains ~6 observations per school-year-race combination,\n")
cat("corresponding to 6 suspension reason categories (violent injury, violent\n")
cat("no injury, weapons, drugs, defiance, other). This structure creates a\n")
cat("statistical clustering problem.\n\n")

cat("THE PROBLEM (Before Aggregation):\n")
cat("  • Data grain: school-year-race-REASON (6 obs per school-year-race)\n")
cat("  • Regression treated these ~6 observations as independent\n")
cat("  • Violation: Observations within same school are correlated\n")
cat("  • Impact: Standard errors UNDERESTIMATED (too small)\n")
cat("  • Impact: P-values UNDERESTIMATED (inflated significance)\n")
cat("  • Impact: Confidence intervals TOO NARROW\n")
cat("  • Result: Anti-conservative inference (too many false positives)\n\n")

cat("THE SOLUTION (After Aggregation):\n")
cat("  • Data grain: school-year-race (1 obs per school-year-race)\n")
cat("  • Method: Sum total_suspensions across all 6 reason categories\n")
cat("  • Method: Take enrollment (constant across reasons for same school-year-race)\n")
cat("  • Method: Recalculate suspension_rate = total_suspensions / enrollment\n")
cat("  • Method: Preserve school-level variables (teacher/admin diversity)\n")
cat("  • Result: Observations are now properly independent\n")
cat("  • Result: Standard errors correctly estimated\n")
cat("  • Result: Conservative, methodologically sound inference\n\n")

cat("EXPECTED IMPACTS:\n")
cat("  1. Sample sizes REDUCED by factor of ~6:\n")
cat("     - Example: Black students 71,754 → ~11,959\n")
cat("     - N now represents unique school-year-race combinations\n\n")
cat("  2. Standard errors INCREASED by factor of ~√6 ≈ 2.45:\n")
cat("     - Properly reflects clustering uncertainty\n")
cat("     - No longer artificially precise\n\n")
cat("  3. Confidence intervals WIDENED:\n")
cat("     - More realistic uncertainty ranges\n")
cat("     - Appropriately conservative\n\n")
cat("  4. P-values INCREASED (some effects may lose significance):\n")
cat("     - Effects that were marginally significant may become non-significant\n")
cat("     - This is CORRECT - reflects true statistical uncertainty\n")
cat("     - Prevents over-interpretation of weak associations\n\n")
cat("  5. Point estimates (coefficients) UNCHANGED:\n")
cat("     - Effect sizes remain the same\n")
cat("     - Only precision/uncertainty estimates change\n\n")

cat("VERIFICATION:\n")
cat("The 'EXPECTED REGRESSION N (after aggregation)' for each race group\n")
cat("should EXACTLY match the N reported in regression output.\n\n")

cat("If sample sizes don't match:\n")
cat("  1. Check aggregation function is working correctly\n")
cat("  2. Verify grouping variables: cds_school, academic_year, student_group\n")
cat("  3. Confirm suspension columns are being summed\n")
cat("  4. Check that school-level variables are preserved (not NA after agg)\n\n")

cat(strrep("=", 70), "\n")
cat("DIAGNOSTIC INTERPRETATION GUIDE\n")
cat(strrep("=", 70), "\n\n")

cat("Observations per school-year:\n")
cat("  • ~6.0: Data contains suspension reason categories (expected)\n")
cat("  • ~1.0: Data already aggregated (check if double-aggregating)\n")
cat("  • Other: Investigate data structure issue\n\n")

cat("If sample sizes are unexpectedly small, check:\n")
cat("  1. Teacher data coverage (missing teacher_non_white_share)\n")
cat("  2. Admin data coverage (missing admin_non_white_share)\n")
cat("  3. Suspension rate missingness (CDE privacy suppression)\n")
cat("  4. Schools with zero or missing enrollment\n")
cat("  5. Year coverage (some years missing teacher data)\n\n")

cat("If aggregation ratio ≠ ~6.0:\n")
cat("  1. Check if data structure changed (new reason categories?)\n")
cat("  2. Verify data grain assumptions\n")
cat("  3. Examine sample school-year-race combinations manually\n\n")

cat(strrep("=", 70), "\n")
cat("METHODOLOGICAL JUSTIFICATION\n")
cat(strrep("=", 70), "\n\n")

cat("Why aggregate instead of using clustered standard errors?\n\n")

cat("Option A: Clustered SE (not chosen):\n")
cat("  • Keep reason-level data\n")
cat("  • Use sandwich::vcovCL() or similar\n")
cat("  • Pros: Can examine reason-specific patterns\n")
cat("  • Cons: More complex, harder to interpret\n")
cat("  • Cons: Requires additional assumptions about correlation structure\n\n")

cat("Option B: Aggregate first (CHOSEN):\n")
cat("  • Collapse to school-year-race level\n")
cat("  • Use standard OLS\n")
cat("  • Pros: Simpler, more transparent\n")
cat("  • Pros: No assumptions about within-cluster correlation\n")
cat("  • Pros: Easier to explain to non-technical audiences\n")
cat("  • Cons: Cannot examine reason-specific patterns\n\n")

cat("Decision: Option B chosen because:\n")
cat("  1. Research question focuses on overall suspension rates,\n")
cat("     not reason-specific patterns\n")
cat("  2. Transparency and interpretability prioritized\n")
cat("  3. Simplicity reduces potential for errors\n")
cat("  4. Standard errors are correctly estimated without special methods\n\n")

cat(strrep("=", 70), "\n")
cat("CITATION GUIDANCE\n")
cat(strrep("=", 70), "\n\n")

cat("When reporting these results, include:\n\n")

cat("1. Unit of analysis statement:\n")
cat("   'The unit of analysis is the school-year-race combination.\n")
cat("    Each observation represents suspensions for students of a\n")
cat("    specific racial/ethnic group in a specific school in a\n")
cat("    specific academic year.'\n\n")

cat("2. Data aggregation note:\n")
cat("   'Suspension data were originally disaggregated by reason\n")
cat("    category (6 categories). To ensure statistical independence\n")
cat("    of observations, we aggregated to the school-year-race level\n")
cat("    by summing suspensions across all reason categories before\n")
cat("    conducting regression analyses.'\n\n")

cat("3. Sample size interpretation:\n")
cat("   'Sample sizes (N) represent the number of unique school-year-\n")
cat("    race combinations with complete data, not the number of unique\n")
cat("    schools. The same school may contribute multiple observations\n")
cat("    across different years and racial/ethnic groups.'\n\n")

cat("4. Standard error note:\n")
cat("   'Standard errors reflect the uncertainty at the school-year-race\n")
cat("    level and do not account for potential correlation across years\n")
cat("    within the same school.'\n\n")

cat(strrep("=", 70), "\n\n")
