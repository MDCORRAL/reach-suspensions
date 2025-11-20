# scripts/diagnostics/validate_script_24_25_fixes.R
#
# Validation Script for Scripts 24 & 25 Fixes
#
# Purpose: Verify that clustering and enrollment fixes are working correctly
# Run this after fixing scripts 24 and 25
#
# Expected outcomes:
# 1. Sample size ~130K school-year observations (not 3.4M)
# 2. Suspension rates 0-30% (not hundreds of thousands of percent)
# 3. Standard errors reasonable magnitude
# 4. No validation warnings

suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
})

message("\n═══════════════════════════════════════════════════════════════")
message("VALIDATION: Scripts 24 & 25 Fixes")
message("═══════════════════════════════════════════════════════════════\n")

# Load the standardized aggregation function
source(here::here("R", "aggregate_school_year.R"))

# Load raw data
message(">>> Loading raw data...")
MERGED_PATH <- here::here("data-stage", "susp_v6_teacher_features.parquet")

if (!file.exists(MERGED_PATH)) {
  stop("Missing file: ", MERGED_PATH, "\n",
       "Run Analysis/18_merge_teacher_student.R first.")
}

df_raw <- read_parquet(MERGED_PATH)
message(">>> Loaded: ", format(nrow(df_raw), big.mark = ","), " rows")

# Test aggregation with validation
message("\n>>> Testing aggregation function...")
df_agg <- aggregate_to_school_year(df_raw, verbose = TRUE, validate = TRUE)

# Check sample size
message("\n═══════════════════════════════════════════════════════════════")
message("TEST 1: Sample Size")
message("═══════════════════════════════════════════════════════════════")

expected_obs <- nrow(df_agg)
if (expected_obs >= 50000 && expected_obs <= 150000) {
  message("✓ PASS: Sample size in expected range")
  message("  Expected: 50,000 - 150,000 school-year observations")
  message("  Actual: ", format(expected_obs, big.mark = ","))
} else {
  message("✗ FAIL: Sample size outside expected range")
  message("  Expected: 50,000 - 150,000 school-year observations")
  message("  Actual: ", format(expected_obs, big.mark = ","))
}

# Check suspension rates
message("\n═══════════════════════════════════════════════════════════════")
message("TEST 2: Suspension Rate Validity")
message("═══════════════════════════════════════════════════════════════")

if ("total_suspensions" %in% names(df_agg) && "cumulative_enrollment" %in% names(df_agg)) {
  df_check <- df_agg %>%
    filter(cumulative_enrollment > 0) %>%
    mutate(suspension_rate_pct = (total_suspensions / cumulative_enrollment) * 100)

  rate_summary <- summary(df_check$suspension_rate_pct)
  message(">>> Suspension rate distribution (%):")
  print(rate_summary)

  if (max(df_check$suspension_rate_pct, na.rm = TRUE) <= 100) {
    message("✓ PASS: All suspension rates <= 100%")
  } else {
    high_rate_count <- sum(df_check$suspension_rate_pct > 100, na.rm = TRUE)
    message("✗ FAIL: ", high_rate_count, " observations have suspension rate > 100%")
    message("  Max rate: ", sprintf("%.1f%%", max(df_check$suspension_rate_pct, na.rm = TRUE)))
    message("  This indicates enrollment aggregation issues")
  }

  if (median(df_check$suspension_rate_pct, na.rm = TRUE) >= 1 &&
      median(df_check$suspension_rate_pct, na.rm = TRUE) <= 20) {
    message("✓ PASS: Median suspension rate in realistic range (1-20%)")
  } else {
    message("⚠ WARNING: Median suspension rate outside typical range (1-20%)")
    message("  Median: ", sprintf("%.2f%%", median(df_check$suspension_rate_pct, na.rm = TRUE)))
  }
} else {
  message("⚠ SKIP: Cannot calculate suspension rates (missing columns)")
}

# Check enrollment distribution
message("\n═══════════════════════════════════════════════════════════════")
message("TEST 3: Enrollment Distribution")
message("═══════════════════════════════════════════════════════════════")

if ("cumulative_enrollment" %in% names(df_agg)) {
  enrollment_summary <- summary(df_agg$cumulative_enrollment)
  message(">>> Enrollment distribution:")
  print(enrollment_summary)

  if (median(df_agg$cumulative_enrollment, na.rm = TRUE) >= 100 &&
      median(df_agg$cumulative_enrollment, na.rm = TRUE) <= 2000) {
    message("✓ PASS: Median enrollment in realistic range (100-2000 students)")
  } else {
    message("⚠ WARNING: Median enrollment outside typical range")
    message("  Median: ", format(median(df_agg$cumulative_enrollment, na.rm = TRUE), big.mark = ","))
  }
}

# Check for unexpected NAs
message("\n═══════════════════════════════════════════════════════════════")
message("TEST 4: Missing Data Check")
message("═══════════════════════════════════════════════════════════════")

key_cols <- intersect(
  c("cumulative_enrollment", "total_suspensions", "teacher_total_staff_count_white_share",
    "prop_black", "black_share"),
  names(df_agg)
)

for (col in key_cols) {
  na_count <- sum(is.na(df_agg[[col]]))
  na_pct <- na_count / nrow(df_agg) * 100

  if (na_pct < 50) {
    message("✓ ", col, ": ", sprintf("%.1f%%", na_pct), " missing")
  } else {
    message("⚠ ", col, ": ", sprintf("%.1f%%", na_pct), " missing (HIGH)")
  }
}

# Compare with unaggregated data (clustering check)
message("\n═══════════════════════════════════════════════════════════════")
message("TEST 5: Clustering Fix Verification")
message("═══════════════════════════════════════════════════════════════")

obs_ratio <- nrow(df_raw) / nrow(df_agg)
message(">>> Observations per school-year: ", round(obs_ratio, 1))

if (obs_ratio >= 20 && obs_ratio <= 60) {
  message("✓ PASS: Aggregation ratio in expected range (20-60 obs per school-year)")
  message("  This confirms raw data was at school-year-race-reason granularity")
} else {
  message("⚠ WARNING: Aggregation ratio outside expected range")
  message("  Expected: 20-60 observations aggregated per school-year")
  message("  Actual: ", round(obs_ratio, 1))
}

# Final summary
message("\n═══════════════════════════════════════════════════════════════")
message("VALIDATION SUMMARY")
message("═══════════════════════════════════════════════════════════════\n")

message("Next steps:")
message("1. Run Analysis/25_interaction_term_regression.R")
message("2. Check console output for:")
message("   - Aggregated sample size (~130K)")
message("   - Suspension rates (should be 0-30%, not thousands)")
message("   - No validation warnings")
message("3. Review outputs/summaries/25_interaction_term_regression_SUMMARY.md")
message("4. Compare results with Analysis 24 (quartile slopes)")
message("\nExpected changes after fix:")
message("- Sample size: ~3.4M → ~130K (clustering fix)")
message("- Suspension rates: Hundreds of thousands % → 3-10% (enrollment fix)")
message("- Standard errors: Larger (more realistic)")
message("- P-values: May increase (still significant if real effect)\n")

message("═══════════════════════════════════════════════════════════════")
message("✓ VALIDATION COMPLETE")
message("═══════════════════════════════════════════════════════════════\n")
