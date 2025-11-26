# R/aggregate_school_year.R
#
# Standardized School-Year Aggregation Function
#
# Purpose: Aggregate school-year-race-reason level data to school-year level
#          for use in school-level regression analyses.
#
# CRITICAL: Raw CDE data is at school-year-race-reason granularity (~48 obs
#           per school-year). Using this directly in regression violates
#           independence assumptions and produces biased standard errors.
#
# Input: Data frame with school-year-race-reason level observations
# Output: Data frame with school-year level observations
#
# Key Features:
# - Sums suspensions across all races and reasons
# - Correctly handles enrollment (uses max to get total school enrollment)
# - Preserves school-level variables
# - Validates aggregation quality
#
# Usage:
#   source("R/aggregate_school_year.R")
#   df_aggregated <- aggregate_to_school_year(df_raw)

#' Aggregate School-Year-Race-Reason Data to School-Year Level
#'
#' This function aggregates CDE suspension data from school-year-race-reason
#' granularity to school-year level for use in school-level regression analyses.
#'
#' @param df Data frame with school-year-race-reason level observations
#' @param verbose Logical, print diagnostic messages? Default: TRUE
#' @param validate Logical, perform validation checks? Default: TRUE
#'
#' @return Data frame aggregated to school-year level
#'
#' @details
#' **Aggregation Logic**:
#' - **Suspensions**: Sum across all races and reasons (captures total suspensions)
#' - **Enrollment**: Max across races (total school enrollment, constant across race rows)
#' - **School-level variables**: First value (should be constant within school-year)
#'
#' **Why Max for Enrollment**:
#' CDE data includes race-specific enrollment (e.g., Black students = 100,
#' Hispanic students = 200) but cumulative_enrollment at the school level is
#' the SAME across all race rows (e.g., all rows show 500 total students).
#' Taking max() ensures we get the total school enrollment, not race-specific.
#'
#' **Validation**:
#' - Checks that school-level variables are truly constant within school-year
#' - Warns if enrollment varies unexpectedly
#' - Reports aggregation statistics
#'
#' @examples
#' \dontrun{
#' library(arrow)
#' df_raw <- read_parquet("data-stage/susp_v6_teacher_features.parquet")
#' df_agg <- aggregate_to_school_year(df_raw, verbose = TRUE)
#' }
aggregate_to_school_year <- function(df, verbose = TRUE, validate = TRUE) {

  if (!requireNamespace("dplyr", quietly = TRUE)) {
    stop("Package 'dplyr' is required for aggregation")
  }

  library(dplyr)

  if (verbose) {
    message("\n>>> Aggregating to school-year level...")
    message(">>> Initial rows: ", format(nrow(df), big.mark = ","))
  }

  # Identify key column types
  susp_cols <- grep("^total_suspensions", names(df), value = TRUE)
  enrollment_cols <- intersect(c("cumulative_enrollment", "sup_cumulative_enrollment"), names(df))

  # Identify school-level variables (should be constant within school-year)
  teacher_cols <- grep("^teacher_", names(df), value = TRUE)
  charter_cols <- grep("^charter_|^is_traditional", names(df), value = TRUE)
  level_cols <- grep("^level_strict|^school_level", names(df), value = TRUE)
  sed_cols <- grep("^sed_rate|^socio", names(df), value = TRUE, ignore.case = TRUE)
  black_cols <- grep("prop_black|black_share", names(df), value = TRUE)
  quartile_cols <- grep("_prop_q|_quartile", names(df), value = TRUE)

  constant_cols <- unique(c(
    teacher_cols,
    charter_cols,
    level_cols,
    sed_cols,
    black_cols,
    quartile_cols
  ))
  constant_cols <- intersect(constant_cols, names(df))

  if (verbose) {
    message(">>> Identified columns:")
    message("    - Suspensions: ", paste(susp_cols, collapse = ", "))
    message("    - Enrollment: ", paste(enrollment_cols, collapse = ", "))
    message("    - School-level variables: ", length(constant_cols), " columns")
  }

  # VALIDATION: Check if enrollment is constant within school-year
  if (validate && length(enrollment_cols) > 0) {
    enrollment_col <- enrollment_cols[1]

    enrollment_check <- df %>%
      group_by(cds_school, academic_year) %>%
      summarise(
        n_distinct_enrollment = n_distinct(!!sym(enrollment_col), na.rm = TRUE),
        min_enrollment = min(!!sym(enrollment_col), na.rm = TRUE),
        max_enrollment = max(!!sym(enrollment_col), na.rm = TRUE),
        .groups = "drop"
      )

    varying_enrollment <- enrollment_check %>%
      filter(n_distinct_enrollment > 1)

    if (nrow(varying_enrollment) > 0) {
      warning(
        "VALIDATION WARNING: Enrollment varies within school-year for ",
        nrow(varying_enrollment), " school-years.\n",
        "  This may indicate race-specific enrollment values.\n",
        "  Using max() to extract total school enrollment.\n",
        "  Sample school-year with varying enrollment:\n",
        "    CDS: ", varying_enrollment$cds_school[1], "\n",
        "    Year: ", varying_enrollment$academic_year[1], "\n",
        "    Min: ", varying_enrollment$min_enrollment[1], "\n",
        "    Max: ", varying_enrollment$max_enrollment[1]
      )
    } else {
      if (verbose) {
        message(">>> ✓ Enrollment is constant within school-year (validation passed)")
      }
    }
  }

  # Perform aggregation
  agg_df <- df %>%
    group_by(cds_school, academic_year) %>%
    summarise(
      # Sum suspensions across all races and reasons
      across(any_of(susp_cols), ~sum(.x, na.rm = TRUE), .names = "{.col}"),

      # CRITICAL FIX: Use max() for enrollment to get total school enrollment
      # (not race-specific enrollment)
      across(any_of(enrollment_cols), ~max(.x, na.rm = TRUE), .names = "{.col}"),

      # Take first value of school-level variables (should be constant)
      across(any_of(constant_cols), ~first(.x), .names = "{.col}"),

      # Preserve additional metadata
      across(any_of(c("school_code", "aggregate_level")), ~first(.x)),

      # Count observations aggregated (diagnostic)
      n_observations_aggregated = n(),

      .groups = "drop"
    )

  if (verbose) {
    message(">>> Aggregated to: ", format(nrow(agg_df), big.mark = ","), " school-year observations")
    message(">>> Average observations per school-year: ", round(nrow(df) / nrow(agg_df), 1))

    # Report enrollment range (sanity check)
    if (length(enrollment_cols) > 0) {
      enrollment_col <- enrollment_cols[1]
      enrollment_range <- range(agg_df[[enrollment_col]], na.rm = TRUE)
      message(">>> Enrollment range after aggregation: [",
              format(enrollment_range[1], big.mark = ","), ", ",
              format(enrollment_range[2], big.mark = ","), "]")
    }
  }

  # VALIDATION: Check for suspicious suspension rates
  if (validate && length(susp_cols) > 0 && length(enrollment_cols) > 0) {
    susp_col <- susp_cols[1]
    enrollment_col <- enrollment_cols[1]

    check_df <- agg_df %>%
      mutate(
        susp_rate_check = !!sym(susp_col) / !!sym(enrollment_col)
      )

    high_rate <- check_df %>%
      filter(susp_rate_check > 1.0, !is.na(susp_rate_check))

    if (nrow(high_rate) > 0) {
      warning(
        "VALIDATION WARNING: ", nrow(high_rate),
        " school-years have suspension rate > 100% after aggregation.\n",
        "  This may indicate enrollment aggregation issues.\n",
        "  Max suspension rate: ", sprintf("%.1f%%", max(check_df$susp_rate_check, na.rm = TRUE) * 100), "\n",
        "  Review aggregation logic and data quality."
      )
    } else {
      if (verbose) {
        message(">>> ✓ All suspension rates <= 100% (validation passed)")
      }
    }
  }

  return(agg_df)
}

# Convenience wrapper for backward compatibility
aggregate_school_year <- aggregate_to_school_year
