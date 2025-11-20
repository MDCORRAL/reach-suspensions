# R/aggregate_school_year_v2.R
#
# IMPROVED: Aggregate School-Year Data Using CDE's "All Students" Total
#
# This version uses CDE's pre-calculated "All Students" (TA) category
# instead of summing across individual races.
#
# Key Improvement: Filters to race == "All Students" FIRST, which:
# - Uses CDE's official totals (more accurate)
# - Avoids potential double-counting across races
# - Reduces data immediately (8 races → 1 total)
# - Makes enrollment truly constant (not race-specific)
#
# Credit: User suggestion to use existing "Total" row instead of summing

#' Aggregate School-Year-Race-Reason Data to School-Year Level (v2)
#'
#' IMPROVED VERSION: Uses CDE's "All Students" category for totals.
#'
#' @param df Data frame with school-year-race-reason level observations
#' @param verbose Logical, print diagnostic messages? Default: TRUE
#' @param validate Logical, perform validation checks? Default: TRUE
#'
#' @return Data frame aggregated to school-year level
#'
#' @details
#' **Key Improvement**: This version filters to `race == "All Students"`
#' (CDE code "TA") which provides pre-calculated totals across all races.
#' This is more accurate and efficient than summing individual race categories.
#'
#' **Aggregation Steps**:
#' 1. Filter to race == "All Students" (reduces ~8x immediately)
#' 2. Aggregate across suspension reasons (sum suspensions, first enrollment)
#' 3. Result: One row per school-year with total suspensions and enrollment
#'
#' **Why This is Better**:
#' - Uses CDE's official aggregation (authoritative source)
#' - Enrollment is truly constant (not race-specific values)
#' - Simpler logic (filter then aggregate reasons only)
#' - Faster (processes 8x less data)
#'
#' @examples
#' \dontrun{
#' library(arrow)
#' df_raw <- read_parquet("data-stage/susp_v6_teacher_features.parquet")
#' df_agg <- aggregate_to_school_year_v2(df_raw, verbose = TRUE)
#' }
aggregate_to_school_year_v2 <- function(df, verbose = TRUE, validate = TRUE) {

  if (!requireNamespace("dplyr", quietly = TRUE)) {
    stop("Package 'dplyr' is required for aggregation")
  }

  library(dplyr)

  if (verbose) {
    message("\n>>> Aggregating to school-year level (using 'All Students' total)...")
    message(">>> Initial rows: ", format(nrow(df), big.mark = ","))
  }

  # Check if race column exists
  if (!"race" %in% names(df)) {
    stop("Column 'race' not found in dataset. Cannot filter to 'All Students'.")
  }

  # Check if "All Students" category exists
  race_values <- unique(df$race)
  if (!"All Students" %in% race_values) {
    warning(
      "race == 'All Students' not found in data.\n",
      "  Available race values: ", paste(head(race_values, 10), collapse = ", "), "\n",
      "  Falling back to summing across races (less accurate)."
    )
    # Fall back to summing approach if "All Students" doesn't exist
    return(aggregate_to_school_year_fallback(df, verbose, validate))
  }

  # STEP 1: Filter to "All Students" (CDE's pre-calculated total)
  df_all_students <- df %>%
    filter(race == "All Students")

  n_all_students <- nrow(df_all_students)
  reduction_ratio <- nrow(df) / n_all_students

  if (verbose) {
    message(">>> After filtering to 'All Students': ",
            format(n_all_students, big.mark = ","), " rows")
    message(">>> Data reduction: ", round(reduction_ratio, 1), "x")
  }

  # Identify column types
  susp_cols <- grep("^total_suspensions", names(df_all_students), value = TRUE)
  enrollment_cols <- intersect(
    c("cumulative_enrollment", "sup_cumulative_enrollment"),
    names(df_all_students)
  )

  # Identify school-level variables (should be constant within school-year)
  teacher_cols <- grep("^teacher_", names(df_all_students), value = TRUE)
  charter_cols <- grep("^charter_|^is_traditional", names(df_all_students), value = TRUE)
  level_cols <- grep("^level_strict|^school_level", names(df_all_students), value = TRUE)
  sed_cols <- grep("^sed_rate|^socio", names(df_all_students), value = TRUE, ignore.case = TRUE)
  black_cols <- grep("prop_black|black_share", names(df_all_students), value = TRUE)
  quartile_cols <- grep("_prop_q|_quartile", names(df_all_students), value = TRUE)

  constant_cols <- unique(c(
    enrollment_cols,  # Now truly constant (not race-specific)
    teacher_cols,
    charter_cols,
    level_cols,
    sed_cols,
    black_cols,
    quartile_cols
  ))
  constant_cols <- intersect(constant_cols, names(df_all_students))

  if (verbose && length(susp_cols) > 0) {
    message(">>> Aggregating across suspension reasons...")
    message("    - Suspensions: ", paste(susp_cols, collapse = ", "))
    message("    - Enrollment: ", paste(enrollment_cols, collapse = ", "))
    message("    - School-level variables: ", length(constant_cols), " columns")
  }

  # VALIDATION: Check if enrollment is constant within school-year-race
  # (Should be TRUE since we filtered to "All Students")
  if (validate && length(enrollment_cols) > 0) {
    enrollment_col <- enrollment_cols[1]

    enrollment_check <- df_all_students %>%
      group_by(cds_school, academic_year) %>%
      summarise(
        n_distinct_enrollment = n_distinct(!!sym(enrollment_col), na.rm = TRUE),
        .groups = "drop"
      )

    varying_enrollment <- enrollment_check %>%
      filter(n_distinct_enrollment > 1)

    if (nrow(varying_enrollment) > 0) {
      warning(
        "VALIDATION WARNING: Enrollment varies within school-year for ",
        nrow(varying_enrollment), " school-years even after filtering to 'All Students'.\n",
        "  This is unexpected. Enrollment should be constant for 'All Students' category."
      )
    } else {
      if (verbose) {
        message(">>> ✓ Enrollment is constant within school-year (validation passed)")
      }
    }
  }

  # STEP 2: Aggregate across suspension reasons
  agg_df <- df_all_students %>%
    group_by(cds_school, academic_year) %>%
    summarise(
      # Sum suspensions across all reasons (now truly summing reasons only, not races)
      across(any_of(susp_cols), ~sum(.x, na.rm = TRUE), .names = "{.col}"),

      # Take first value of enrollment (truly constant now that we have "All Students")
      across(any_of(enrollment_cols), ~first(.x), .names = "{.col}"),

      # Take first value of school-level variables (should be constant)
      across(any_of(constant_cols), ~first(.x), .names = "{.col}"),

      # Preserve additional metadata
      across(any_of(c("school_code", "aggregate_level")), ~first(.x)),

      # Count observations aggregated (diagnostic - should be ~6 reasons)
      n_reasons_aggregated = n(),

      .groups = "drop"
    )

  if (verbose) {
    message(">>> Final aggregated rows: ", format(nrow(agg_df), big.mark = ","),
            " school-year observations")
    message(">>> Average reasons per school-year: ",
            round(nrow(df_all_students) / nrow(agg_df), 1))

    # Report enrollment range (sanity check)
    if (length(enrollment_cols) > 0) {
      enrollment_col <- enrollment_cols[1]
      enrollment_range <- range(agg_df[[enrollment_col]], na.rm = TRUE)
      message(">>> Enrollment range: [",
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
        "  Max suspension rate: ",
        sprintf("%.1f%%", max(check_df$susp_rate_check, na.rm = TRUE) * 100), "\n",
        "  This may indicate data quality issues."
      )
    } else {
      if (verbose) {
        message(">>> ✓ All suspension rates <= 100% (validation passed)")
      }
    }
  }

  return(agg_df)
}

# Fallback function if "All Students" category doesn't exist
aggregate_to_school_year_fallback <- function(df, verbose, validate) {
  # This is the old summing approach (less accurate but works as fallback)
  if (verbose) {
    message(">>> WARNING: Using fallback aggregation (summing across races)")
    message(">>> This is less accurate than using 'All Students' total")
  }

  # [Original aggregation logic would go here]
  # For now, just source the original function
  source(here::here("R", "aggregate_school_year.R"))
  return(aggregate_to_school_year(df, verbose, validate))
}

# Convenience wrapper
aggregate_school_year_v2 <- aggregate_to_school_year_v2
