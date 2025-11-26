# R/aggregate_school_year_v3.R
#
# FINAL CORRECT VERSION: Use subgroup == "All Students" for aggregation
#
# Key Discovery: CDE data uses "subgroup" (not "race") for demographic categories.
# The "All Students" value in subgroup provides pre-calculated totals.
#
# This approach:
# - Uses CDE's official totals (authoritative source)
# - Filters to subgroup == "All Students" first (reduces data ~8x immediately)
# - Then aggregates across suspension reasons only
# - Results in accurate enrollment and suspension totals

#' Aggregate School-Year-Subgroup-Reason Data to School-Year Level (FINAL)
#'
#' Uses CDE's "All Students" subgroup category for accurate totals.
#'
#' @param df Data frame with school-year-subgroup-reason level observations
#' @param verbose Logical, print diagnostic messages? Default: TRUE
#' @param validate Logical, perform validation checks? Default: TRUE
#'
#' @return Data frame aggregated to school-year level
#'
#' @details
#' **Key Improvement**: Filters to `subgroup == "All Students"` which provides
#' CDE's pre-calculated totals across all demographic groups.
#'
#' **Data Structure**:
#' - Raw CDE data: school-year-subgroup-reason level (~56 obs per school-year)
#' - After filtering to "All Students": school-year-reason level (~6 obs per school-year)
#' - After aggregation: school-year level (1 obs per school-year)
#'
#' **Why This Works**:
#' - `subgroup == "All Students"` has enrollment = total school enrollment
#' - `subgroup == "All Students"` has `total_suspensions` = TOTAL across ALL reasons
#' - In long format, `total_suspensions` is repeated on each reason row (constant)
#' - Use `first(total_suspensions)` to get correct total (NOT sum, which would multiply by 6)
#' - No need for max() workaround - enrollment is truly constant
#'
#' @examples
#' \dontrun{
#' library(arrow)
#' df_raw <- read_parquet("data-stage/susp_v6_teacher_features.parquet")
#' df_agg <- aggregate_to_school_year_v3(df_raw, verbose = TRUE)
#' }
aggregate_to_school_year_v3 <- function(df, verbose = TRUE, validate = TRUE) {

  if (!requireNamespace("dplyr", quietly = TRUE)) {
    stop("Package 'dplyr' is required for aggregation")
  }

  library(dplyr)

  if (verbose) {
    message("\n>>> Aggregating to school-year level (using 'All Students' subgroup)...")
    message(">>> Initial rows: ", format(nrow(df), big.mark = ","))
  }

  # Check if subgroup column exists
  if (!"subgroup" %in% names(df)) {
    stop("Column 'subgroup' not found in dataset. Cannot filter to 'All Students'.")
  }

  # Check if "All Students" category exists
  subgroup_values <- unique(df$subgroup)
  if (!"All Students" %in% subgroup_values) {
    warning(
      "subgroup == 'All Students' not found in data.\n",
      "  Available subgroup values: ", paste(head(subgroup_values, 10), collapse = ", "), "\n",
      "  Cannot use filtering approach."
    )
    stop("'All Students' subgroup not found. Cannot proceed with aggregation.")
  }

  # STEP 1: Filter to "All Students" (CDE's pre-calculated total)
  df_all_students <- df %>%
    filter(subgroup == "All Students")

  n_all_students <- nrow(df_all_students)
  reduction_ratio <- nrow(df) / n_all_students

  if (verbose) {
    message(">>> After filtering to 'All Students': ",
            format(n_all_students, big.mark = ","), " rows")
    message(">>> Data reduction: ", round(reduction_ratio, 1), "x")
  }

  # Identify column types
  susp_cols <- grep("^total_suspensions|^suspension_count", names(df_all_students), value = TRUE)
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
    enrollment_cols,  # Now truly constant (not subgroup-specific)
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
    message("    - Suspension columns: ", length(susp_cols))
    message("    - Enrollment columns: ", paste(enrollment_cols, collapse = ", "))
    message("    - School-level variables: ", length(constant_cols), " columns")
  }

  # VALIDATION: Check if enrollment is constant within school-year-subgroup
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
        "  This is unexpected. Enrollment should be constant for 'All Students' subgroup."
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
      # Take first value of total_suspensions (constant across reason rows)
      # NOTE: In long format, total_suspensions is the SAME on all reason rows
      # Summing would multiply by ~6 (number of reasons) - use first() instead
      across(any_of(susp_cols), ~first(.x), .names = "{.col}"),

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
  if (validate && "total_suspensions" %in% names(agg_df) && length(enrollment_cols) > 0) {
    enrollment_col <- enrollment_cols[1]

    check_df <- agg_df %>%
      mutate(
        susp_rate_check = total_suspensions / !!sym(enrollment_col)
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

    # Report realistic range
    median_rate <- median(check_df$susp_rate_check, na.rm = TRUE) * 100
    if (median_rate >= 1 && median_rate <= 20) {
      if (verbose) {
        message(">>> ✓ Median suspension rate is realistic: ", sprintf("%.2f%%", median_rate))
      }
    } else {
      warning(
        "VALIDATION WARNING: Median suspension rate outside typical range (1-20%).\n",
        "  Median: ", sprintf("%.2f%%", median_rate)
      )
    }
  }

  return(agg_df)
}

# Convenience wrapper
aggregate_school_year_v3 <- aggregate_to_school_year_v3
