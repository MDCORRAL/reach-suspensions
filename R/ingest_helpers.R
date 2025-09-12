# R/ingest_helpers.R -- shared helper functions for ingest scripts

#' Pick likely column name from candidates.
#'
#' @param df data.frame
#' @param candidates character vector of possible column names
#' @param required logical: error if not found
#' @param label optional label for error message
#' @return name of the first matching column or NA if none and `required` is FALSE
pick_col <- function(df, candidates, required = TRUE, label = NULL) {
  nm <- intersect(candidates, names(df))[1]
  if (is.na(nm) && required) {
    stop(
      "Missing expected column",
      if (!is.null(label)) paste0(" for ", label), ": ",
      paste(candidates, collapse = ", ")
    )
  }
  nm
}

#' Derive year and academic_year columns from possible fields.
#'
#' Attempts to find a numeric `year` column or a preformatted `academic_year`
#' column. If only `year` is present, `academic_year` is constructed as
#' `"<year-1>-<yy>"` (e.g. 2024 -> "2023-24").
#'
#' @param df data.frame containing possible year columns
#' @return list with elements `year` (integer) and `academic_year` (character)
derive_year <- function(df) {
  ay_col   <- pick_col(df, c("academic_year", "academic_yr"), FALSE)
  year_col <- pick_col(df, c("year"), FALSE)

  yr <- if (!is.na(year_col)) suppressWarnings(as.integer(df[[year_col]])) else NA_integer_
  ay <- if (!is.na(ay_col)) as.character(df[[ay_col]]) else NA_character_

  academic_year <- dplyr::coalesce(
    if (!all(is.na(ay))) ay else NA_character_,
    ifelse(!is.na(yr), paste0(yr - 1, "-", substr(yr, 3, 4)), NA_character_)
  )

  list(year = yr, academic_year = academic_year)
}

#' Identify numeric columns in CDE suspension files.
#'
#' Uses a consistent regex to pick canonical numeric fields (counts and rates).
#'
#' @param df data.frame whose column names will be scanned
#' @return character vector of numeric-like column names
numeric_cols <- function(df) {
  grep(
    "^cumulative_enrollment$|^total_suspensions$|^unduplicated_count_of_students_suspended|^suspension_",
    names(df),
    value = TRUE
  )
}

