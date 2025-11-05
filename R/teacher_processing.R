# R/teacher_processing.R -- helpers for teacher demographic ingestion/merging

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(stringr)
  library(tidyr)
})

#' Internal: lowercase helper that returns empty strings when column is missing.
#'
teacher_pull_lower <- function(df, col) {
  if (!col %in% names(df)) return(rep("", nrow(df)))
  stringr::str_to_lower(dplyr::coalesce(df[[col]], ""))
}

#' Internal: uppercase helper that returns empty strings when column is missing.
#'
teacher_pull_upper <- function(df, col) {
  if (!col %in% names(df)) return(rep("", nrow(df)))
  stringr::str_to_upper(dplyr::coalesce(df[[col]], ""))
}

#' Identify rows that represent totals across race/ethnicity or gender.
#'
#' The teacher TXT extracts follow the same reporting-category conventions as
#' the student suspension files. We therefore treat the `TA` reporting category,
#' "All"/"Total" descriptors, and gender code `ALL` as total rows.
teacher_is_total_row <- function(df) {
  total_tokens <- c("ta", "total", "all students", "all staff", "all teachers", "all")
  rc      <- teacher_pull_lower(df, "reporting_category")
  rc_desc <- teacher_pull_lower(df, "reporting_category_description")
  race    <- teacher_pull_lower(df, "race_ethnicity")
  subgroup<- teacher_pull_lower(df, "subgroup")
  gender  <- teacher_pull_upper(df, "staff_gender_code")
  rc %in% total_tokens |
    rc_desc %in% total_tokens |
    race %in% total_tokens |
    subgroup %in% total_tokens |
    gender %in% c("ALL", "TA")
}

#' Map staff gender codes to readable labels.
teacher_gender_label <- function(code, fallback = NA_character_) {
  code_upper <- stringr::str_to_upper(dplyr::coalesce(code, ""))
  mapped <- dplyr::case_when(
    code_upper %in% c("GF", "F") ~ "Female",
    code_upper %in% c("GM", "M") ~ "Male",
    code_upper == "GX"             ~ "Non-Binary",
    code_upper == "GZ"             ~ "Gender Missing",
    code_upper == "ALL"            ~ "All Staff",
    TRUE                           ~ NA_character_
  )
  dplyr::coalesce(mapped, fallback)
}

#' Convert arbitrary text to a safe snake_case slug.
teacher_slugify <- function(x) {
  slug <- stringr::str_squish(dplyr::coalesce(as.character(x), ""))
  slug <- ifelse(nzchar(slug), slug, "unknown")
  slug <- stringr::str_to_lower(slug)
  slug <- stringr::str_replace_all(slug, "[^a-z0-9]+", "_")
  stringr::str_replace_all(slug, "^_|_$", "")
}

#' Identify numeric-like columns in a character data frame.
teacher_numeric_like <- function(x) {
  if (is.list(x)) return(FALSE)
  if (is.numeric(x)) return(TRUE)
  vals <- x[!is.na(x)]
  if (!length(vals)) return(TRUE)
  vals_chr <- as.character(vals)
  vals_chr <- vals_chr[nzchar(vals_chr)]
  if (!length(vals_chr)) return(TRUE)
  parsed <- suppressWarnings(readr::parse_number(vals_chr))
  mean(is.na(parsed)) < 0.2
}

#' Determine which value columns should be aggregated.
teacher_value_columns <- function(df) {
  numeric_names <- names(df)[vapply(df, is.numeric, logical(1))]
  setdiff(unique(numeric_names), c("year"))
}

teacher_safe_div <- function(num, den) {
  ifelse(is.na(den) | den == 0, NA_real_, num / den)
}

#' Summarise long-form teacher demographics into school-year wide metrics.
#'
#' @param df Long-form teacher data (one row per campus/year/race/gender).
#' @param value_cols Optional vector of numeric columns to aggregate. When
#'   omitted, all numeric columns are included.
#' @return A wide tibble with totals, race shares, and gender shares.
teacher_summarise_long <- function(df, value_cols = NULL) {
  stopifnot("academic_year" %in% names(df))
  stopifnot("cds_school" %in% names(df))

  if (is.null(value_cols)) value_cols <- teacher_value_columns(df)
  if (!length(value_cols)) {
    keys <- intersect(c("academic_year", "cds_school", "school_code",
                        "county_code", "district_code", "year",
                        "aggregate_level", "charter_yn"), names(df))
    return(dplyr::distinct(df, dplyr::across(dplyr::all_of(keys))))
  }

  key_cols <- intersect(c("academic_year", "cds_school", "school_code",
                           "county_code", "district_code", "year",
                           "aggregate_level", "charter_yn"), names(df))
  if (!length(key_cols)) stop("teacher_summarise_long: no joinable key columns present")

  total_mask <- teacher_is_total_row(df)
  totals_src <- if (any(total_mask, na.rm = TRUE)) df[total_mask, , drop = FALSE] else df
  totals <- totals_src %>%
    dplyr::group_by(dplyr::across(dplyr::all_of(key_cols))) %>%
    dplyr::summarise(dplyr::across(dplyr::all_of(value_cols), ~ sum(.x, na.rm = TRUE)),
                     .groups = "drop") %>%
    dplyr::rename_with(~ paste0("teacher_", ., "_total"), dplyr::all_of(value_cols))

  race_label <- dplyr::coalesce(df$reporting_category_description, df$race_ethnicity)
  if (!"race_ethnicity" %in% names(df)) {
    df$race_ethnicity <- race_label
  }

  race_tbl <- df %>%
    dplyr::mutate(race_label = dplyr::coalesce(reporting_category_description, race_ethnicity),
                  race_label = ifelse(is.na(race_label) | !nzchar(race_label), "Unknown", race_label),
                  race_slug = teacher_slugify(race_label)) %>%
    dplyr::filter(!race_slug %in% c("total", "all", "all_students", "all_staff")) %>%
    dplyr::group_by(dplyr::across(dplyr::all_of(c(key_cols, "race_slug")))) %>%
    dplyr::summarise(dplyr::across(dplyr::all_of(value_cols), ~ sum(.x, na.rm = TRUE)),
                     .groups = "drop") %>%
    tidyr::pivot_wider(
      id_cols = dplyr::all_of(key_cols),
      names_from = race_slug,
      values_from = dplyr::all_of(value_cols),
      names_glue = "teacher_{.value}_{race_slug}",
      values_fill = 0
    )

  gender_tbl <- if ("staff_gender_code" %in% names(df) || "staff_gender" %in% names(df)) {
    df %>%
      dplyr::mutate(gender_code = stringr::str_to_upper(dplyr::coalesce(staff_gender_code, "")),
                    gender_label = teacher_gender_label(gender_code,
                                                        if ("staff_gender" %in% names(df)) staff_gender else NA_character_),
                    gender_slug = teacher_slugify(gender_label)) %>%
      dplyr::filter(!gender_slug %in% c("all_staff", "all", "total")) %>%
      dplyr::group_by(dplyr::across(dplyr::all_of(c(key_cols, "gender_slug")))) %>%
      dplyr::summarise(dplyr::across(dplyr::all_of(value_cols), ~ sum(.x, na.rm = TRUE)),
                       .groups = "drop") %>%
      tidyr::pivot_wider(
        id_cols = dplyr::all_of(key_cols),
        names_from = gender_slug,
        values_from = dplyr::all_of(value_cols),
        names_glue = "teacher_{.value}_by_gender_{gender_slug}",
        values_fill = 0
      )
  } else {
    NULL
  }

  summary <- totals %>%
    dplyr::left_join(race_tbl, by = key_cols)

  if (!is.null(gender_tbl)) {
    summary <- summary %>% dplyr::left_join(gender_tbl, by = key_cols)
  }

  total_cols <- grep("^teacher_.*_total$", names(summary), value = TRUE)
  for (tc in total_cols) {
    metric <- stringr::str_match(tc, "^teacher_(.*)_total$")[, 2]
    if (is.na(metric)) next
    race_cols <- grep(paste0("^teacher_", metric, "_(?!total)(?!by_gender_).+"),
                      names(summary), value = TRUE, perl = TRUE)
    gender_cols <- grep(paste0("^teacher_", metric, "_by_gender_.+"),
                        names(summary), value = TRUE)
    for (col in c(race_cols, gender_cols)) {
      share_col <- paste0(col, "_share")
      summary[[share_col]] <- teacher_safe_div(summary[[col]], summary[[tc]])
    }
  }

  summary
}

# End of file ------------------------------------------------------------------
