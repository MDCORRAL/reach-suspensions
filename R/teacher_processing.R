# R/teacher_processing.R -- helpers for teacher demographic ingestion/merging

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(stringr)
  library(tidyr)
  library(tibble)
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

#' Map reporting_category codes to readable labels.
teacher_reporting_category_label <- function(code, fallback = NA_character_) {
  code_upper <- stringr::str_to_upper(dplyr::coalesce(code, ""))
  mapped <- dplyr::case_when(
    code_upper == "ALL" ~ "All Staff",
    code_upper == "TCH" ~ "Teachers",
    code_upper == "ADM" ~ "Administrators",
    code_upper == "PSV" ~ "Pupil Services",
    code_upper == "OTH" ~ "Other Staff",
    TRUE                ~ NA_character_
  )
  dplyr::coalesce(mapped, fallback)
}

#' Map reporting_category codes to snake_case slugs.
teacher_reporting_category_slug <- function(code, fallback = "unknown") {
  label <- teacher_reporting_category_label(code, fallback = NA_character_)
  raw   <- dplyr::coalesce(label, code, fallback)
  teacher_slugify(raw)
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

#' Lookup table mapping known race suffixes to reporting metadata.
teacher_race_suffix_lookup <- function() {
  tibble::tibble(
    race_suffix = c(
      "african_american",
      "american_indian_or_alaska_native",
      "asian",
      "filipino",
      "hispanic_or_latino",
      "pacific_islander",
      "white",
      "two_or_more_races",
      "not_reported"
    ),
    reporting_category_code = c("RB", "RI", "RA", "RF", "RH", "RP", "RW", "RT", "RD"),
    race_ethnicity_label = c(
      "African American",
      "American Indian or Alaska Native",
      "Asian",
      "Filipino",
      "Hispanic or Latino",
      "Native Hawaiian/Pacific Islander",
      "White",
      "Two or More Races",
      "Not Reported"
    )
  )
}

#' Convert wide race-suffixed columns into long form.
#'
#' @param df Data frame containing numeric columns encoded as
#'   `<metric>_<race_suffix>` (or just `<race_suffix>`).
#' @return Data frame with one row per race suffix and metric columns widened.
teacher_longify_wide_counts <- function(df) {
  lookup <- teacher_race_suffix_lookup()
  suffixes <- lookup$race_suffix
  pattern <- paste0("^(.*?)(?:_)?(", paste(suffixes, collapse = "|"), ")$")
  race_cols <- grep(pattern, names(df), value = TRUE)
  if (!length(race_cols)) return(df)

  base_cols <- setdiff(names(df), race_cols)
  existing_race <- "race_ethnicity" %in% base_cols
  id_cols <- unique(c(if (existing_race) base_cols[base_cols != "race_ethnicity"] else base_cols,
                      "race_ethnicity"))

  long_df <- df %>%
    tidyr::pivot_longer(
      cols = dplyr::all_of(race_cols),
      names_to = c("metric", "race_suffix"),
      names_pattern = pattern,
      values_to = "value",
      values_drop_na = FALSE
    ) %>%
    dplyr::mutate(
      metric = dplyr::if_else(metric == "" | metric == "_", "staff_count", stringr::str_remove(metric, "^_"))
    ) %>%
    dplyr::left_join(lookup, by = "race_suffix")

  missing_cols <- setdiff(c("reporting_category", "reporting_category_description", "race_ethnicity"), names(long_df))
  for (col in missing_cols) {
    long_df[[col]] <- rep(NA_character_, nrow(long_df))
  }

  long_df %>%
    dplyr::mutate(
      reporting_category = dplyr::coalesce(reporting_category, reporting_category_code),
      reporting_category_description = dplyr::coalesce(
        reporting_category_description, race_ethnicity_label
      ),
      race_ethnicity = dplyr::coalesce(race_ethnicity, race_ethnicity_label)
    ) %>%
    dplyr::select(-race_suffix, -reporting_category_code, -race_ethnicity_label) %>%
    tidyr::pivot_wider(
      id_cols = dplyr::all_of(id_cols),
      names_from = metric,
      values_from = value
    )
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

  has_staff_type <- "reporting_category" %in% names(df)
  if (has_staff_type) {
    df <- df %>%
      mutate(
        reporting_category = stringr::str_to_upper(dplyr::coalesce(reporting_category, "")),
        reporting_category = ifelse(nzchar(reporting_category), reporting_category, NA_character_),
        reporting_category_label = teacher_reporting_category_label(reporting_category, reporting_category),
        reporting_category_slug  = teacher_reporting_category_slug(reporting_category, fallback = "unknown_staff_type")
      )
  }

  total_mask <- teacher_is_total_row(df)
  total_mask <- total_mask %in% TRUE
  df$.teacher_total_row <- total_mask
  totals_src <- if (any(df$.teacher_total_row, na.rm = TRUE)) df[df$.teacher_total_row, , drop = FALSE] else df
  totals <- totals_src %>%
    dplyr::group_by(dplyr::across(dplyr::all_of(key_cols))) %>%
    dplyr::summarise(dplyr::across(dplyr::all_of(value_cols), ~ sum(.x, na.rm = TRUE)),
                     .groups = "drop") %>%
    dplyr::rename_with(~ paste0("teacher_", ., "_total"), dplyr::all_of(value_cols))

  totals_by_type <- NULL
  if (has_staff_type) {
    totals_by_type <- df %>%
      dplyr::filter(!is.na(reporting_category_slug)) %>%
      dplyr::group_by(dplyr::across(dplyr::all_of(c(key_cols, "reporting_category_slug")))) %>%
      dplyr::filter(if (any(.teacher_total_row, na.rm = TRUE)) .teacher_total_row else TRUE) %>%
      dplyr::summarise(dplyr::across(dplyr::all_of(value_cols), ~ sum(.x, na.rm = TRUE)),
                       .groups = "drop") %>%
      tidyr::pivot_wider(
        id_cols    = dplyr::all_of(key_cols),
        names_from = reporting_category_slug,
        values_from = dplyr::all_of(value_cols),
        names_glue = "teacher_{.value}_total_by_type_{reporting_category_slug}",
        values_fill = 0
      )
  }

  df$.teacher_total_row <- NULL

  # Create reporting_category_description if it doesn't exist
  if (!"reporting_category_description" %in% names(df)) {
    if ("reporting_category" %in% names(df)) {
      df$reporting_category_description <- teacher_reporting_category_label(df$reporting_category)
    } else {
      df$reporting_category_description <- NA_character_
    }
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

  race_by_type_tbl <- NULL
  if (has_staff_type) {
    race_by_type_tbl <- df %>%
      dplyr::filter(!is.na(reporting_category_slug)) %>%
      dplyr::mutate(race_label = dplyr::coalesce(reporting_category_description, race_ethnicity),
                    race_label = ifelse(is.na(race_label) | !nzchar(race_label), "Unknown", race_label),
                    race_slug = teacher_slugify(race_label)) %>%
      dplyr::filter(!race_slug %in% c("total", "all", "all_students", "all_staff")) %>%
      dplyr::group_by(dplyr::across(dplyr::all_of(c(key_cols, "reporting_category_slug", "race_slug")))) %>%
      dplyr::summarise(dplyr::across(dplyr::all_of(value_cols), ~ sum(.x, na.rm = TRUE)),
                       .groups = "drop") %>%
      tidyr::pivot_wider(
        id_cols    = dplyr::all_of(key_cols),
        names_from = c(reporting_category_slug, race_slug),
        values_from = dplyr::all_of(value_cols),
        names_glue = "teacher_{.value}_by_type_{reporting_category_slug}_{race_slug}",
        values_fill = 0
      )
  }

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

  gender_by_type_tbl <- NULL
  if (has_staff_type && !is.null(gender_tbl)) {
    gender_by_type_tbl <- df %>%
      dplyr::filter(!is.na(reporting_category_slug)) %>%
      dplyr::mutate(gender_code = stringr::str_to_upper(dplyr::coalesce(staff_gender_code, "")),
                    gender_label = teacher_gender_label(gender_code,
                                                        if ("staff_gender" %in% names(df)) staff_gender else NA_character_),
                    gender_slug = teacher_slugify(gender_label)) %>%
      dplyr::filter(!gender_slug %in% c("all_staff", "all", "total")) %>%
      dplyr::group_by(dplyr::across(dplyr::all_of(c(key_cols, "reporting_category_slug", "gender_slug")))) %>%
      dplyr::summarise(dplyr::across(dplyr::all_of(value_cols), ~ sum(.x, na.rm = TRUE)),
                       .groups = "drop") %>%
      tidyr::pivot_wider(
        id_cols    = dplyr::all_of(key_cols),
        names_from = c(reporting_category_slug, gender_slug),
        values_from = dplyr::all_of(value_cols),
        names_glue = "teacher_{.value}_by_type_{reporting_category_slug}_by_gender_{gender_slug}",
        values_fill = 0
      )
  }

  summary <- totals %>%
    dplyr::left_join(race_tbl, by = key_cols)

  if (!is.null(gender_tbl)) {
    summary <- summary %>% dplyr::left_join(gender_tbl, by = key_cols)
  }

  if (!is.null(totals_by_type)) {
    summary <- summary %>% dplyr::left_join(totals_by_type, by = key_cols)
  }

  if (!is.null(race_by_type_tbl)) {
    summary <- summary %>% dplyr::left_join(race_by_type_tbl, by = key_cols)
  }

  if (!is.null(gender_by_type_tbl)) {
    summary <- summary %>% dplyr::left_join(gender_by_type_tbl, by = key_cols)
  }

  total_cols <- grep("^teacher_.*_total$", names(summary), value = TRUE)
  for (tc in total_cols) {
    metric <- stringr::str_match(tc, "^teacher_(.*)_total$")[, 2]
    if (is.na(metric)) next
    race_cols <- grep(paste0("^teacher_", metric, "_(?!total)(?!by_gender_)(?!by_type_).+"),
                      names(summary), value = TRUE, perl = TRUE)
    gender_cols <- grep(paste0("^teacher_", metric, "_by_gender_.+"),
                        names(summary), value = TRUE)
    for (col in c(race_cols, gender_cols)) {
      share_col <- paste0(col, "_share")
      summary[[share_col]] <- teacher_safe_div(summary[[col]], summary[[tc]])
    }
  }

  total_cols_by_type <- grep("^teacher_.*_total_by_type_.+$", names(summary), value = TRUE)
  for (tc in total_cols_by_type) {
    parts <- stringr::str_match(tc, "^teacher_(.*)_total_by_type_(.+)$")
    metric <- parts[, 2]
    type_slug <- parts[, 3]
    if (is.na(metric) || is.na(type_slug)) next
    race_cols <- grep(paste0("^teacher_", metric, "_by_type_", type_slug, "_(?!total)(?!share).+"),
                      names(summary), value = TRUE, perl = TRUE)
    gender_cols <- grep(paste0("^teacher_", metric, "_by_type_", type_slug, "_by_gender_.+"),
                        names(summary), value = TRUE)
    for (col in c(race_cols, gender_cols)) {
      share_col <- paste0(col, "_share")
      summary[[share_col]] <- teacher_safe_div(summary[[col]], summary[[tc]])
    }
  }

  summary
}

# End of file ------------------------------------------------------------------
