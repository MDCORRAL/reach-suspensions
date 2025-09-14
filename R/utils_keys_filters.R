# ---- R/utils_keys_filters.R --------------------------------------------------
suppressPackageStartupMessages({
  library(dplyr); library(stringr); library(rlang)
})

SPECIAL_SCHOOL_CODES <- c("0000000", "0000001")

# ---- School level classification ---------------------------------------------
#' Canonical grade-span labels and helpers for school-level classification.
#'
#' `LEVEL_LABELS` enumerates the accepted school level categories.
#' `span_label()` maps numeric grade bounds to those labels.
#' `is_alt()` returns TRUE when a `school_type` string denotes an alternative program.
#' Source this file to access these helpers in downstream scripts.
LEVEL_LABELS <- c("Elementary", "Middle", "High", "Other", "Alternative")

#' Consistent color palette for grade levels
pal_level <- c(
  Elementary = "#1b9e77",
  Middle     = "#d95f02",
  High       = "#7570b3",
  Other      = "#e7298a",
  Alternative= "#66a61e"
)

span_label <- function(gmin, gmax) {
  if (is.na(gmin) || is.na(gmax)) return("Other")
  if (gmin <= 0 && gmax >= 12) return("Other")
  if (gmax <= 5) return("Elementary")
  if (gmin >= 6 && gmax <= 8) return("Middle")
  if (gmax >= 9) return("High")
  if (gmin <= 0 && gmax <= 8) return("Elementary")
  "Other"
}

is_alt <- function(school_type) {
  st <- tolower(ifelse(is.na(school_type), "", school_type))
  str_detect(st, "juvenile court|community day|alternative|continuation")
}

# ---- Locale reference ---------------------------------------------------------
#' Canonical locale levels and color palette.
#'
#' `locale_levels` enumerates the only accepted locale strings, in plotting
#' order. `pal_locale` maps those levels to a consistent color palette.
#' To add or modify locales, edit these objects here rather than creating
#' ad-hoc strings or palettes elsewhere. All scripts should source this file
#' to access `locale_levels` and `pal_locale`.
locale_levels <- c("City", "Suburban", "Town", "Rural", "Unknown")
pal_locale <- c(
  City     = "#0072B2",
  Suburban = "#009E73",
  Town     = "#E69F00",
  Rural    = "#D55E00",
  Unknown  = "#7F7F7F"
)

##
# mapping from raw reason keys to display labels
reason_labels <- dplyr::tibble(
  reason = c(
    "violent_injury",
    "violent_no_injury",
    "weapons_possession",
    "illicit_drug",
    "defiance_only",
    "other_reasons"
  ),
  reason_lab = c(
    "Violent (Injury)",
    "Violent (No Injury)",
    "Weapons",
    "Illicit Drugs",
    "Willful Defiance",
    "Other"
  )
)

# consistent color palette for suspension reasons
pal_reason <- setNames(
  c("#d62728", "#ff7f0e", "#2ca02c", "#1f77b4", "#9467bd", "#8c564b"),
  reason_labels$reason_lab
)

# helper to append readable reason labels
add_reason_label <- function(df, reason_col = "reason") {
  reason_sym <- rlang::sym(reason_col)

  # identify unexpected reason codes and stop early
  unmatched <- setdiff(unique(df[[reason_col]]), reason_labels$reason)
  unmatched <- unmatched[!is.na(unmatched)]
  if (length(unmatched) > 0) {
    stop(
      sprintf(
        "Unexpected reason codes: %s",
        paste(unmatched, collapse = ", ")
      )
    )
  }

  dplyr::left_join(df, reason_labels, by = setNames("reason", reason_col)) %>%
    dplyr::mutate(
      !!reason_sym := factor(!!reason_sym, levels = reason_labels$reason),
      reason_lab   = factor(reason_lab, levels = reason_labels$reason_lab)
    )
}

# Ensure county, district, and school codes are present.
# If they are missing but a 14-digit `cds_school` key exists, split it into
# components. Otherwise, halt with a helpful message so upstream data can be
# rebuilt.
ensure_keys <- function(df) {
  needed <- c("county_code", "district_code", "school_code")
  if (!all(needed %in% names(df))) {
    if ("cds_school" %in% names(df)) {
      df <- df %>%
        mutate(
          county_code   = substr(cds_school, 1, 2),
          district_code = substr(cds_school, 3, 7),
          school_code   = substr(cds_school, 8, 14)
        )
    }
  }
  if (!all(needed %in% names(df))) {
    stop("Missing county/district/school codes. Rebuild data-stage/susp_v6_long.parquet.")
  }
  df
}

# build canonical 14-digit CDS keys
build_keys <- function(df) {
  df <- ensure_keys(df)
  df %>%
    mutate(
      county_code  = str_pad(as.character(county_code),  width = 2,  pad = "0"),
      district_code= str_pad(as.character(district_code),width = 5,  pad = "0"),
      school_code  = str_pad(as.character(school_code),  width = 7,  pad = "0"),
      cds_district = paste0(county_code, district_code),            # 7-digit district
      cds_school   = paste0(county_code, district_code, school_code) # 14-digit campus
    )
}

# strict campus-only filter (excludes special rows that mimic schools)
filter_campus_only <- function(df) {
  df %>%
    filter(
      tolower(aggregate_level) %in% c("s", "school"),
      !school_code %in% SPECIAL_SCHOOL_CODES
    )
}

# roll nonpublic (0000001) to district-level aggregates
roll_nonpublic_to_district <- function(df, value_cols, year_col = "year") {
  ysym <- rlang::sym(year_col)
  np <- df %>%
    dplyr::filter(school_code == "0000001") %>%
    dplyr::group_by(county_code, district_code, !!ysym) %>%
    dplyr::summarise(dplyr::across(dplyr::all_of(value_cols), ~ sum(.x, na.rm = TRUE)),
                     .groups = "drop") %>%
    dplyr::mutate(aggregate_level = "district", school_code = NA_character_)
  dplyr::bind_rows(
    df %>% dplyr::filter(school_code != "0000001"),
    np
  )
}

# assert uniqueness for a campus-level frame
assert_unique_campus <- function(df, campus_col = "cds_school", year_col = "year", extra_keys = character()) {
  ysym <- rlang::sym(year_col)
  key_syms <- rlang::syms(c(campus_col, extra_keys))
  dup <- df %>%
    dplyr::count(!!!key_syms, !!ysym, name = "n") %>%
    dplyr::filter(n > 1)
  if (nrow(dup) > 0) {
    stop("Duplicate campus-year keys found. Inspect 'dup' in the calling frame.")
  }
  df
}

# assert uniqueness for a district-level frame
# (function intentionally left for future implementation)

#############
# Map various race/ethnicity inputs to canonical labels. Accepts either
# legacy reporting-category codes (e.g., "RB") or descriptive subgroup
# names (e.g., "Black").
canon_race_label <- function(x) {
  x_clean <- stringr::str_to_lower(stringr::str_trim(x))
  dplyr::case_when(
    x_clean %in% c("ta", "total", "all students", "all_students") ~ "All Students",
    x_clean %in% c("ra", "asian") ~ "Asian",
    x_clean %in% c(
      "rb", "black", "african american", "black/african american",
      "african_american"
    ) ~ "Black/African American",
    x_clean %in% c("rf", "filipino") ~ "Filipino",
    x_clean %in% c(
      "rh", "rl", "hispanic", "latino", "hispanic/latino",
      "hispanic_latino"
    ) ~ "Hispanic/Latino",
    x_clean %in% c(
      "ri", "american indian", "alaska native",
      "american indian/alaska native", "native american"
    ) ~ "American Indian/Alaska Native",
    x_clean %in% c("rp", "pacific islander", "native hawaiian") ~ "Native Hawaiian/Pacific Islander",
    x_clean %in% c(
      "rt", "two or more", "two or more races", "multirace",
      "multiple"
    ) ~ "Two or More Races",
    x_clean %in% c("rw", "white") ~ "White",


 # Map CRDC code RD and similar strings to the canonical "Not Reported" label

    x_clean %in% c("rd", "not reported", "not_reported", "notreported") ~ "Not Reported",

    stringr::str_detect(x_clean, "gender|male|female") ~ "Sex",
    TRUE ~ NA_character_
  )
}

#' Canonical race labels referenced across analysis scripts.
#'
#' Use `ALLOWED_RACES` when filtering or subsetting by race instead of
#' hard-coding vectors in individual scripts. Combine with helpers like
#' `intersect()` or `setdiff()` to derive custom subsets, e.g.:
#'
#' ```r
#' core_races <- intersect(ALLOWED_RACES,
#'                         c("Black/African American", "White"))
#' ```
#'
#' "Not Reported" is mapped by `canon_race_label()` but intentionally omitted
#' from this set to encourage explicit handling of missing race data.
ALLOWED_RACES <- c(
  "All Students",
  "Black/African American",
  "White",
  "Hispanic/Latino",
  "American Indian/Alaska Native",
  "Asian",
  "Filipino",
  "Native Hawaiian/Pacific Islander",
  "Two or More Races" # "Not Reported" intentionally excluded; treat missing race separately
)

###############
# construct standardized quartile labels like "Q1 (Lowest % Black)"
get_quartile_label <- function(q4, race = c("Black", "White")) {
  race <- match.arg(race)
  dplyr::case_when(
    is.na(q4) ~ "Unknown",
    q4 == 1L ~ paste0("Q1 (Lowest % ", race, ")"),
    q4 == 2L ~ "Q2",
    q4 == 3L ~ "Q3",
    q4 == 4L ~ paste0("Q4 (Highest % ", race, ")")
  )
}
