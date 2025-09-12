# ---- R/utils_keys_filters.R --------------------------------------------------
suppressPackageStartupMessages({
  library(dplyr); library(stringr); library(rlang)
})

SPECIAL_SCHOOL_CODES <- c("0000000", "0000001")

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
    "Illicit Drug",
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
  dplyr::left_join(df, reason_labels, by = setNames("reason", reason_col)) %>%
    dplyr::mutate(
      !!reason_sym := factor(!!reason_sym, levels = reason_labels$reason),
      reason_lab   = factor(reason_lab, levels = reason_labels$reason_lab)
    )
}

# build canonical 14-digit CDS keys
build_keys <- function(df) {
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
assert_unique_campus <- function(df, year_col = "year", extra_keys = character()) {
  ysym <- rlang::sym(year_col)
  key_syms <- rlang::syms(c("cds_school", extra_keys))
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
#codex/remove-obsolete-race_label-function

# map CRDC race codes to descriptive labels
race_label <- function(code) dplyr::recode(
  code,
  RB = "Black/African American", RW = "White",
  RH = "Hispanic/Latino", RL = "Hispanic/Latino",
  RI = "American Indian/Alaska Native", RA = "Asian",
  RF = "Filipino", RP = "Native Hawaiian/Pacific Islander",
  RT = "Two or More Races", TA = "All Students",
  .default = NA_character_
)

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
    stringr::str_detect(x_clean, "gender|male|female") ~ "Sex",
    TRUE ~ NA_character_
  )
}
#codex/remove-obsolete-race_label-function

# Canonical race labels referenced across analysis scripts
ALLOWED_RACES <- c(
  "All Students",
  "Black/African American",
  "White",
  "Hispanic/Latino",
  "American Indian/Alaska Native",
  "Asian",
  "Filipino",
  "Native Hawaiian/Pacific Islander",
  "Two or More Races"
)

# Backward-compatible alias used by legacy scripts
race_label <- canon_race_label
##main
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
