# ---- R/utils_keys_filters.R --------------------------------------------------
suppressPackageStartupMessages({
  library(dplyr); library(stringr); library(rlang)
})

SPECIAL_SCHOOL_CODES <- c("0000000", "0000001")

# Consistent locale ordering and color palette used across analyses
locale_levels <- c("City", "Suburban", "Town", "Rural", "Unknown")
pal_locale <- c(
  City     = "#0072B2",
  Suburban = "#009E73",
  Town     = "#E69F00",
  Rural    = "#D55E00",
  Unknown  = "#7F7F7F"
)

=======
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

# helper to append readable reason labels
add_reason_label <- function(df, reason_col = "reason") {
  dplyr::left_join(df, reason_labels, by = setNames("reason", reason_col))
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

# map CRDC race codes to descriptive labels
race_label <- function(code) dplyr::recode(
  code,
  RB = "Black/African American", RW = "White",
  RH = "Hispanic/Latino", RL = "Hispanic/Latino",
  RI = "American Indian/Alaska Native", RA = "Asian",
  RF = "Filipino", RP = "Pacific Islander",
  RT = "Two or More Races", TA = "All Students",
  .default = NA_character_
)

=======
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
