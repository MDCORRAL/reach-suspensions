# graph_scripts/graph_utils.R
# Shared helpers for suspension rate graphics.

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(forcats)
  library(glue)
  library(here)
  library(janitor)
  library(scales)
  library(stringr)
  library(tidyr)
})

DATA_STAGE <- here::here("data-stage")
SUSP_PATH  <- file.path(DATA_STAGE, "susp_v6_long.parquet")
FEAT_PATH  <- file.path(DATA_STAGE, "susp_v6_features.parquet")
OUTPUT_DIR <- here::here("outputs", "graphs")
TEXT_DIR   <- file.path(OUTPUT_DIR, "descriptions")

dir.create(OUTPUT_DIR, recursive = TRUE, showWarnings = FALSE)
dir.create(TEXT_DIR,   recursive = TRUE, showWarnings = FALSE)

SPECIAL_SCHOOL_CODES <- c("0000000", "0000001")
DEFAULT_IS_TRADITIONAL <- TRUE
SETTINGS_TO_INCLUDE <- c("Traditional")  # set to NULL to keep all settings
DROP_UNKNOWN_QUARTILES <- FALSE

race_levels <- c(
  "Black/African American",
  "Hispanic/Latino",
  "White",
  "Asian",
  "American Indian/Alaska Native",
  "Native Hawaiian/Pacific Islander",
  "Filipino",
  "Two or More Races"
)

race_palette <- c(
  "Black/African American"          = "#D62828",
  "Hispanic/Latino"                 = "#F77F00",
  "White"                           = "#003049",
  "Asian"                           = "#2A9D8F",
  "American Indian/Alaska Native"   = "#8E7DBE",
  "Native Hawaiian/Pacific Islander"= "#577590",
  "Filipino"                        = "#E9C46A",
  "Two or More Races"               = "#588157"
)

comparison_palette <- c(
  "Statewide Traditional Average"   = "#3B6FB6",
  "Q4: Highest Black Enrollment"    = "#D1495B"
)

setting_palette <- c(
  "All Traditional Schools"         = "#3B6FB6",

  "Elementary Traditional Schools"  = "#F4A261",
  "Middle Traditional Schools"      = "#2A9D8F",
  "High Traditional Schools"        = "#D1495B"

)

safe_div <- function(n, d) ifelse(is.na(d) | d == 0, NA_real_, n / d)

standardize_quartile_label <- function(x, group = "Black") {
  x <- as.character(x)
  group <- as.character(group)
  high_label <- glue::glue("Q4 (Highest % {group})")
  low_label  <- glue::glue("Q1 (Lowest % {group})")

  dplyr::case_when(
    stringr::str_detect(x, stringr::regex("^q1", ignore_case = TRUE)) ~ low_label,
    stringr::str_detect(x, stringr::regex("^q2", ignore_case = TRUE)) ~ "Q2",
    stringr::str_detect(x, stringr::regex("^q3", ignore_case = TRUE)) ~ "Q3",
    stringr::str_detect(x, stringr::regex("^q4", ignore_case = TRUE)) ~ high_label,
    TRUE ~ NA_character_
  )
}

load_joined_data <- function() {
  stopifnot(file.exists(SUSP_PATH), file.exists(FEAT_PATH))

  cols_needed <- c(
    "school_code", "academic_year", "subgroup",
    "cumulative_enrollment", "total_suspensions",
    "school_level", "school_type", "school_locale", "locale_simple",
    "black_prop_q", "black_prop_q_label",
    "white_prop_q", "white_prop_q_label",
    "hispanic_prop_q", "hispanic_prop_q_label",
    "aggregate_level"
  )

  susp <- arrow::read_parquet(SUSP_PATH, col_select = cols_needed) %>%
    janitor::clean_names() %>%
    dplyr::mutate(
      aggregate_level = stringr::str_to_lower(as.character(aggregate_level)),
      school_code = stringr::str_pad(as.character(school_code), width = 7, pad = "0"),
      academic_year = as.character(academic_year),
      subgroup = as.character(subgroup),
      cumulative_enrollment = suppressWarnings(as.numeric(cumulative_enrollment)),
      total_suspensions = suppressWarnings(as.numeric(total_suspensions)),
      school_level = as.character(school_level),
      school_type = as.character(school_type),
      school_locale = as.character(school_locale),
      locale_simple = as.character(locale_simple),
      black_prop_q = suppressWarnings(as.integer(black_prop_q)),
      white_prop_q = suppressWarnings(as.integer(white_prop_q)),
      hispanic_prop_q = suppressWarnings(as.integer(hispanic_prop_q)),
      black_prop_q_label = standardize_quartile_label(black_prop_q_label, "Black"),
      white_prop_q_label = standardize_quartile_label(white_prop_q_label, "White"),
      hispanic_prop_q_label = standardize_quartile_label(hispanic_prop_q_label, "Hispanic/Latino")
    ) %>%
    dplyr::filter(
      aggregate_level %in% c("s", "school"),
      !school_code %in% SPECIAL_SCHOOL_CODES
    ) %>%
    dplyr::select(-aggregate_level)

  feat <- arrow::read_parquet(FEAT_PATH) %>%
    janitor::clean_names() %>%
    dplyr::transmute(
      school_code = as.character(school_code),
      academic_year = as.character(academic_year),
      is_traditional = as.logical(is_traditional)
    )

  joined <- susp %>%
    dplyr::left_join(feat, by = c("school_code", "academic_year")) %>%
    dplyr::mutate(
      is_traditional = dplyr::case_when(
        !is.na(is_traditional) ~ as.logical(is_traditional),
        TRUE ~ DEFAULT_IS_TRADITIONAL
      ),
      setting = ifelse(is_traditional, "Traditional", "Non-traditional")
    )

  if (DROP_UNKNOWN_QUARTILES) {
    joined <- joined %>%
      dplyr::filter(
        !is.na(black_prop_q_label),
        !is.na(white_prop_q_label),
        !is.na(hispanic_prop_q_label)
      )
  } else {
    joined <- joined %>%
      tidyr::replace_na(list(
        black_prop_q_label = "Unknown",
        white_prop_q_label = "Unknown",
        hispanic_prop_q_label = "Unknown"
      ))
  }

  if (!is.null(SETTINGS_TO_INCLUDE) && length(SETTINGS_TO_INCLUDE) > 0) {
    joined <- joined %>% dplyr::filter(setting %in% SETTINGS_TO_INCLUDE)
  }

  joined
}

factorize_race <- function(x) forcats::fct_relevel(factor(x), race_levels)

theme_reach <- function(base_size = 12, base_family = NULL) {
  ggplot2::theme_minimal(base_size = base_size, base_family = base_family) +
    ggplot2::theme(
      panel.grid.major = ggplot2::element_line(color = "#DFE2E5", linewidth = 0.4),
      panel.grid.minor = ggplot2::element_blank(),
      plot.title = ggplot2::element_text(face = "bold", size = base_size + 4, hjust = 0),
      plot.subtitle = ggplot2::element_text(size = base_size + 0, margin = ggplot2::margin(b = 10)),
      plot.caption = ggplot2::element_text(size = base_size - 2, color = "#5A5A5A", hjust = 0),
      axis.text.x = ggplot2::element_text(angle = 45, hjust = 1),
      legend.title = ggplot2::element_text(face = "bold"),
      legend.position = "bottom",
      plot.background = ggplot2::element_rect(fill = "white", color = NA),
      panel.background = ggplot2::element_rect(fill = "white", color = NA),
      plot.margin = ggplot2::margin(20, 30, 20, 20)
    )
}

latest_year_available <- function(years) {
  yrs <- sort(unique(as.character(years)))
  if (length(yrs) == 0) NA_character_ else tail(yrs, 1)
}

write_description <- function(text, filename) {
  full_path <- file.path(TEXT_DIR, filename)
  dir.create(dirname(full_path), recursive = TRUE, showWarnings = FALSE)
  writeLines(text, full_path, useBytes = TRUE)
  invisible(full_path)
}
