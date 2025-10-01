# graph_scripts/21_black_quartile_suspension_trends.R
# Suspension trends for Black students across enrollment quartiles.

suppressPackageStartupMessages({
  library(dplyr)
  library(ggplot2)
  library(glue)
  library(here)
  library(readr)
  library(scales)
})

source(here::here("graph_scripts", "graph_utils.R"))

quartile_palette <- c(
  "Q1" = "#0B3954",
  "Q2" = "#087E8B",
  "Q3" = "#FF5A5F",
  "Q4" = "#C81D25"
)

statewide_color <- "#D00000"
statewide_label <- "Statewide Traditional Average"

joined <- load_joined_data()

black_base <- joined %>%
  dplyr::filter(
    is_traditional,
    subgroup == "Black/African American",
    !is.na(total_suspensions),
    !is.na(cumulative_enrollment),
    cumulative_enrollment > 0
  ) %>%
  dplyr::mutate(academic_year = as.character(academic_year))

if (nrow(black_base) == 0) {
  stop("No traditional school records available for Black student quartile trends.")
}

statewide_rates <- black_base %>%
  dplyr::group_by(academic_year) %>%
  dplyr::summarise(
    suspensions = sum(total_suspensions, na.rm = TRUE),
    enrollment = sum(cumulative_enrollment, na.rm = TRUE),
    rate = safe_div(suspensions, enrollment),
    .groups = "drop"
  )

year_levels <- statewide_rates$academic_year %>% unique() %>% sort()

statewide_rates <- statewide_rates %>%
  dplyr::mutate(academic_year = factor(academic_year, levels = year_levels, ordered = TRUE))

prepare_quartile_data <- function(data, quartile_col, quartile_label_col, cohort_label) {
  data %>%
    dplyr::filter(!is.na({{ quartile_col }}), {{ quartile_col }} %in% 1:4) %>%
    dplyr::mutate(
      quartile_value = {{ quartile_col }},
      quartile = factor(glue::glue("Q{quartile_value}"), levels = names(quartile_palette), ordered = TRUE),
      quartile_label = {{ quartile_label_col }}
    ) %>%
    dplyr::group_by(academic_year, quartile, quartile_label) %>%
    dplyr::summarise(
      suspensions = sum(total_suspensions, na.rm = TRUE),
      enrollment = sum(cumulative_enrollment, na.rm = TRUE),
      rate = safe_div(suspensions, enrollment),
      .groups = "drop"
    ) %>%
    dplyr::mutate(
      academic_year = factor(academic_year, levels = year_levels, ordered = TRUE),
      cohort = cohort_label
    )
}

black_quartiles <- prepare_quartile_data(
  black_base,
  quartile_col = black_prop_q,
  quartile_label_col = black_prop_q_label,
  cohort_label = "Black enrollment quartiles"
)

white_quartiles <- prepare_quartile_data(
  black_base,
  quartile_col = white_prop_q,
  quartile_label_col = white_prop_q_label,
  cohort_label = "White enrollment quartiles"
)

if (nrow(black_quartiles) == 0) {
  stop("No Black enrollment quartile data available for Black students.")
}

if (nrow(white_quartiles) == 0) {
  stop("No White enrollment quartile data available for Black students.")
}

build_quartile_plot <- function(quartile_data, cohort_label) {
  ggplot() +
    geom_line(
      data = quartile_data,
      aes(x = academic_year, y = rate, color = quartile, group = quartile),
      linewidth = 1
    ) +
    geom_point(
      data = quartile_data,
      aes(x = academic_year, y = rate, color = quartile),
      size = 2
    ) +
    geom_line(
      data = statewide_rates,
      aes(x = academic_year, y = rate, color = statewide_label, group = statewide_label),
      linewidth = 1
    ) +
    geom_point(
      data = statewide_rates,
      aes(x = academic_year, y = rate, color = statewide_label),
      size = 2
    ) +
    scale_color_manual(
      values = c(quartile_palette, setNames(statewide_color, statewide_label)),
      breaks = c(names(quartile_palette), statewide_label),
      name = "Series"
    ) +
    scale_y_continuous(labels = scales::percent_format(accuracy = 0.1)) +
    labs(
      title = glue::glue("Black student suspension trends in {cohort_label}"),
      subtitle = "Traditional schools only; lines show suspension rates by enrollment quartile",
      x = "Academic year",
      y = "Suspension rate",
      caption = "Source: California statewide suspension data (susp_v6_long + v6 features)"
    ) +
    theme_reach() +
    theme(legend.position = "bottom")
}

black_plot <- build_quartile_plot(black_quartiles, "Black enrollment quartiles")
white_plot <- build_quartile_plot(white_quartiles, "White enrollment quartiles")

output_black_path <- file.path(OUTPUT_DIR, "black_students_black_quartile_trends.png")
output_white_path <- file.path(OUTPUT_DIR, "black_students_white_quartile_trends.png")

ggsave(output_black_path, black_plot, width = 12, height = 7, dpi = 320)
ggsave(output_white_path, white_plot, width = 12, height = 7, dpi = 320)

message("Saved Black enrollment quartile plot to ", output_black_path)
message("Saved White enrollment quartile plot to ", output_white_path)

readr::write_csv(
  black_quartiles %>%
    dplyr::select(academic_year, quartile, quartile_label, suspensions, enrollment, rate),
  file.path(OUTPUT_DIR, "black_students_black_quartile_trends.csv")
)

readr::write_csv(
  white_quartiles %>%
    dplyr::select(academic_year, quartile, quartile_label, suspensions, enrollment, rate),
  file.path(OUTPUT_DIR, "black_students_white_quartile_trends.csv")
)

invisible(TRUE)
