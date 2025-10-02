# graph_scripts/21_black_quartile_suspension_trends.R
# Suspension trends for Black students across enrollment quartiles.

suppressPackageStartupMessages({
  library(dplyr)
  library(ggplot2)
  library(glue)
  library(here)
  library(ggrepel)
  library(readr)
  library(scales)
  library(tidyr)
})

source(here::here("graph_scripts", "graph_utils.R"))

quartile_palette <- c(
  'Q1' = '#003B5C',  # Darkest Blue
  'Q2' = '#2774AE',  # UCLA Blue
  'Q3' = '#FFC72C',  # Darker Gold
  'Q4' = '#8A69D4'   # Purple accent keeps high-contrast separation
)

statewide_color <- 'red'  # Matches the Willful Defiance accent in palette_utils.py
statewide_linetype <- 'dashed'
statewide_label <- 'Statewide average (All Students)'

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

all_students_base <- joined %>%
  dplyr::filter(
    is_traditional,
    subgroup == "All Students",
    !is.na(total_suspensions),
    !is.na(cumulative_enrollment),
    cumulative_enrollment > 0
  ) %>%
  dplyr::mutate(academic_year = as.character(academic_year))

if (nrow(all_students_base) == 0) {
  stop("No statewide All Students records available for traditional schools.")
}

year_levels <- union(black_base$academic_year, all_students_base$academic_year) %>%
  unique() %>%
  sort()

statewide_rates <- all_students_base %>%
  dplyr::group_by(academic_year) %>%
  dplyr::summarise(
    suspensions = sum(total_suspensions, na.rm = TRUE),
    enrollment = sum(cumulative_enrollment, na.rm = TRUE),
    rate = safe_div(suspensions, enrollment),
    .groups = "drop"
  ) %>%
  dplyr::mutate(
    academic_year = factor(academic_year, levels = year_levels, ordered = TRUE)
  ) %>%
  dplyr::arrange(academic_year)

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
    tidyr::complete(
      academic_year = year_levels,
      tidyr::nesting(quartile, quartile_label),
      fill = list(suspensions = NA_real_, enrollment = NA_real_, rate = NA_real_)
    ) %>%
    dplyr::mutate(
      academic_year = factor(academic_year, levels = year_levels, ordered = TRUE),
      cohort = cohort_label
    ) %>%
    dplyr::arrange(academic_year)
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
  quartile_labels <- quartile_data %>%
    dplyr::filter(!is.na(rate)) %>%
    dplyr::mutate(
      label = scales::percent(rate, accuracy = 0.1),
      segment_colour = quartile_palette[as.character(quartile)]
    )

  statewide_labels <- statewide_rates %>%
    dplyr::filter(!is.na(rate)) %>%
    dplyr::mutate(
      label = scales::percent(rate, accuracy = 0.1),
      segment_colour = statewide_color
    )

  ggplot() +
    geom_line(
      data = quartile_data,
      aes(x = academic_year, y = rate, color = quartile, group = quartile),
      linewidth = 1.1,
      lineend = 'round'
    ) +
    geom_point(
      data = quartile_data,
      aes(x = academic_year, y = rate, color = quartile),
      size = 2.3,
      stroke = 0
    ) +
    geom_line(
      data = statewide_rates,
      aes(
        x = academic_year,
        y = rate,
        color = statewide_label,
        group = statewide_label,
        linetype = statewide_label
      ),
      linewidth = 1.1,
      lineend = 'round'
    ) +
    geom_point(
      data = statewide_rates,
      aes(x = academic_year, y = rate, color = statewide_label),
      size = 2.6,
      stroke = 0
    ) +
    ggrepel::geom_label_repel(
      data = quartile_labels,
      aes(
        x = academic_year,
        y = rate,
        label = label,
        color = quartile,
        segment.colour = segment_colour
      ),
      inherit.aes = FALSE,
      fill = scales::alpha("white", 0.85),
      fontface = "bold",
      size = 3,
      show.legend = FALSE,
      label.size = 0,
      label.padding = grid::unit(0.18, "lines"),
      box.padding = grid::unit(0.35, "lines"),
      point.padding = grid::unit(0.3, "lines"),
      label.r = grid::unit(0.08, "lines"),
      direction = "y",
      max.overlaps = Inf,
      segment.size = 0.4,
      segment.linetype = "solid"
    ) +
    ggrepel::geom_label_repel(
      data = statewide_labels,
      aes(
        x = academic_year,
        y = rate,
        label = label,
        color = statewide_label,
        segment.colour = segment_colour
      ),
      inherit.aes = FALSE,
      fill = scales::alpha("white", 0.85),
      fontface = "bold",
      size = 3,
      show.legend = FALSE,
      label.size = 0,
      label.padding = grid::unit(0.18, "lines"),
      box.padding = grid::unit(0.35, "lines"),
      point.padding = grid::unit(0.3, "lines"),
      label.r = grid::unit(0.08, "lines"),
      direction = "y",
      max.overlaps = Inf,
      segment.size = 0.4,
      segment.linetype = "solid"
    ) +
    scale_color_manual(
      values = c(quartile_palette, setNames(statewide_color, statewide_label)),
      breaks = c(names(quartile_palette), statewide_label),
      name = "Series"
    ) +
    scale_linetype_manual(
      values = setNames(statewide_linetype, statewide_label),
      breaks = statewide_label,
      guide = guide_legend(title = "Series")
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
