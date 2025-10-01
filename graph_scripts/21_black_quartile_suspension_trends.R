# graph_scripts/21_black_quartile_suspension_trends.R

# Plot Black student suspension trends by Black enrollment quartile using UCLA brand colors.

suppressPackageStartupMessages({
  library(dplyr)
  library(ggplot2)
  library(glue)
  library(here)
  library(scales)
  library(tidyr)
})

try(here::i_am("graph_scripts/21_black_quartile_suspension_trends.R"), silent = TRUE)

source(here::here("graph_scripts", "graph_utils.R"))

quartile_palette <- c(
  "Q1 (Lowest % Black)"   = "#8A69D4",  # UCLA Purple (accessible on white)
  "Q2"                     = "#2774AE",  # UCLA Blue
  "Q3"                     = "#005587",  # Darker Blue
  "Q4 (Highest % Black)"  = "#003B5C"   # Darkest Blue
)

statewide_color <- "#000000"  # Brand-approved black for strong contrast

line_palette <- c(
  "Statewide Traditional Average" = statewide_color,
  quartile_palette
)

legend_levels <- names(line_palette)

joined <- load_joined_data()

analysis_base <- joined %>%
  dplyr::filter(
    is_traditional,
    subgroup == "Black/African American",
    !is.na(total_suspensions),
    !is.na(cumulative_enrollment),
    cumulative_enrollment > 0
  ) %>%
  dplyr::mutate(
    academic_year = as.character(academic_year),
    black_prop_q_label = dplyr::case_when(
      black_prop_q_label %in% names(quartile_palette) ~ black_prop_q_label,
      TRUE ~ NA_character_
    )
  )

if (nrow(analysis_base) == 0) {
  stop("No eligible traditional school records available for quartile trends plot.")
}

quartile_rates <- analysis_base %>%
  dplyr::filter(!is.na(black_prop_q_label)) %>%
  dplyr::group_by(academic_year, black_prop_q_label) %>%
  dplyr::summarise(
    suspensions = sum(total_suspensions, na.rm = TRUE),
    enrollment  = sum(cumulative_enrollment, na.rm = TRUE),
    rate        = safe_div(suspensions, enrollment),
    .groups     = "drop"
  )

statewide_rates <- analysis_base %>%
  dplyr::group_by(academic_year) %>%
  dplyr::summarise(
    suspensions = sum(total_suspensions, na.rm = TRUE),
    enrollment  = sum(cumulative_enrollment, na.rm = TRUE),
    rate        = safe_div(suspensions, enrollment),
    .groups     = "drop"
  ) %>%
  dplyr::mutate(line_label = "Statewide Traditional Average")

plot_data <- quartile_rates %>%
  dplyr::mutate(line_label = black_prop_q_label) %>%
  dplyr::select(academic_year, line_label, rate) %>%
  dplyr::bind_rows(statewide_rates) %>%
  dplyr::mutate(
    academic_year = factor(academic_year, levels = sort(unique(academic_year))),
    line_label    = factor(line_label, levels = legend_levels, ordered = TRUE)
  ) %>%
  dplyr::arrange(line_label, academic_year)

if (nrow(plot_data) == 0) {
  stop("No rates available to plot for quartile trends.")
}

plot_caption <- paste(
  "Source: California statewide suspension data (susp_v6_long.parquet + susp_v6_features.parquet).",
  "Traditional schools only; rates represent Black students statewide.")

plot_object <- ggplot(plot_data,
                      aes(x = academic_year,
                          y = rate,
                          color = line_label,
                          group = line_label)) +
  geom_line(linewidth = 1.1) +
  geom_point(size = 2.4) +
  scale_color_manual(
    values = line_palette,
    breaks = legend_levels,
    limits = legend_levels,
    name = "Reference group"
  ) +
  scale_y_continuous(labels = scales::percent_format(accuracy = 0.1)) +
  labs(
    title = "Black student suspension trends by Black enrollment quartile",
    subtitle = "Traditional public schools statewide",
    x = "Academic year",
    y = "Suspension rate",
    caption = plot_caption
  ) +
  theme_reach(base_size = 13) +
  theme(legend.position = "bottom")

output_path <- file.path(OUTPUT_DIR, "21_black_quartile_suspension_trends.png")

ggsave(output_path, plot_object, width = 12, height = 7.5, dpi = 320)

latest_year <- latest_year_available(plot_data$academic_year)

latest_summary <- plot_data %>%
  dplyr::filter(academic_year == latest_year) %>%
  dplyr::mutate(rate = scales::percent(rate, accuracy = 0.1))

statewide_latest <- latest_summary %>%
  dplyr::filter(line_label == "Statewide Traditional Average") %>%
  dplyr::pull(rate)

quartile_latest <- latest_summary %>%
  dplyr::filter(line_label != "Statewide Traditional Average") %>%
  dplyr::mutate(summary = glue::glue("{line_label}: {rate}")) %>%
  dplyr::pull(summary)

description_text <- glue::glue(
  "Traditional schools statewide suspended Black students at {statewide_latest} in {latest_year}. ",
  "Quartile comparisons show {paste(quartile_latest, collapse = '; ')}."
)

write_description(description_text, "21_black_quartile_suspension_trends.txt")

message("Saved ", output_path)
