# graph_scripts/07_quartile_enrollment_comparison.R
# Compare suspension rates across enrollment quartiles defined by Black, White,
# and Hispanic/Latino student concentration for every race/ethnicity and year.

suppressPackageStartupMessages({
  library(dplyr)
  library(ggrepel)
  library(ggplot2)
  library(glue)
  library(here)
  library(scales)
  library(tidyr)
})

source(here::here("graph_scripts", "graph_utils.R"))

quartile_palette <- c(
  "Q1" = "#0B3954",
  "Q2" = "#087E8B",
  "Q3" = "#FF5A5F",
  "Q4" = "#C81D25"
)

quartile_group_levels <- c(
  "Black Enrollment Quartile",
  "White Enrollment Quartile",
  "Hispanic/Latino Enrollment Quartile"
)

joined <- load_joined_data()

quartile_base <- joined %>%
  dplyr::filter(
    is_traditional,
    subgroup %in% race_levels,
    !is.na(total_suspensions),
    !is.na(cumulative_enrollment),
    cumulative_enrollment > 0
  ) %>%
  dplyr::mutate(academic_year = as.character(academic_year))

if (nrow(quartile_base) == 0) {
  stop("No traditional school records available for quartile comparison.")
}

quartile_long <- quartile_base %>%
  dplyr::select(
    academic_year,
    subgroup,
    total_suspensions,
    cumulative_enrollment,
    black_quartile = black_prop_q,
    black_label = black_prop_q_label,
    white_quartile = white_prop_q,
    white_label = white_prop_q_label,
    hispanic_quartile = hispanic_prop_q,
    hispanic_label = hispanic_prop_q_label
  ) %>%
  tidyr::pivot_longer(
    cols = c(
      black_quartile, black_label,
      white_quartile, white_label,
      hispanic_quartile, hispanic_label
    ),
    names_to = c("quartile_group", ".value"),
    names_pattern = "(black|white|hispanic)_(quartile|label)"
  ) %>%
  dplyr::mutate(
    quartile_group = factor(quartile_group, levels = c("black", "white", "hispanic")),
    quartile_group = dplyr::recode(
      quartile_group,
      black = quartile_group_levels[1],
      white = quartile_group_levels[2],
      hispanic = quartile_group_levels[3]
    ),
    quartile_group = factor(quartile_group, levels = quartile_group_levels, ordered = TRUE),
    quartile = as.integer(quartile)
  ) %>%
  dplyr::filter(!is.na(quartile), !is.na(label)) %>%
  dplyr::filter(quartile == 4L)

if (nrow(quartile_long) == 0) {
  stop("No quartile-tagged observations available for the comparison chart.")
}

year_levels <- quartile_long$academic_year %>% unique() %>% sort()

quartile_rates <- quartile_long %>%
  dplyr::group_by(quartile_group, academic_year, subgroup) %>%
  dplyr::summarise(
    suspensions = sum(total_suspensions, na.rm = TRUE),
    enrollment = sum(cumulative_enrollment, na.rm = TRUE),
    rate = safe_div(suspensions, enrollment),
    quartile_label = dplyr::first(label),
    .groups = "drop"
  ) %>%
  dplyr::filter(!is.na(rate)) %>%
  dplyr::mutate(
    subgroup = factorize_race(subgroup),
    academic_year = factor(academic_year, levels = year_levels, ordered = TRUE)
  ) %>%

  dplyr::arrange(subgroup, quartile_group, academic_year)

quartile_q4_rates <- quartile_rates %>%
  dplyr::mutate(
    quartile_group = droplevels(quartile_group),
    quartile_group = dplyr::case_when(
      quartile_group == quartile_group_levels[1] ~ "Highest-Black enrollment schools",
      quartile_group == quartile_group_levels[2] ~ "Highest-White enrollment schools",
      quartile_group == quartile_group_levels[3] ~ "Highest-Hispanic/Latino enrollment schools",
      TRUE ~ as.character(quartile_group)
    ),
    quartile_group = factor(
      quartile_group,
      levels = c(
        "Highest-Black enrollment schools",
        "Highest-White enrollment schools",
        "Highest-Hispanic/Latino enrollment schools"
      ),
      ordered = TRUE
    )
  ) %>%
  dplyr::arrange(subgroup, quartile_group, academic_year)

plot_title <- "Suspension rates in highest-enrollment schools (Q4 cohort)"
plot_subtitle <- glue::glue(
  "Traditional schools in Q4—the highest share of Black, White, or Hispanic/Latino enrollment\n",
  "Lines trace suspension rates by race/ethnicity across highest-Black, highest-White, and highest-Hispanic/Latino panels"
)

plot_rates <- ggplot(quartile_q4_rates,
                     aes(x = academic_year,
                         y = rate,
                         color = subgroup,
                         group = subgroup)) +
  geom_line(linewidth = 0.7) +
  geom_point(size = 1.6) +

  scale_color_manual(
    values = race_palette,
    breaks = race_levels,
    limits = race_levels,
    name = "Student race/ethnicity"
  ) +

  scale_y_continuous(labels = scales::percent_format(accuracy = 0.1)) +
  labs(
    title = plot_title,
    subtitle = plot_subtitle,
    x = "Academic year",
    y = "Suspension rate",
    caption = "Source: California statewide suspension data (susp_v5 + v6 features)"
  ) +
  facet_wrap(~ quartile_group, nrow = 1) +
  theme_reach() +
  theme(
    legend.position = "bottom"
  )

out_path <- file.path(OUTPUT_DIR, "quartile_enrollment_comparison.png")

ggsave(out_path, plot_rates, width = 13, height = 14, dpi = 320)

table_path <- file.path(OUTPUT_DIR, "quartile_enrollment_comparison.csv")

readr::write_csv(quartile_q4_rates %>%
                   dplyr::select(
                     quartile_group,
                     academic_year,
                     subgroup,
                     suspensions,
                     enrollment,
                     rate
                   ),
                 table_path)

message("Saved comparison plot to ", out_path)
message("Saved comparison table to ", table_path)
