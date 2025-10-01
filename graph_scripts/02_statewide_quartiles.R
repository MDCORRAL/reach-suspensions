# graph_scripts/02_statewide_quartiles.R
# Clustered bar chart comparing statewide rates to Q4 Black-enrollment schools by race.

suppressPackageStartupMessages({
  library(dplyr)
  library(ggplot2)
  library(glue)
  library(here)
  library(scales)
})

source(here::here("graph_scripts", "graph_utils.R"))

calc_rates <- function(df) {
  df %>%
    dplyr::group_by(subgroup) %>%
    dplyr::summarise(
      suspensions = sum(total_suspensions, na.rm = TRUE),
      enrollment = sum(cumulative_enrollment, na.rm = TRUE),
      rate = safe_div(suspensions, enrollment),
      .groups = "drop"
    ) %>%
    dplyr::filter(!is.na(rate)) %>%
    dplyr::mutate(subgroup = factorize_race(subgroup))
}

joined <- load_joined_data()

quartile_base <- joined %>%
  dplyr::filter(
    is_traditional,
    subgroup %in% race_levels,
    !is.na(total_suspensions),
    !is.na(cumulative_enrollment),
    cumulative_enrollment > 0
  )

year_levels <- quartile_base$academic_year %>% unique() %>% sort()
analysis_year <- latest_year_available(year_levels)

if (is.na(analysis_year)) {
  stop("No academic year available for quartile comparison")
}

latest_slice <- quartile_base %>% dplyr::filter(academic_year == analysis_year)

statewide_rates <- calc_rates(latest_slice) %>%
  dplyr::mutate(comparison = "Statewide Traditional Average")

q4_rates <- latest_slice %>%
  dplyr::filter(black_prop_q == 4 | black_prop_q_label == "Q4 (Highest % Black)") %>%
  calc_rates() %>%
  dplyr::mutate(comparison = "Q4: Highest Black Enrollment")

bars <- dplyr::bind_rows(statewide_rates, q4_rates) %>%
  dplyr::mutate(
    comparison = factor(comparison, levels = names(comparison_palette)),
    subgroup = forcats::fct_relevel(subgroup, race_levels)
  )

plot_quartiles <- ggplot(bars,
                          aes(x = subgroup, y = rate, fill = comparison)) +
  geom_col(position = position_dodge(width = 0.72), width = 0.68, color = NA) +
  geom_text(aes(label = scales::percent(rate, accuracy = 0.1)),
            position = position_dodge(width = 0.72),
            vjust = -0.25,
            size = 3,
            color = "#2E2E2E") +
  scale_fill_manual(values = comparison_palette, name = NULL) +
  scale_y_continuous(labels = scales::percent_format(accuracy = 0.1),
                     expand = expansion(mult = c(0, 0.12))) +
  labs(
    title = glue("Suspension Rates by Race/Ethnicity — {analysis_year}"),
    subtitle = "Traditional schools statewide vs. schools in the highest quartile of Black enrollment",
    x = NULL,
    y = "Suspension rate",
    caption = "Source: California statewide suspension data (susp_v6_long + v6 features)"
  ) +
  theme_reach() +
  theme(axis.text.x = element_text(angle = 35, hjust = 1))

out_path <- file.path(OUTPUT_DIR, "statewide_quartile_comparison.png")

ggsave(out_path, plot_quartiles, width = 11, height = 7, dpi = 320)

black_state <- statewide_rates %>% dplyr::filter(subgroup == "Black/African American") %>% dplyr::pull(rate)
black_q4    <- q4_rates %>% dplyr::filter(subgroup == "Black/African American") %>% dplyr::pull(rate)
white_state <- statewide_rates %>% dplyr::filter(subgroup == "White") %>% dplyr::pull(rate)
white_q4    <- q4_rates %>% dplyr::filter(subgroup == "White") %>% dplyr::pull(rate)

spread_black <- if (length(black_state) > 0 && length(black_q4) > 0) black_q4[1] - black_state[1] else NA_real_
spread_white <- if (length(white_state) > 0 && length(white_q4) > 0) white_q4[1] - white_state[1] else NA_real_
ratio_black  <- if (!is.na(black_state[1]) && black_state[1] > 0) black_q4[1] / black_state[1] else NA_real_

quartile_text <- glue(
  "In {analysis_year}, statewide traditional schools suspended Black students at {scales::percent(black_state, accuracy = 0.1)}. ",
  "In the highest-Black-enrollment campuses the rate climbed to {scales::percent(black_q4, accuracy = 0.1)}—",
  "an increase of {scales::percent(spread_black, accuracy = 0.1)} and {scales::number(ratio_black, accuracy = 0.1)}× the statewide average. ",
  "White students also see a bump from {scales::percent(white_state, accuracy = 0.1)} statewide to {scales::percent(white_q4, accuracy = 0.1)} in Q4 schools, indicating that discipline practices in high-Black-enrollment campuses elevate suspensions for every group while leaving Black students with the highest burden."
)

write_description(quartile_text, "statewide_quartile_comparison.txt")

message("Saved plot to ", out_path)
