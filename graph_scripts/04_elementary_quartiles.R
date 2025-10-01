# graph_scripts/04_elementary_quartiles.R
# Clustered bar chart for elementary traditional schools comparing statewide vs Q4 Black enrollment.

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

elem_base <- joined %>%
  dplyr::filter(
    is_traditional,
    school_level == "Elementary",
    subgroup %in% race_levels,
    !is.na(total_suspensions),
    !is.na(cumulative_enrollment),
    cumulative_enrollment > 0
  )

year_levels <- elem_base$academic_year %>% unique() %>% sort()
analysis_year <- latest_year_available(year_levels)

if (is.na(analysis_year)) {
  stop("No academic year available for elementary quartile comparison")
}

latest_slice <- elem_base %>% dplyr::filter(academic_year == analysis_year)

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

plot_elem_quartiles <- ggplot(bars,
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
    title = glue("Elementary Suspension Rates — {analysis_year}"),
    subtitle = "Traditional elementary schools statewide vs. highest-Black-enrollment campuses",
    x = NULL,
    y = "Suspension rate",
    caption = "Source: California statewide suspension data (susp_v6_long + v6 features)"
  ) +
  theme_reach() +
  theme(axis.text.x = element_text(angle = 35, hjust = 1))

out_path <- file.path(OUTPUT_DIR, "elementary_quartile_comparison.png")

ggsave(out_path, plot_elem_quartiles, width = 11, height = 7, dpi = 320)

black_state <- statewide_rates %>% dplyr::filter(subgroup == "Black/African American") %>% dplyr::pull(rate)
black_q4    <- q4_rates %>% dplyr::filter(subgroup == "Black/African American") %>% dplyr::pull(rate)
hisp_state  <- statewide_rates %>% dplyr::filter(subgroup == "Hispanic/Latino") %>% dplyr::pull(rate)
hisp_q4     <- q4_rates %>% dplyr::filter(subgroup == "Hispanic/Latino") %>% dplyr::pull(rate)

spread_black <- if (length(black_state) > 0 && length(black_q4) > 0) black_q4[1] - black_state[1] else NA_real_
spread_hisp  <- if (length(hisp_state) > 0 && length(hisp_q4) > 0) hisp_q4[1] - hisp_state[1] else NA_real_

summary_text <- glue(
  "Even in elementary grades, high-Black-enrollment schools drive sharper discipline. {scales::percent(black_state, accuracy = 0.1)} of Black students were suspended statewide in {analysis_year}, but the rate jumps to {scales::percent(black_q4, accuracy = 0.1)} in Q4 campuses—an extra {scales::percent(spread_black, accuracy = 0.1)}. ",
  "Hispanic/Latino students also see higher rates ({scales::percent(hisp_state, accuracy = 0.1)} statewide vs. {scales::percent(hisp_q4, accuracy = 0.1)} in Q4), showing that concentrated discipline practices impact multiple groups while still falling hardest on Black children."
)

write_description(summary_text, "elementary_quartile_comparison.txt")

message("Saved plot to ", out_path)
