# graph_scripts/03_elementary_disparities.R
# Line graph of elementary traditional school suspension rates by race/ethnicity.

suppressPackageStartupMessages({
  library(dplyr)
  library(ggplot2)
  library(glue)
  library(here)
  library(scales)
  library(tidyr)
  library(ggrepel)
})

source(here::here("graph_scripts", "graph_utils.R"))

extract_rate <- function(df, group_name) {
  vals <- df %>% dplyr::filter(subgroup == group_name) %>% dplyr::pull(rate)
  if (length(vals) == 0) NA_real_ else vals[1]
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
earliest_year <- if (length(year_levels) > 0) year_levels[1] else NA_character_

elem_rates <- elem_base %>%
  dplyr::group_by(academic_year, subgroup) %>%
  dplyr::summarise(
    suspensions = sum(total_suspensions, na.rm = TRUE),
    enrollment = sum(cumulative_enrollment, na.rm = TRUE),
    rate = safe_div(suspensions, enrollment),
    .groups = "drop"
  ) %>%
  dplyr::filter(!is.na(rate)) %>%
  dplyr::mutate(
    academic_year = factor(academic_year, levels = year_levels),
    subgroup = factorize_race(subgroup)
  )

label_data <- elem_rates %>%
  dplyr::mutate(label = scales::percent(rate, accuracy = 0.1))
latest_year <- latest_year_available(elem_rates$academic_year)

plot_elem <- ggplot(elem_rates,
                    aes(x = academic_year, y = rate, color = subgroup, group = subgroup)) +
  geom_line(linewidth = 1.05) +
  geom_point(size = 2.6) +

  geom_text_repel(data = label_data,
                  aes(label = label),
                  size = 3,
                  show.legend = FALSE,
                  max.overlaps = Inf,
                  seed = 123) +
  scale_color_manual(values = race_palette, guide = guide_legend(nrow = 2)) +
  scale_x_discrete(expand = expansion(mult = c(0.02, 0.02))) +
  scale_y_continuous(labels = scales::percent_format(accuracy = 0.1),
                     expand = expansion(mult = c(0, 0.1))) +
  coord_cartesian(clip = "off") +
  labs(
    title = "Elementary Suspension Rates by Race/Ethnicity",
    subtitle = "Traditional elementary schools • Rate = total suspensions ÷ cumulative enrollment",
    x = NULL,
    y = "Suspension rate",
    color = "Student group",
    caption = "Source: California statewide suspension data (susp_v5 + v6 features)"
  ) +
  theme_reach()

out_path <- file.path(OUTPUT_DIR, "elementary_race_trends.png")

ggsave(out_path, plot_elem, width = 12, height = 7, dpi = 320)

latest_summary <- elem_rates %>%
  dplyr::filter(as.character(academic_year) == latest_year) %>%
  dplyr::select(subgroup, rate)
black_rate <- extract_rate(latest_summary, "Black/African American")
white_rate <- extract_rate(latest_summary, "White")
hisp_rate  <- extract_rate(latest_summary, "Hispanic/Latino")
ratio_bw <- if (!is.na(black_rate) && !is.na(white_rate) && white_rate > 0) black_rate / white_rate else NA_real_

opening_gap <- elem_rates %>%
  dplyr::filter(as.character(academic_year) == earliest_year,
                subgroup %in% c("Black/African American", "White")) %>%
  tidyr::pivot_wider(names_from = subgroup, values_from = rate)

opening_ratio <- if (nrow(opening_gap) == 1 && !is.na(opening_gap$`White`)) {
  opening_gap$`Black/African American` / opening_gap$`White`
} else {
  NA_real_
}

history_sentence <- if (!is.na(opening_ratio) && opening_ratio > 0) {
  glue("The Black–White gap has persisted since the first year of the series at roughly {scales::number(opening_ratio, accuracy = 0.1)}× the White rate.")
} else {
  "The Black–White gap has persisted since the first year of the series with no sign of closing."
}

desc_text <- glue(
  "Disparities emerge in the earliest grades. In {latest_year}, {scales::percent(black_rate, accuracy = 0.1)} of Black elementary students were suspended statewide—",
  "{scales::number(ratio_bw, accuracy = 0.1)} times the rate for White peers ({scales::percent(white_rate, accuracy = 0.1)}) and well above Hispanic/Latino classmates ({scales::percent(hisp_rate, accuracy = 0.1)}). ",
  history_sentence
)

write_description(desc_text, "elementary_race_trends.txt")

message("Saved plot to ", out_path)
