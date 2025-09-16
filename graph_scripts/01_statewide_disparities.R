# graph_scripts/01_statewide_disparities.R
# Line graph of statewide suspension rates by race/ethnicity across years.

suppressPackageStartupMessages({
  library(dplyr)
  library(ggplot2)
  library(glue)
  library(here)
  library(scales)
  library(ggrepel)
})

source(here::here("graph_scripts", "graph_utils.R"))

extract_rate <- function(df, group_name) {
  vals <- df %>% dplyr::filter(subgroup == group_name) %>% dplyr::pull(rate)
  if (length(vals) == 0) NA_real_ else vals[1]
}

joined <- load_joined_data()

statewide_base <- joined %>%
  dplyr::filter(
    is_traditional,
    subgroup %in% race_levels,
    !is.na(total_suspensions),
    !is.na(cumulative_enrollment),
    cumulative_enrollment > 0
  )

year_levels <- statewide_base$academic_year %>% unique() %>% sort()

statewide_rates <- statewide_base %>%
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

label_data <- statewide_rates %>%
  dplyr::mutate(label = scales::percent(rate, accuracy = 0.1))
latest_year <- latest_year_available(statewide_rates$academic_year)

plot_statewide <- ggplot(statewide_rates,
                         aes(x = academic_year, y = rate,
                             color = subgroup, group = subgroup)) +
  geom_line(linewidth = 1.1) +
  geom_point(size = 2.7) +
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
    title = "Statewide Suspension Rates by Race/Ethnicity",
    subtitle = "Traditional schools • All grades • Rate = total suspensions ÷ cumulative enrollment",
    x = NULL,
    y = "Suspension rate",
    color = "Student group",
    caption = "Source: California statewide suspension data (susp_v5 + v6 features)"
  ) +
  theme_reach()

out_path <- file.path(OUTPUT_DIR, "statewide_race_trends.png")

ggsave(out_path, plot_statewide, width = 12, height = 7, dpi = 320)

latest_summary <- statewide_rates %>%
  dplyr::filter(as.character(academic_year) == latest_year) %>%
  dplyr::select(subgroup, rate)
black_rate <- extract_rate(latest_summary, "Black/African American")
white_rate <- extract_rate(latest_summary, "White")
hisp_rate  <- extract_rate(latest_summary, "Hispanic/Latino")
asian_rate <- extract_rate(latest_summary, "Asian")
ratio_bw <- if (!is.na(black_rate) && !is.na(white_rate) && white_rate > 0) black_rate / white_rate else NA_real_
change_text <- statewide_rates %>%
  dplyr::group_by(subgroup) %>%
  dplyr::summarise(
    first_rate = dplyr::first(rate),
    last_rate  = dplyr::last(rate),
    .groups = "drop"
  ) %>%
  dplyr::filter(subgroup %in% c("Black/African American", "White"))

trend_dir <- if (nrow(change_text) == 2 && all(!is.na(change_text$first_rate), !is.na(change_text$last_rate))) {
  diff_black <- change_text$last_rate[change_text$subgroup == "Black/African American"] -
    change_text$first_rate[change_text$subgroup == "Black/African American"]
  diff_white <- change_text$last_rate[change_text$subgroup == "White"] -
    change_text$first_rate[change_text$subgroup == "White"]
  if (diff_black > 0.001 | diff_white > 0.001) "re-opened era increases" else "slight shifts"
} else {
  "persistent gaps"
}

desc_text <- glue(
  "Across the statewide series, Black students face the steepest suspension burden every year. " ,
  "In {latest_year}, {scales::percent(black_rate, accuracy = 0.1)} of Black students were suspended statewide—", 
  "{scales::number(ratio_bw, accuracy = 0.1)} times the rate for White students ({scales::percent(white_rate, accuracy = 0.1)}) and nearly double the Hispanic/Latino rate ({scales::percent(hisp_rate, accuracy = 0.1)}). ",
  "Asian students remain the lowest at {scales::percent(asian_rate, accuracy = 0.1)}, underscoring {trend_dir} rather than any real narrowing of racial gaps."
)

write_description(desc_text, "statewide_race_trends.txt")

message("Saved plot to ", out_path)
