# graph_scripts/21_black_quartile_suspension_trends.R
# Suspension rate trends across Black-enrollment quartiles.

suppressPackageStartupMessages({
  library(dplyr)
  library(ggplot2)
  library(ggrepel)
  library(here)
  library(scales)
})

source(here::here("graph_scripts", "graph_utils.R"))

quartile_levels <- c(
  "Q1 (Lowest % Black)",
  "Q2",
  "Q3",
  "Q4 (Highest % Black)"
)

quartile_palette <- c(
  "Q1 (Lowest % Black)" = "#0B3954",
  "Q2"                 = "#087E8B",
  "Q3"                 = "#FF5A5F",
  "Q4 (Highest % Black)" = "#C81D25"
)

joined <- load_joined_data()

analysis_base <- joined %>%
  dplyr::filter(
    is_traditional,
    subgroup %in% race_levels,
    !is.na(total_suspensions),
    !is.na(cumulative_enrollment),
    cumulative_enrollment > 0,
    black_prop_q_label %in% quartile_levels
  ) %>%
  dplyr::mutate(
    academic_year = as.character(academic_year)
  )

if (nrow(analysis_base) == 0) {
  stop("No traditional school records available for Black quartile trends.")
}

year_levels <- analysis_base$academic_year %>% unique() %>% sort()

quartile_rates <- analysis_base %>%
  dplyr::group_by(academic_year, subgroup, black_quartile = black_prop_q_label) %>%
  dplyr::summarise(
    suspensions = sum(total_suspensions, na.rm = TRUE),
    enrollment = sum(cumulative_enrollment, na.rm = TRUE),
    rate = safe_div(suspensions, enrollment),
    .groups = "drop"
  ) %>%
  dplyr::filter(!is.na(rate)) %>%
  dplyr::mutate(
    academic_year = factor(academic_year, levels = year_levels, ordered = TRUE),
    subgroup = factorize_race(subgroup),
    black_quartile = factor(black_quartile, levels = quartile_levels, ordered = TRUE)
  ) %>%
  dplyr::arrange(subgroup, black_quartile, academic_year)

plot_title <- "Suspension rates by Black enrollment quartile"
plot_subtitle <- "Traditional schools statewide; pooled suspensions per enrolled student"
plot_caption <- "Source: California statewide suspension data (susp_v6_long + susp_v6_features)"

quartile_plot <- ggplot(
  quartile_rates,
  aes(x = academic_year, y = rate, color = black_quartile, group = black_quartile)
) +
  geom_line(linewidth = 0.9) +
  geom_point(size = 1.8) +
  ggrepel::geom_label_repel(
    aes(label = scales::percent(rate, accuracy = 0.1)),
    size = 2.8,
    label.size = 0,
    label.padding = grid::unit(0.12, "lines"),
    label.r = grid::unit(0.15, "lines"),
    fill = "white",
    box.padding = grid::unit(0.35, "lines"),
    point.padding = grid::unit(0.25, "lines"),
    min.segment.length = 0,
    max.overlaps = Inf,
    show.legend = FALSE
  ) +
  facet_wrap(~ subgroup, scales = "free_y") +
  scale_color_manual(values = quartile_palette, name = "Black enrollment quartile") +
  scale_y_continuous(
    labels = scales::percent_format(accuracy = 0.1),
    expand = expansion(mult = c(0, 0.15))
  ) +
  labs(
    title = plot_title,
    subtitle = plot_subtitle,
    x = "Academic year",
    y = "Suspension rate",
    caption = plot_caption
  ) +
  theme_reach() +
  theme(
    strip.text = element_text(face = "bold", size = 9),
    legend.position = "bottom"
  )

output_dir <- file.path(OUTPUT_DIR, "21_black_quartile_trends")
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

png_path <- file.path(output_dir, "black_quartile_suspension_trends.png")
pdf_path <- file.path(output_dir, "black_quartile_suspension_trends.pdf")

message("Saving plot to ", png_path)

ggsave(png_path, quartile_plot, width = 12, height = 8, dpi = 320)

ggplot2::ggsave(pdf_path, quartile_plot, width = 12, height = 8)

message("Done.")
