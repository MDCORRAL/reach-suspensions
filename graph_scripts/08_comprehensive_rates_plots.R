# graph_scripts/08_comprehensive_rates_plots.R
# Generate UCLA brand-aligned graphics from the comprehensive suspension
# rate analysis outputs without modifying the original analysis script.

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(forcats)
  library(ggplot2)
  library(ggrepel)
  library(here)
  library(janitor)
  library(readr)
  library(scales)
  library(stringr)
})

try(here::i_am("graph_scripts/08_comprehensive_rates_plots.R"), silent = TRUE)

# -----------------------------------------------------------------------------
# Configuration ----------------------------------------------------------------
# -----------------------------------------------------------------------------
source(here("R", "utils_keys_filters.R"))

DATA_STAGE <- here("data-stage")
V6_LONG <- file.path(DATA_STAGE, "susp_v6_long.parquet")
V6_FEAT <- file.path(DATA_STAGE, "susp_v6_features.parquet")

stopifnot(file.exists(V6_LONG), file.exists(V6_FEAT))

OUTPUT_DIR <- here("outputs", "graphs", "comprehensive_rates")
dir.create(OUTPUT_DIR, recursive = TRUE, showWarnings = FALSE)

# UCLA brand-aligned palette ----------------------------------------------------
ucla_colors <- c(
  "Darkest Blue" = "#003B5C",
  "Darker Blue" = "#005587",
  "UCLA Blue" = "#2774AE",
  "Lighter Blue" = "#8BB8E8",
  "UCLA Gold" = "#FFD100",
  "Darker Gold" = "#FFC72C",
  "Darkest Gold" = "#FFB81C",
  "Purple" = "#8A69D4",
  "Green" = "#00FF87",
  "Magenta" = "#FF00A5",
  "Cyan" = "#00FFFF"
)

race_palette <- c(
  "All Students" = ucla_colors[["Darkest Blue"]],
  "Black/African American" = ucla_colors[["UCLA Blue"]],
  "Hispanic/Latino" = ucla_colors[["UCLA Gold"]],
  "White" = ucla_colors[["Darker Blue"]],
  "Asian" = ucla_colors[["Lighter Blue"]],
  "American Indian/Alaska Native" = ucla_colors[["Purple"]],
  "Filipino" = ucla_colors[["Darkest Gold"]],
  "Native Hawaiian/Pacific Islander" = ucla_colors[["Green"]],
  "Two or More Races" = ucla_colors[["Magenta"]]
)

safe_div <- function(num, den) ifelse(is.na(den) | den == 0, NA_real_, num / den)

ucla_theme <- function(base_size = 12, base_family = NULL) {
  ggplot2::theme_minimal(base_size = base_size, base_family = base_family) +
    ggplot2::theme(
      plot.title = element_text(face = "bold", size = base_size + 6, hjust = 0, color = ucla_colors[["Darkest Blue"]]),
      plot.subtitle = element_text(size = base_size + 1, margin = margin(b = 10), color = ucla_colors[["Darkest Blue"]]),
      plot.caption = element_text(size = base_size - 1, color = "#5A5A5A", hjust = 0),
      axis.title = element_text(color = ucla_colors[["Darkest Blue"]], face = "bold"),
      axis.text = element_text(color = ucla_colors[["Darkest Blue"]]),
      panel.grid.major = element_line(color = "#DFE2E5", linewidth = 0.4),
      panel.grid.minor = element_blank(),
      legend.title = element_text(face = "bold", color = ucla_colors[["Darkest Blue"]]),
      legend.text = element_text(color = ucla_colors[["Darkest Blue"]]),
      legend.position = "bottom",
      plot.background = element_rect(fill = "white", color = NA),
      panel.background = element_rect(fill = "white", color = NA),
      strip.text = element_text(face = "bold", color = ucla_colors[["Darkest Blue"]])
    )
}

# -----------------------------------------------------------------------------
# Data preparation --------------------------------------------------------------
# -----------------------------------------------------------------------------

v6_long <- read_parquet(V6_LONG) %>%
  clean_names() %>%
  build_keys() %>%
  filter_campus_only() %>%
  transmute(
    school_code = school_code,
    year = as.character(academic_year),
    race_ethnicity = canon_race_label(subgroup),
    enrollment = as.numeric(cumulative_enrollment),
    total_suspensions = as.numeric(total_suspensions),
    school_level = school_level
  ) %>%
  filter(
    !is.na(race_ethnicity),
    race_ethnicity %in% names(race_palette),
    !is.na(enrollment), enrollment > 0,
    !is.na(total_suspensions), total_suspensions >= 0
  ) %>%
  mutate(
    suspension_rate = safe_div(total_suspensions, enrollment),
    grade_level = factor(school_level, levels = LEVEL_LABELS)
  )

# Join in features to align with analysis conventions (ensures unique school codes)
v6_features <- read_parquet(V6_FEAT) %>%
  clean_names() %>%
  transmute(
    school_code = as.character(school_code),
    year = as.character(academic_year)
  ) %>%
  distinct()

year_levels <- sort(unique(v6_long$year))

analytic_data <- v6_long %>%
  left_join(v6_features, by = c("school_code", "year")) %>%
  mutate(year = factor(year, levels = year_levels))

calc_summary_stats <- function(data, ...) {
  data %>%
    group_by(...) %>%
    summarise(
      n_schools = n_distinct(school_code),
      n_records = n(),
      total_enrollment = sum(enrollment, na.rm = TRUE),
      total_suspensions = sum(total_suspensions, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    mutate(
      pooled_rate = safe_div(total_suspensions, total_enrollment),
      pooled_rate_pct = percent(pooled_rate, accuracy = 0.1)
    )
}

rates_by_race_year <- calc_summary_stats(analytic_data, year, race_ethnicity) %>%
  mutate(race_ethnicity = forcats::fct_relevel(race_ethnicity, names(race_palette)))

rates_by_grade <- calc_summary_stats(analytic_data, year, grade_level, race_ethnicity) %>%
  filter(grade_level %in% c("Elementary", "Middle", "High")) %>%
  mutate(
    grade_level = forcats::fct_relevel(grade_level, c("Elementary", "Middle", "High")),
    race_ethnicity = forcats::fct_relevel(race_ethnicity, names(race_palette))
  )

# -----------------------------------------------------------------------------
# Plot builders -----------------------------------------------------------------
# -----------------------------------------------------------------------------

plot_mean_rates <- function(df) {
  ggplot(df, aes(x = year, y = pooled_rate, color = race_ethnicity, group = race_ethnicity)) +
    geom_line(linewidth = 1.1) +
    geom_point(size = 2.5) +
    geom_label_repel(
      aes(label = pooled_rate_pct),
      size = 3,
      label.size = 0,
      fill = scales::alpha("white", 0.9),
      label.padding = grid::unit(0.15, "lines"),
      point.padding = grid::unit(0.25, "lines"),
      label.r = grid::unit(0.1, "lines"),
      segment.alpha = 0.4,
      show.legend = FALSE,
      max.overlaps = Inf
    ) +
    scale_color_manual(values = race_palette, drop = FALSE) +
    scale_y_continuous(labels = percent_format(accuracy = 0.1), expand = expansion(mult = c(0.05, 0.1))) +
    labs(
      title = "Pooled Suspension Rates by Race/Ethnicity",
      subtitle = "Enrollment-weighted statewide suspension rates by academic year",
      x = "Academic Year",
      y = "Pooled suspension rate",
      color = "Race/Ethnicity",
      caption = "Source: REACH suspension v6 staged files; rates reflect total suspensions divided by total enrollment"
    ) +
    ucla_theme()
}

plot_grade_rates <- function(df, grade_label) {
  ggplot(df, aes(x = year, y = pooled_rate, color = race_ethnicity, group = race_ethnicity)) +
    geom_line(linewidth = 1.1) +
    geom_point(size = 2.5) +
    geom_label_repel(
      aes(label = pooled_rate_pct),
      size = 3,
      label.size = 0,
      fill = scales::alpha("white", 0.9),
      label.padding = grid::unit(0.15, "lines"),
      point.padding = grid::unit(0.25, "lines"),
      label.r = grid::unit(0.1, "lines"),
      segment.alpha = 0.4,
      show.legend = FALSE,
      max.overlaps = Inf
    ) +
    scale_color_manual(values = race_palette, drop = FALSE) +
    scale_y_continuous(labels = percent_format(accuracy = 0.1), expand = expansion(mult = c(0.05, 0.1))) +
    labs(
      title = glue::glue("Pooled Suspension Rates by Race/Ethnicity — {grade_label} Schools"),
      subtitle = "Enrollment-weighted statewide suspension rates by academic year",
      x = "Academic Year",
      y = "Pooled suspension rate",
      color = "Race/Ethnicity",
      caption = "Source: REACH suspension v6 staged files; rates reflect total suspensions divided by total enrollment"
    ) +
    ucla_theme()
}

# -----------------------------------------------------------------------------
# Generate and save plots -------------------------------------------------------
# -----------------------------------------------------------------------------

overall_plot <- plot_mean_rates(rates_by_race_year)

ggsave(
  filename = file.path(OUTPUT_DIR, "mean_suspension_rates_by_race_year.png"),
  plot = overall_plot,
  width = 11, height = 6.5, dpi = 320
)

purrr::walk(
  c("Elementary", "Middle", "High"),
  function(gl) {
    plot_data <- rates_by_grade %>% filter(grade_level == gl)
    if (nrow(plot_data) == 0) {
      warning("No data available for grade level: ", gl)
      return(NULL)
    }
    grade_plot <- plot_grade_rates(plot_data, gl)
    file_name <- paste0("mean_suspension_rates_by_race_year_", stringr::str_to_lower(gl), ".png")
    ggsave(
      filename = file.path(OUTPUT_DIR, file_name),
      plot = grade_plot,
      width = 11, height = 6.5, dpi = 320
    )
  }
)

message("Plots saved to ", OUTPUT_DIR)
