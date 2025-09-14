# Analysis/20_suspension_reason_trends_by_level_and_locale.R
# Trend analysis of suspension reasons for all students.

# --- 1) Setup -----------------------------------------------------------------
suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
  library(tidyr)
  library(ggplot2)
  library(ggrepel)
  library(scales)
  library(readr)
})

try(here::i_am("Analysis/20_suspension_reason_trends_by_level_and_locale.R"), silent = TRUE)

# canonical reason labels + palettes + locale levels
source(here::here("R", "utils_keys_filters.R"))

# output directory
out_dir <- here::here("outputs")
dir.create(out_dir, showWarnings = FALSE)

# --- 2) Load Data -------------------------------------------------------------
# long-format v6 with suspension reasons and school attributes
v6 <- arrow::read_parquet(here::here("data-stage", "susp_v6_long.parquet")) %>%
  filter(
    category_type == "Race/Ethnicity",
    subgroup == "All Students"
  )

# academic year order (lexical sort works for "2017-18" style)
year_levels <- v6 %>% distinct(academic_year) %>% arrange(academic_year) %>% pull(academic_year)
v6 <- v6 %>% mutate(academic_year = factor(academic_year, levels = year_levels))

# --- helpers ------------------------------------------------------------------
# compute suspension reason rates for arbitrary grouping columns
summarise_reason_rates <- function(df, group_cols) {
  df %>%
    group_by(across(all_of(group_cols))) %>%
    summarise(
      total_suspensions = sum(total_suspensions, na.rm = TRUE),
      enrollment        = sum(cumulative_enrollment, na.rm = TRUE),
      violent_injury     = sum(suspension_count_violent_incident_injury,    na.rm = TRUE),
      violent_no_injury  = sum(suspension_count_violent_incident_no_injury, na.rm = TRUE),
      weapons_possession = sum(suspension_count_weapons_possession,         na.rm = TRUE),
      illicit_drug       = sum(suspension_count_illicit_drug_related,       na.rm = TRUE),
      defiance_only      = sum(suspension_count_defiance_only,              na.rm = TRUE),
      other_reasons      = sum(suspension_count_other_reasons,              na.rm = TRUE),
      .groups = "drop"
    ) %>%
    mutate(total_rate = if_else(enrollment > 0, total_suspensions / enrollment, NA_real_)) %>%
    pivot_longer(
      cols = violent_injury:other_reasons,
      names_to = "reason",
      values_to = "count"
    ) %>%
    add_reason_label("reason") %>%
    mutate(reason_rate = if_else(enrollment > 0, count / enrollment, NA_real_))
}

# save helper
save_table <- function(df, filename) {
  readr::write_csv(df, file.path(out_dir, filename))
}

# plot helpers ---------------------------------------------------------------
plot_total_rate <- function(df, title_txt, color_col = NULL, palette = NULL) {
  if (is.null(color_col)) {
    p <- ggplot(df, aes(x = academic_year, y = total_rate, group = 1))
  } else {
    p <- ggplot(df, aes(x = academic_year, y = total_rate, group = !!sym(color_col), color = !!sym(color_col)))
  }
  p +
    geom_line(linewidth = 1.2) +
    geom_point(size = 2.5) +
    ggrepel::geom_text_repel(
      aes(label = percent(total_rate, accuracy = 0.1)),
      segment.linetype = "dashed",
      size = 3.0,
      na.rm = TRUE
    ) +
    scale_y_continuous(labels = percent_format(accuracy = 1), limits = c(0, NA)) +
    scale_color_manual(values = palette) +
    labs(title = title_txt, x = "Academic Year", y = "Suspension Rate", color = NULL) +
    theme_minimal(base_size = 14) +
    theme(axis.text.x = element_text(angle = 45, hjust = 1),
          plot.title = element_text(face = "bold"))
}

plot_reason_area <- function(df, facet_col = NULL, title_txt) {
  if (is.null(facet_col)) {
    labels <- df %>% group_by(academic_year) %>%
      summarise(total_rate = first(total_rate), .groups = "drop")
    ggplot(df, aes(x = academic_year, y = reason_rate, fill = reason_lab)) +
      geom_area(position = "stack") +
      geom_text(data = labels,
                aes(x = academic_year, y = total_rate, label = percent(total_rate, accuracy = 0.1)),
                vjust = -0.5, fontface = "bold", inherit.aes = FALSE) +
      scale_fill_manual(values = pal_reason, name = "Reason for Suspension") +
      scale_y_continuous(labels = percent_format(accuracy = 1), limits = c(0, NA)) +
      labs(title = title_txt, x = "Academic Year", y = "Suspension Rate") +
      theme_minimal(base_size = 14) +
      theme(axis.text.x = element_text(angle = 45, hjust = 1),
            plot.title = element_text(face = "bold"),
            legend.position = "bottom")
  } else {
    labels <- df %>% group_by(across(all_of(c("academic_year", facet_col)))) %>%
      summarise(total_rate = first(total_rate), .groups = "drop")
    ggplot(df, aes(x = academic_year, y = reason_rate, fill = reason_lab)) +
      geom_area(position = "stack") +
      geom_text(data = labels,
                aes(x = academic_year, y = total_rate, label = percent(total_rate, accuracy = 0.1)),
                vjust = -0.5, fontface = "bold", inherit.aes = FALSE) +
      facet_wrap(as.formula(paste0("~", facet_col)), ncol = 2) +
      scale_fill_manual(values = pal_reason, name = "Reason for Suspension") +
      scale_y_continuous(labels = percent_format(accuracy = 1), limits = c(0, NA)) +
      labs(title = title_txt, x = "Academic Year", y = "Suspension Rate") +
      theme_minimal(base_size = 14) +
      theme(axis.text.x = element_text(angle = 45, hjust = 1),
            plot.title = element_text(face = "bold"),
            legend.position = "bottom")
  }
}

# --- 3) Overall trends -------------------------------------------------------
overall_rates <- summarise_reason_rates(v6, "academic_year")
save_table(overall_rates, "20_overall_reason_rates.csv")

p_overall_total <- plot_total_rate(distinct(overall_rates, academic_year, total_rate),
                                   "All Students Suspension Rate")

ggsave(file.path(out_dir, "20_overall_total_rate.png"), p_overall_total,
       width = 10, height = 6, dpi = 300)

p_overall_reason <- plot_reason_area(overall_rates, NULL,
                                     "Composition of Suspensions by Reason — All Students")

ggsave(file.path(out_dir, "20_overall_reason_rates.png"), p_overall_reason,
       width = 10, height = 6, dpi = 300)

# --- 4) By grade level -------------------------------------------------------
grade_levels <- setdiff(LEVEL_LABELS, c("Other", "Alternative"))
by_grade <- v6 %>%
  filter(school_level %in% grade_levels) %>%
  mutate(school_level = factor(school_level, levels = grade_levels))

grade_rates <- summarise_reason_rates(by_grade, c("academic_year", "school_level"))
save_table(grade_rates, "20_grade_reason_rates.csv")

p_grade_total <- plot_total_rate(distinct(grade_rates, academic_year, school_level, total_rate),
                                 "Suspension Rate by Grade Level",
                                 color_col = "school_level",
                                 palette = pal_level[grade_levels])

ggsave(file.path(out_dir, "20_grade_total_rate.png"), p_grade_total,
       width = 10, height = 6, dpi = 300)

p_grade_reason <- plot_reason_area(grade_rates, "school_level",
                                   "Suspension Reasons by Grade Level")

ggsave(file.path(out_dir, "20_grade_reason_rates.png"), p_grade_reason,
       width = 12, height = 8, dpi = 300)

# --- 5) By locale ------------------------------------------------------------
loc_levels <- setdiff(locale_levels, "Unknown")
by_locale <- v6 %>%
  filter(locale_simple %in% loc_levels) %>%
  mutate(locale_simple = factor(locale_simple, levels = loc_levels))

locale_rates <- summarise_reason_rates(by_locale, c("academic_year", "locale_simple"))
save_table(locale_rates, "20_locale_reason_rates.csv")

p_locale_total <- plot_total_rate(distinct(locale_rates, academic_year, locale_simple, total_rate),
                                  "Suspension Rate by Locale",
                                  color_col = "locale_simple",
                                  palette = pal_locale[loc_levels])

ggsave(file.path(out_dir, "20_locale_total_rate.png"), p_locale_total,
       width = 10, height = 6, dpi = 300)

p_locale_reason <- plot_reason_area(locale_rates, "locale_simple",
                                    "Suspension Reasons by Locale")

ggsave(file.path(out_dir, "20_locale_reason_rates.png"), p_locale_reason,
       width = 12, height = 8, dpi = 300)

message("Analysis complete. Tables and graphics saved to:", out_dir)

