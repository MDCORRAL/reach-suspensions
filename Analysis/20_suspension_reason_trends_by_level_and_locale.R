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
  library(purrr)
})

try(here::i_am("Analysis/20_suspension_reason_trends_by_level_and_locale.R"), silent = TRUE)

# canonical reason labels + palettes + locale levels
source(here::here("R", "utils_keys_filters.R"))

# palette for traditional vs. non-traditional comparisons
pal_setting <- c(
  "Traditional" = "#1f78b4",
  "Non-traditional" = "#e31a1c"
)

# palette for enrollment quartiles
pal_quartile <- c(
  "Q1" = "#1b9e77",
  "Q2" = "#d95f02",
  "Q3" = "#7570b3",
  "Q4" = "#e7298a"
)

# output directory
out_dir <- here::here("outputs")
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

# --- 2) Load Data -------------------------------------------------------------
# long-format v6 with suspension reasons and school attributes
v6 <- arrow::read_parquet(
  here::here("data-stage", "susp_v6_long.parquet")
) %>%
  filter(
    category_type == "Race/Ethnicity",
  )

# bring in school setting (traditional vs. non-traditional) and race quartiles
features <- arrow::read_parquet(
  here::here("data-stage", "susp_v6_features.parquet")
) %>%
  select(
    school_code,
    academic_year,

    is_traditional,
    ends_with("_prop_q_label")
  ) %>%
  mutate(
    setting = case_when(
      isTRUE(is_traditional)  ~ "Traditional",
      isFALSE(is_traditional) ~ "Non-traditional",
      TRUE                    ~ NA_character_
    )

  )

v6 <- v6 %>%
  left_join(features, by = c("school_code", "academic_year")) %>%
  mutate(
    across(ends_with("_prop_q_label"), ~ as.character(.x))
  ) %>%
  select(-is_traditional)

valid_settings <- sort(unique(stats::na.omit(v6$setting)))
unexpected_settings <- setdiff(valid_settings, names(pal_setting))
if (length(unexpected_settings) > 0) {
  stop(
    sprintf(
      "Unexpected school setting values: %s",
      paste(unexpected_settings, collapse = ", ")
    )
  )
}

v6 <- v6 %>%
  mutate(setting = factor(as.character(setting), levels = names(pal_setting)))

# academic year order (lexical sort works for "2017-18" style)
year_levels <- v6 %>%
  distinct(academic_year) %>%
  arrange(academic_year) %>%
  pull(academic_year)
v6 <- v6 %>%
  mutate(academic_year = factor(academic_year, levels = year_levels))

v6_all <- v6 %>%

  filter(subgroup == "All Students")

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
    mutate(
      reason_lab = factor(as.character(reason_lab), levels = names(pal_reason)),
      reason_rate = if_else(enrollment > 0, count / enrollment, NA_real_)
    )
}

# save helper
save_table <- function(df, filename) {
  readr::write_csv(df, file.path(out_dir, filename))
}

# plotting helpers -------------------------------------------------------------
plot_total_rate <- function(df, title_txt, color_col = NULL, palette = NULL) {
  if (is.null(color_col)) {
    p <- ggplot(df, aes(x = academic_year, y = total_rate, group = 1))
  } else {
    p <- ggplot(df, aes(x = academic_year, y = total_rate, group = !!sym(color_col), color = !!sym(color_col)))
  }
  p <- p +
    geom_line(linewidth = 1.2) +
    geom_point(size = 2.5) +
    ggrepel::geom_text_repel(
      aes(label = percent(total_rate, accuracy = 0.1)),
      segment.linetype = "dashed",
      size = 3.0,
      na.rm = TRUE
    ) +
    scale_y_continuous(labels = percent_format(accuracy = 1), limits = c(0, NA))

  if (!is.null(color_col)) {
    if (!is.null(palette)) {
      p <- p + scale_color_manual(
        values = palette,
        breaks = names(palette),
        limits = names(palette),
        drop = FALSE
      )
    } else {
      p <- p + scale_color_discrete()
    }
  }

  p +
    labs(title = title_txt, x = "Academic Year", y = "Suspension Rate", color = NULL) +
    theme_minimal(base_size = 14) +
    theme(axis.text.x = element_text(angle = 45, hjust = 1),
          plot.title = element_text(face = "bold"))
}

plot_reason_area <- function(df, facet_col = NULL, title_txt) {
  df <- df %>% filter(!is.na(reason_lab))

  if (nrow(df) == 0) {
    stop("No data available to plot for the requested grouping.")
  }

  if (is.null(facet_col)) {
    labels <- df %>% group_by(academic_year) %>%
      summarise(total_rate = first(total_rate), .groups = "drop")
    ggplot(df, aes(x = academic_year, y = reason_rate, fill = reason_lab)) +
      geom_area(position = "stack") +
      geom_text(data = labels,
                aes(x = academic_year, y = total_rate, label = percent(total_rate, accuracy = 0.1)),
                vjust = -0.5, fontface = "bold", inherit.aes = FALSE) +
      scale_fill_manual(values = pal_reason,
                        breaks = names(pal_reason),
                        limits = names(pal_reason),
                        name = "Reason for Suspension",
                        drop = FALSE) +
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
      scale_fill_manual(values = pal_reason,
                        breaks = names(pal_reason),
                        limits = names(pal_reason),
                        name = "Reason for Suspension",
                        drop = FALSE) +
      scale_y_continuous(labels = percent_format(accuracy = 1), limits = c(0, NA)) +
      labs(title = title_txt, x = "Academic Year", y = "Suspension Rate") +
      theme_minimal(base_size = 14) +
      theme(axis.text.x = element_text(angle = 45, hjust = 1),
            plot.title = element_text(face = "bold"),
            legend.position = "bottom")
  }
}

# --- 3) Overall trends -------------------------------------------------------
# Calculate overall statewide reason rates
overall_rates <- summarise_reason_rates(v6_all, "academic_year") %>%
  mutate(reason_lab = factor(reason_lab, levels = names(pal_reason)))
save_table(overall_rates, "20_overall_reason_rates.csv")

p_overall_total <- plot_total_rate(distinct(overall_rates, academic_year, total_rate),
                                   "All Students Suspension Rate")

ggsave(file.path(out_dir, "20_overall_total_rate.png"), p_overall_total,
       width = 10, height = 6, dpi = 300)

p_overall_reason <- plot_reason_area(overall_rates, NULL,
                                     "Composition of Suspensions by Reason — All Students")

ggsave(file.path(out_dir, "20_overall_reason_rates.png"), p_overall_reason,
       width = 10, height = 6, dpi = 300)

# --- 3a) By school setting ---------------------------------------------------

setting_rates <- summarise_reason_rates(
  v6_all %>% filter(!is.na(setting)),
  c("academic_year", "setting")
) %>%
  mutate(reason_lab = factor(reason_lab, levels = names(pal_reason)))
save_table(setting_rates, "20_setting_reason_rates.csv")
save_table(
  distinct(setting_rates, academic_year, setting, total_rate),
  "20_setting_total_rates.csv"
)

p_setting_total <- plot_total_rate(
  distinct(setting_rates, academic_year, setting, total_rate),
  "Suspension Rate by School Setting",
  color_col = "setting",
  palette = pal_setting
)

ggsave(file.path(out_dir, "20_setting_total_rate.png"), p_setting_total,
       width = 10, height = 6, dpi = 300)

p_setting_reason <- plot_reason_area(
  setting_rates,
  "setting",
  "Suspension Reasons by School Setting"
)

ggsave(file.path(out_dir, "20_setting_reason_rates.png"), p_setting_reason,
       width = 12, height = 8, dpi = 300)

# --- 4) By grade level -------------------------------------------------------
grade_levels <- LEVEL_LABELS[!LEVEL_LABELS %in% c("Other", "Alternative")]
by_grade <- v6_all %>%
  filter(school_level %in% grade_levels) %>%
  mutate(school_level = factor(school_level, levels = grade_levels))

grade_rates <- summarise_reason_rates(by_grade, c("academic_year", "school_level")) %>%
  mutate(reason_lab = factor(reason_lab, levels = names(pal_reason)))
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

grade_setting_rates <- summarise_reason_rates(
  by_grade %>% filter(!is.na(setting)),
  c("academic_year", "school_level", "setting")
) %>%

  mutate(reason_lab = factor(reason_lab, levels = names(pal_reason)))
save_table(grade_setting_rates, "20_grade_setting_reason_rates.csv")
save_table(
  distinct(grade_setting_rates, academic_year, school_level, setting, total_rate),
  "20_grade_setting_total_rates.csv"
)

grade_setting_groups <- split(grade_setting_rates, grade_setting_rates$setting)
purrr::walk(names(grade_setting_groups), function(setting_name) {
  df <- grade_setting_groups[[setting_name]]
  if (nrow(df) == 0) return(NULL)
  title_suffix <- sprintf("— %s Schools", setting_name)
  total_plot <- plot_total_rate(
    distinct(df, academic_year, school_level, total_rate),
    paste("Suspension Rate by Grade Level", title_suffix),
    color_col = "school_level",
    palette = pal_level[grade_levels]
  )
  ggsave(
    file.path(out_dir, sprintf("20_grade_total_rate_%s.png", tolower(gsub("[^[:alnum:]]+", "_", setting_name)))),
    total_plot,
    width = 10,
    height = 6,
    dpi = 300
  )

  reason_plot <- plot_reason_area(
    df,
    "school_level",
    paste("Suspension Reasons by Grade Level", title_suffix)
  )
  ggsave(
    file.path(out_dir, sprintf("20_grade_reason_rates_%s.png", tolower(gsub("[^[:alnum:]]+", "_", setting_name)))),
    reason_plot,
    width = 12,
    height = 8,
    dpi = 300
  )
})

# --- 5) By locale ------------------------------------------------------------
loc_levels <- locale_levels[locale_levels != "Unknown"]
by_locale <- v6_all %>%
  filter(locale_simple %in% loc_levels) %>%
  mutate(locale_simple = factor(locale_simple, levels = loc_levels))

locale_rates <- summarise_reason_rates(by_locale, c("academic_year", "locale_simple")) %>%
  mutate(reason_lab = factor(reason_lab, levels = names(pal_reason)))
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

locale_setting_rates <- summarise_reason_rates(
  by_locale %>% filter(!is.na(setting)),
  c("academic_year", "locale_simple", "setting")
) %>%
  mutate(reason_lab = factor(reason_lab, levels = names(pal_reason)))
save_table(locale_setting_rates, "20_locale_setting_reason_rates.csv")
save_table(
  distinct(locale_setting_rates, academic_year, locale_simple, setting, total_rate),
  "20_locale_setting_total_rates.csv"
)

locale_setting_groups <- split(locale_setting_rates, locale_setting_rates$setting)
purrr::walk(names(locale_setting_groups), function(setting_name) {
  df <- locale_setting_groups[[setting_name]]
  if (nrow(df) == 0) return(NULL)
  title_suffix <- sprintf("— %s Schools", setting_name)
  total_plot <- plot_total_rate(
    distinct(df, academic_year, locale_simple, total_rate),
    paste("Suspension Rate by Locale", title_suffix),
    color_col = "locale_simple",
    palette = pal_locale[loc_levels]
  )
  ggsave(
    file.path(out_dir, sprintf("20_locale_total_rate_%s.png", tolower(gsub("[^[:alnum:]]+", "_", setting_name)))),
    total_plot,
    width = 10,
    height = 6,
    dpi = 300
  )

  reason_plot <- plot_reason_area(
    df,
    "locale_simple",
    paste("Suspension Reasons by Locale", title_suffix)
  )
  ggsave(
    file.path(out_dir, sprintf("20_locale_reason_rates_%s.png", tolower(gsub("[^[:alnum:]]+", "_", setting_name)))),
    reason_plot,
    width = 12,
    height = 8,
    dpi = 300
  )
})

# --- 6) Reason trends by race quartile ---------------------------------------
quartile_levels <- names(pal_quartile)

race_quartile_data <- v6 %>%
  filter(subgroup %in% c("Black/African American", "Hispanic/Latino", "White")) %>%
  mutate(
    enrollment_quartile = case_when(
      subgroup == "Black/African American" ~ black_prop_q_label,
      subgroup == "Hispanic/Latino"        ~ hispanic_prop_q_label,
      subgroup == "White"                  ~ white_prop_q_label,
      TRUE                                  ~ NA_character_
    ),
    enrollment_quartile = factor(as.character(enrollment_quartile), levels = quartile_levels),
    setting = factor(setting, levels = names(pal_setting))
  ) %>%
  filter(!is.na(enrollment_quartile), !is.na(setting))

quartile_rates <- summarise_reason_rates(
  race_quartile_data,
  c("academic_year", "subgroup", "setting", "enrollment_quartile")
) %>%
  mutate(reason_lab = factor(reason_lab, levels = names(pal_reason)))

save_table(quartile_rates, "20_race_quartile_reason_rates.csv")

quartile_total_rates <- quartile_rates %>%
  distinct(academic_year, subgroup, setting, enrollment_quartile, total_rate)
save_table(quartile_total_rates, "20_race_quartile_total_rates.csv")

p_quartile_total <- ggplot(quartile_total_rates,
                           aes(x = academic_year, y = total_rate,
                               color = enrollment_quartile,
                               group = enrollment_quartile)) +
  geom_line(linewidth = 1.1) +
  geom_point(size = 2.5) +
  scale_color_manual(values = pal_quartile, drop = FALSE) +
  scale_y_continuous(labels = percent_format(accuracy = 1), limits = c(0, NA)) +
  facet_grid(setting ~ subgroup) +
  labs(
    title = "Suspension Rates by Enrollment Quartile and Race/Ethnicity",
    x = "Academic Year",
    y = "Suspension Rate",
    color = "Enrollment Quartile"
  ) +
  theme_minimal(base_size = 14) +
  theme(
    axis.text.x = element_text(angle = 45, hjust = 1),
    plot.title = element_text(face = "bold")
  )

ggsave(file.path(out_dir, "20_race_quartile_total_rates.png"), p_quartile_total,
       width = 14, height = 8, dpi = 300)

quartile_plot_groups <- quartile_rates %>% distinct(setting, subgroup)
purrr::pwalk(quartile_plot_groups, function(setting, subgroup) {
  df <- quartile_rates %>%
    filter(setting == !!setting, subgroup == !!subgroup)
  if (nrow(df) == 0) return(NULL)
  title_txt <- sprintf(
    "Suspension Reasons by Enrollment Quartile — %s (%s)",
    subgroup,
    setting
  )
  reason_plot <- plot_reason_area(
    df,
    "enrollment_quartile",
    title_txt
  )
  file_stub <- paste(setting, subgroup)
  file_stub <- tolower(gsub("[^[:alnum:]]+", "_", file_stub))
  ggsave(
    file.path(out_dir, sprintf("20_race_quartile_reason_rates_%s.png", file_stub)),
    reason_plot,
    width = 12,
    height = 8,
    dpi = 300
  )
})

message("Analysis complete. Tables and graphics saved to: ", out_dir)

