# R/09_analysis_black_student_rates_final.R
# Final analysis of Black student suspension rates by school racial composition with improved data labels.

# --- 1) Setup -----------------------------------------------------------------
suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
  library(ggplot2)
  library(tidyr)
  library(scales)
  library(patchwork)
  library(ggrepel) # For non-overlapping labels
})

# --- 2) Configuration & Data Loading ------------------------------------------
theme_set(theme_minimal(base_size = 12))

# Custom color palettes
black_quartile_colors <- c(
  "Q1 (Lowest % Black)" = "#FEE5D9", "Q2" = "#FCAE91",
  "Q3" = "#FB6A4A", "Q4 (Highest % Black)" = "#CB181D"
)
white_quartile_colors <- c(
  "Q1 (Lowest % White)" = "#EFF3FF", "Q2" = "#BDD7E7",
  "Q3" = "#6BAED6", "Q4 (Highest % White)" = "#08519C"
)

message("Loading data...")
v5 <- arrow::read_parquet(here::here("data-stage", "susp_v5.parquet"))

# Filter for Black students only
black_students_data <- v5 %>%
  filter(reporting_category == "RB")

# --- 3) Helper Function for Total Rate Plots ---------------------------------
create_total_rate_plot <- function(data, group_var, colors, title_suffix, legend_title) {
  
  group_var_sym <- sym(group_var)
  
  plot_data <- data %>%
    filter(!is.na({{ group_var_sym }})) %>%
    group_by(academic_year, {{ group_var_sym }}) %>%
    summarise(
      total_suspensions = sum(total_suspensions, na.rm = TRUE),
      total_enrollment = sum(cumulative_enrollment, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    mutate(suspension_rate = (total_suspensions / total_enrollment) * 100)
  
  ggplot(plot_data, aes(x = academic_year, y = suspension_rate, color = {{ group_var_sym }}, group = {{ group_var_sym }})) +
    geom_line(linewidth = 1.2) +
    geom_point(size = 2.5) +
    # Add data labels with improved spacing and black color
    geom_text_repel(
      aes(label = round(suspension_rate, 1)), 
      color = "black",         # Set label color to black
      size = 3.5, 
      fontface = "bold", 
      segment.color = 'grey50',
      box.padding = 0.5,       # Increase padding around labels
      max.overlaps = Inf       # Ensure all labels are shown
    ) +
    scale_color_manual(values = colors, name = legend_title) +
    scale_y_continuous(labels = label_percent(scale = 1), expand = expansion(mult = c(0.05, 0.15))) + # Give more space at top
    labs(
      title = paste("Black Student Suspension Rate by", title_suffix),
      subtitle = "Overall suspension rate per 100 Black students",
      x = "Academic Year",
      y = "Suspension Rate (%)"
    ) +
    theme(
      legend.position = "bottom",
      plot.title = element_text(face = "bold", size = 14),
      axis.text.x = element_text(angle = 45, hjust = 1)
    )
}

# --- 4) Helper Function for Category Plots ------------------------------------
create_category_rate_plot <- function(data, group_var, colors, title_suffix, legend_title) {
  
  group_var_sym <- sym(group_var)
  
  plot_data <- data %>%
    filter(!is.na({{ group_var_sym }})) %>%
    group_by(academic_year, {{ group_var_sym }}) %>%
    summarise(
      Defiance = sum(suspension_count_defiance_only, na.rm = TRUE),
      `Violent Injury` = sum(suspension_count_violent_incident_injury, na.rm = TRUE),
      `Violent No Injury` = sum(suspension_count_violent_incident_no_injury, na.rm = TRUE),
      Weapons = sum(suspension_count_weapons_possession, na.rm = TRUE),
      Drugs = sum(suspension_count_illicit_drug_related, na.rm = TRUE),
      Other = sum(suspension_count_other_reasons, na.rm = TRUE),
      total_enrollment = sum(cumulative_enrollment, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    pivot_longer(
      cols = c(Defiance, `Violent Injury`, `Violent No Injury`, Weapons, Drugs, Other),
      names_to = "category",
      values_to = "suspension_count"
    ) %>%
    mutate(reason_rate = (suspension_count / total_enrollment) * 100)
  
  ggplot(plot_data, aes(x = academic_year, y = reason_rate, color = {{ group_var_sym }}, group = {{ group_var_sym }})) +
    geom_line(linewidth = 0.8) +
    geom_point(size = 1.5) +
    # Add data labels to every point in the faceted plot
    geom_text_repel(
      aes(label = round(reason_rate, 1)), 
      color = "black", 
      size = 3, 
      segment.color = 'grey50',
      box.padding = 0.4
    ) +
    facet_wrap(~ category, scales = "free_y", ncol = 3) +
    scale_color_manual(values = colors, name = legend_title) +
    scale_y_continuous(labels = label_percent(scale = 1)) +
    labs(
      title = paste("Black Student Suspension Rates by Category and", title_suffix),
      subtitle = "Suspension rate per 100 Black students, by reason",
      x = "Academic Year",
      y = "Suspension Rate by Category (%)"
    ) +
    theme(
      legend.position = "bottom",
      plot.title = element_text(face = "bold", size = 14),
      axis.text.x = element_text(angle = 45, hjust = 1, size = 8),
      strip.text = element_text(face = "bold")
    )
}


# --- 5) Generate Plots --------------------------------------------------------
message("Generating plots...")

# Analysis by Black student proportion quartiles
p1_total_black <- create_total_rate_plot(black_students_data, "black_prop_q_label", black_quartile_colors,
                                         "School Black Student Proportion", "School % Black Students")
p2_categories_black <- create_category_rate_plot(black_students_data, "black_prop_q_label", black_quartile_colors,
                                                 "School Black Student Proportion", "School % Black Students")

# Analysis by White student proportion quartiles
p3_total_white <- create_total_rate_plot(black_students_data, "white_prop_q_label", white_quartile_colors,
                                         "School White Student Proportion", "School % White Students")
p4_categories_white <- create_category_rate_plot(black_students_data, "white_prop_q_label", white_quartile_colors,
                                                 "School White Student Proportion", "School % White Students")

# --- 6) Save Plots ------------------------------------------------------------
message("Saving plots...")
dir.create(here::here("outputs"), showWarnings = FALSE)

ggsave(here::here("outputs", "black_students_by_black_quartiles_total.png"),
       p1_total_black, width = 10, height = 7, dpi = 300)

ggsave(here::here("outputs", "black_students_by_black_quartiles_categories.png"),
       p2_categories_black, width = 12, height = 8, dpi = 300)

ggsave(here::here("outputs", "black_students_by_white_quartiles_total.png"),
       p3_total_white, width = 10, height = 7, dpi = 300)

ggsave(here::here("outputs", "black_students_by_white_quartiles_categories.png"),
       p4_categories_white, width = 12, height = 8, dpi = 300)

message("\n✓ Analysis complete! Check the 'outputs' folder.")