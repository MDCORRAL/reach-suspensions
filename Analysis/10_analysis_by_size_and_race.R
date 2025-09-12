# R/10_analysis_by_size_and_race_individual_plots.R
# Analysis of suspension rates by race, creating a separate plot for each school enrollment quartile.

# --- 1) Setup -----------------------------------------------------------------
suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
  library(ggplot2)
  library(scales)
  library(ggrepel)
})

# Load helper functions
source(here::here("R", "utils_keys_filters.R"))

# --- 2) Load Data -------------------------------------------------------------
message("Loading data...")
v5 <- arrow::read_parquet(here::here("data-stage", "susp_v6_long.parquet"))

# --- 3) Prepare Data for Plotting ---------------------------------------------
message("Preparing data for analysis...")

rates_by_size_race <- v5 %>%
  filter(enroll_q_label != "Unknown", !is.na(enroll_q_label)) %>%
  filter(subgroup != "All Students") %>%
  mutate(student_group = canon_race_label(coalesce(subgroup, reporting_category))) %>%
  group_by(academic_year, enroll_q_label, student_group) %>%
  summarise(
    total_suspensions = sum(total_suspensions, na.rm = TRUE),
    cumulative_enrollment = sum(cumulative_enrollment, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    suspension_rate = if_else(cumulative_enrollment > 0, (total_suspensions / cumulative_enrollment) * 100, 0)
  ) 

# --- 4) Create and Save Individual Plots --------------------------------------

# Get the list of unique quartiles to loop through
enrollment_quartiles <- unique(rates_by_size_race$enroll_q_label)

for (quartile in enrollment_quartiles) {
  
  message(paste("Generating plot for:", quartile))
  
  # Filter data for the current quartile
  plot_data <- rates_by_size_race %>%
    filter(enroll_q_label == quartile)
  
  # Create the plot
  p <- ggplot(plot_data, aes(x = academic_year, y = suspension_rate, group = student_group, color = student_group)) +
    geom_line(linewidth = 1.2) +
    geom_point(size = 2.5) +
    geom_text_repel(
      aes(label = round(suspension_rate, 1)),
      color = "black",
      size = 3.5,
      fontface = "bold",
      segment.color = 'grey50',
      box.padding = 0.5,
      max.overlaps = Inf
    ) +
    scale_y_continuous(labels = label_percent(scale = 1), expand = expansion(mult = c(0.05, 0.15))) +
    labs(
      title = paste("Suspension Rates by Race in", quartile, "Schools"),
      subtitle = "Comparing suspension rates (per 100 students) over time.",
      x = "Academic Year",
      y = "Suspension Rate (%)",
      color = "Student Group"
    ) +
    theme_minimal(base_size = 14) +
    theme(
      legend.position = "bottom",
      plot.title = element_text(face = "bold", size = 18),
      axis.text.x = element_text(angle = 45, hjust = 1)
    )
  
  # Create a clean filename
  file_name <- paste0("suspension_rates_by_race_", tolower(gsub("[^A-Za-z0-9]", "_", quartile)), ".png")
  
  # Save the plot
  ggsave(here::here("outputs", file_name),
         plot = p,
         width = 12, height = 8, dpi = 300)
  
}

message("\n✓ Analysis complete! Check the 'outputs' folder for the four new plot files.")