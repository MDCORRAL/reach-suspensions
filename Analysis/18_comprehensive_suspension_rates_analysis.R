# claude_R/18_comprehensive_suspension_rates_analysis.R
# R/26_comprehensive_suspension_rates_analysis.R
# Comprehensive analysis of suspension rates for all races/ethnicities
# by Black quartile, White quartile, traditional/non-traditional, and grade level
# Following REACH repository conventions

suppressPackageStartupMessages({
  library(dplyr); library(stringr); library(forcats); library(janitor)
  library(arrow); library(here); library(ggplot2); library(openxlsx)
  library(scales); library(tidyr); library(purrr); library(readr); library(ggrepel)
})
# Load repository utilities
source(here("R", "utils_keys_filters.R"))
message(">>> Running from project root: ", here::here())
# -------------------------------------------------------------------------
# Configuration and Helper Functions
# -------------------------------------------------------------------------

# Data paths
DATA_STAGE <- here("data-stage")
V6_LONG_PARQ <- file.path(DATA_STAGE, "susp_v6_long.parquet")
V6F_PARQ <- file.path(DATA_STAGE, "susp_v6_features.parquet")
stopifnot(file.exists(V6_LONG_PARQ), file.exists(V6F_PARQ))
v5 <- read_parquet(V6_LONG_PARQ)
v6_features <- read_parquet(V6F_PARQ)

# Output setup
RUN_TAG <- format(Sys.time(), "%Y%m%d_%H%M")
OUT_DIR <- here("outputs", paste0("comprehensive_rates_", RUN_TAG))
if (!dir.exists(OUT_DIR)) dir.create(OUT_DIR, recursive = TRUE)

# Helper functions from repository conventions
safe_div <- function(x, y) ifelse(y == 0 | is.na(y), NA_real_, x / y)

norm_quartile <- function(x) {
  x_clean <- str_replace_all(as.character(x), "\\s*\\([^)]*\\)|\\s+", "")
  x_clean <- str_to_upper(x_clean)
  case_when(
    x_clean %in% c("Q1", "1") ~ "Q1",
    x_clean %in% c("Q2", "2") ~ "Q2", 
    x_clean %in% c("Q3", "3") ~ "Q3",
    x_clean %in% c("Q4", "4") ~ "Q4",
    TRUE ~ NA_character_
  )
}

order_year <- function(x) {
  factor(x, levels = sort(unique(x)))
}

order_quartile <- function(x) {
  factor(x, levels = c("Q1", "Q2", "Q3", "Q4"))
}

#### codex/refactor-canon_race_label-and-labels
# canon_race_label sourced from R/utils_keys_filters.R

# Canonical race/ethnicity labels following repository conventions
canon_race_label <- function(x) {
  x_clean <- str_to_lower(str_trim(x))
  case_when(
    x_clean %in% c("ta", "total", "all students", "all_students") ~ "All Students",
    x_clean %in% c("ra", "asian") ~ "Asian",
    x_clean %in% c("rb", "black", "african american", "black/african american", "african_american") ~ "Black/African American",
    x_clean %in% c("rf", "filipino") ~ "Filipino", 
    x_clean %in% c("rh", "hispanic", "latino", "hispanic/latino", "hispanic_latino") ~ "Hispanic/Latino",
    x_clean %in% c("ri", "american indian", "alaska native", "american indian/alaska native", "native american") ~ "American Indian/Alaska Native",
    x_clean %in% c("rp", "pacific islander", "native hawaiian") ~ "Native Hawaiian/Pacific Islander",
    x_clean %in% c("rt", "two or more", "multiple", "two or more races", "multirace") ~ "Two or More Races",
    x_clean %in% c("rw", "white") ~ "White",
    str_detect(x_clean, "disabilit|special.{0,5}ed") ~ "Students with Disabilities",
    str_detect(x_clean, "english.{0,5}learn|ell") ~ "English Learner",
    str_detect(x_clean, "gender|male|female") ~ "By Gender",
    TRUE ~ NA_character_
  )
}
####main

# Color palette following repository conventions
reach_quartile_cols <- c(
  Q1 = "#0072B2",  # blue
  Q2 = "#E69F00",  # orange
  Q3 = "#009E73",  # green
  Q4 = "#D55E00"   # vermillion
)

reach_race_cols <- c(
  "All Students" = "#000000",
  "Asian" = "#1f77b4",
  "Black/African American" = "#ff7f0e", 
  "Filipino" = "#2ca02c",
  "Hispanic/Latino" = "#d62728",
  "American Indian/Alaska Native" = "#9467bd",
  "Native Hawaiian/Pacific Islander" = "#8c564b",
  "Two or More Races" = "#e377c2",
  "White" = "#7f7f7f",
  "Students with Disabilities" = "#bcbd22",
  "English Learner" = "#17becf"
)

# -------------------------------------------------------------------------
# Data Loading and Processing
# -------------------------------------------------------------------------

# DIRECT FIX: Get grade levels directly from v6 long
message("=== APPLYING DIRECT GRADE LEVEL FIX ===")

# Step 1: Get everything directly from v6 long (which has all the data we need)
v5_complete <- v5 %>%
  clean_names() %>%
  build_keys() %>%
  filter_campus_only() %>%
  transmute(
    school_code = school_code,
    year = as.character(academic_year),
    race_ethnicity = canon_race_label(subgroup),
    enrollment = as.numeric(cumulative_enrollment),
    total_suspensions = as.numeric(total_suspensions),
    undup_suspensions = as.numeric(unduplicated_count_of_students_suspended_total),
    white_q = norm_quartile(white_prop_q_label),
    black_q = norm_quartile(black_prop_q_label),
    school_level = school_level,
    grade_level = school_level
  ) %>%
  filter(
    !is.na(race_ethnicity),
    !is.na(enrollment),
    !is.na(total_suspensions),
    enrollment > 0,
    total_suspensions >= 0
  ) %>%
  mutate(
    suspension_rate = safe_div(total_suspensions, enrollment),
    undup_rate = safe_div(undup_suspensions, enrollment)
  )
# Step 2: Get traditional status from v6_features
v6_traditional <- v6_features %>%
  clean_names() %>%
  transmute(
    school_code = as.character(school_code),
    year = as.character(year),
    is_traditional = !is.na(is_traditional) & is_traditional
  ) %>%
  distinct()

# Step 3: Join and finalize
analytic_data <- v5_complete %>%
  left_join(v6_traditional, by = c("school_code", "year")) %>%
  mutate(
    is_traditional = ifelse(is.na(is_traditional), TRUE, is_traditional),
    setting = ifelse(is_traditional, "Traditional", "Non-traditional")
  ) %>%
  filter(!is.na(white_q), !is.na(black_q)) %>%
  mutate(
    year = order_year(year),
    black_q = order_quartile(black_q),
    white_q = order_quartile(white_q),
    setting = factor(setting, levels = c("Traditional", "Non-traditional")),
    grade_level = factor(grade_level, levels = c("Elementary", "Middle", "High", "K-12", "Alternative", "Other/Unknown"))
  )

# NOW add the diagnostic code:
unknown_schools <- analytic_data %>%
  filter(grade_level == "Other/Unknown") %>%
  count(school_level, sort = TRUE) %>%
  head(20)

message("Top school level values classified as Other/Unknown:")
print(unknown_schools)

# -------------------------------------------------------------------------
# Analysis Functions
# -------------------------------------------------------------------------

# Summary statistics by grouping variables
calc_summary_stats <- function(data, ...) {
  data %>%
    group_by(...) %>%
    summarise(
      n_schools = n_distinct(school_code),
      n_records = n(),
      total_enrollment = sum(enrollment, na.rm = TRUE),
      total_suspensions = sum(total_suspensions, na.rm = TRUE),
      mean_rate = mean(suspension_rate, na.rm = TRUE),
      median_rate = median(suspension_rate, na.rm = TRUE),
      pooled_rate = safe_div(total_suspensions, total_enrollment),
      .groups = "drop"
    ) %>%
    mutate(
      pooled_rate_pct = percent(pooled_rate, accuracy = 0.1),
      mean_rate_pct = percent(mean_rate, accuracy = 0.1),
      median_rate_pct = percent(median_rate, accuracy = 0.1)
    )
}
# -------------------------------------------------------------------------
# Comprehensive Analysis by All Dimensions
# -------------------------------------------------------------------------
major_races <- c("All Students", "Black/African American", "Hispanic/Latino", "White", "Asian")

# 1. Overall rates by race/ethnicity and year
rates_by_race_year <- calc_summary_stats(analytic_data, year, race_ethnicity)

# 2. Rates by Black quartile
rates_by_black_q <- calc_summary_stats(analytic_data, year, black_q, race_ethnicity)

# 3. Rates by White quartile  
rates_by_white_q <- calc_summary_stats(analytic_data, year, white_q, race_ethnicity)

# 4. Rates by traditional/non-traditional setting
rates_by_setting <- calc_summary_stats(analytic_data, year, setting, race_ethnicity)

# 5. Rates by grade level
rates_by_grade <- calc_summary_stats(analytic_data, year, grade_level, race_ethnicity)

# 6. Comprehensive cross-tabulation: All dimensions together
rates_comprehensive <- calc_summary_stats(
  analytic_data, 
  year, race_ethnicity, black_q, white_q, setting, grade_level)

#6extra_Quartile disparity summary
disparity_summary <- rates_by_black_q %>%
  filter(race_ethnicity %in% major_races) %>%
  select(year, race_ethnicity, black_q, pooled_rate) %>%
  pivot_wider(names_from = black_q, values_from = pooled_rate) %>%
  mutate(
    Q4_Q1_ratio = Q4/Q1,
    Q4_Q1_diff = Q4 - Q1
  )

# 7. Focused comparisons for key demographics
# Traditional schools only, comparing quartiles for major racial groups
key_races <- c("All Students", "Black/African American", "Hispanic/Latino", "White", "Asian")

rates_traditional_focus <- analytic_data %>%
  filter(
    setting == "Traditional",
    race_ethnicity %in% key_races
  ) %>%
  calc_summary_stats(year, black_q, white_q, race_ethnicity, grade_level)

# -------------------------------------------------------------------------
# Visualization Functions
# -------------------------------------------------------------------------

create_quartile_plot <- function(data, quartile_var, quartile_label, race_filter = NULL) {
  if (!is.null(race_filter)) {
    data <- data %>% filter(race_ethnicity %in% race_filter)
  }
  
  ggplot(data, aes(x = year, y = pooled_rate, 
                   color = !!sym(quartile_var), 
                   group = interaction(!!sym(quartile_var), race_ethnicity))) +
    geom_line(linewidth = 1.1) +
    geom_point(size = 2) +
    # ADD :
    geom_text_repel(aes(label = percent(pooled_rate, accuracy = 0.1)),
                    size = 3, box.padding = 0.25, point.padding = 0.2,
                    max.overlaps = 30, segment.color = "grey70", 
                    segment.size = 0.3, fontface = "bold") +
    facet_wrap(~ race_ethnicity, scales = "free_y") +
    scale_color_manual(values = reach_quartile_cols, name = quartile_label) +
    scale_y_continuous(labels = percent_format(accuracy = 0.1)) +
    labs(
      title = paste("Suspension Rates by", quartile_label, "and Race/Ethnicity"),
      subtitle = "Traditional schools only • Pooled rates within year × quartile × race",
      x = "Academic Year",
      y = "Suspension Rate"
    ) +
    theme_minimal(base_size = 11) +
    theme(
      axis.text.x = element_text(angle = 45, hjust = 1),
      strip.text = element_text(size = 9),
      legend.position = "bottom",
      # ADD THESE LINES:
      panel.background = element_rect(fill = "white", color = NA),
      plot.background = element_rect(fill = "white", color = NA),
      panel.grid.major = element_line(color = "grey90", linewidth = 0.3),
      panel.grid.minor = element_blank()
    )
}

# Create comparison plot by school setting
create_setting_plot <- function(data, race_filter = NULL) {
  if (!is.null(race_filter)) {
    data <- data %>% filter(race_ethnicity %in% race_filter)
  }
  
  ggplot(data, aes(x = year, y = pooled_rate, 
                   color = setting, group = interaction(setting, race_ethnicity))) +
    geom_line(linewidth = 1.1) +
    geom_point(size = 2) +
    # ADD THIS LINE:
    geom_text_repel(aes(label = percent(pooled_rate, accuracy = 0.1)),
                    size = 3, box.padding = 0.25, point.padding = 0.2,
                    max.overlaps = 30, segment.color = "grey70", 
                    segment.size = 0.3, fontface = "bold") +
    facet_wrap(~ race_ethnicity, scales = "free_y") +
    scale_color_manual(values = c("Traditional" = "#0072B2", "Non-traditional" = "#D55E00")) +
    scale_y_continuous(labels = percent_format(accuracy = 0.1)) +
    labs(
      title = "Suspension Rates: Traditional vs Non-traditional Schools",
      subtitle = "Pooled rates within year × setting × race",
      x = "Academic Year", 
      y = "Suspension Rate",
      color = "School Setting"
    ) +
    theme_minimal(base_size = 11) +
    theme(
      axis.text.x = element_text(angle = 45, hjust = 1),
      strip.text = element_text(size = 9),
      legend.position = "bottom",
      # ADD THESE LINES:
      panel.background = element_rect(fill = "white", color = NA),
      plot.background = element_rect(fill = "white", color = NA),
      panel.grid.major = element_line(color = "grey90", linewidth = 0.3),
      panel.grid.minor = element_blank()
    )
}
# -------------------------------------------------------------------------
# Generate Visualizations
# -------------------------------------------------------------------------
# -------------------------------------------------------------------------
# Generate Visualizations
# -------------------------------------------------------------------------

# Focus on major racial/ethnic groups for clarity
major_races <- c("All Students", "Black/African American", "Hispanic/Latino", "White", "Asian")

# Plot 1: Rates by Black quartile
p1_black_q <- rates_by_black_q %>%
  filter(race_ethnicity %in% major_races) %>%
  create_quartile_plot("black_q", "Black Enrollment Quartile")

# Plot 2: Rates by White quartile  
p2_white_q <- rates_by_white_q %>%
  filter(race_ethnicity %in% major_races) %>%
  create_quartile_plot("white_q", "White Enrollment Quartile")

# Plot 3: Traditional vs Non-traditional
p3_setting <- rates_by_setting %>%
  filter(race_ethnicity %in% major_races) %>%
  create_setting_plot()

# Grade_level_plots:
create_grade_level_plot <- function() {
  plot_data <- analytic_data %>%
    filter(
      race_ethnicity == "Black/African American",
      setting == "Traditional",
      grade_level %in% c("Elementary", "Middle", "High", "K-12")
    ) %>%
    calc_summary_stats(year, grade_level, black_q)
  
  if (nrow(plot_data) == 0) {
    message("Warning: No data for Black students grade level plot after filtering")
    return(ggplot() + 
             annotate("text", x = 0.5, y = 0.5, label = "No data available after filtering", size = 6) +
             labs(title = "Black Student Suspension Rates by Grade Level",
                  subtitle = "No data available after filtering") +
             theme_void())
  }
  
  # Check what grade levels we actually have
  available_grades <- unique(plot_data$grade_level)
  message("Creating grade level plot with: ", paste(available_grades, collapse = ", "))
  
  ggplot(plot_data, aes(x = year, y = pooled_rate, color = black_q, group = black_q)) +
    geom_line(linewidth = 1.1) +
    geom_point(size = 2) +
    facet_wrap(~ grade_level) +
    scale_color_manual(values = reach_quartile_cols, name = "Black Enrollment\nQuartile") +
    scale_y_continuous(labels = percent_format(accuracy = 0.1)) +
    labs(
      title = "Black Student Suspension Rates by Grade Level and Black Enrollment Quartile",
      subtitle = "Traditional schools only",
      x = "Academic Year",
      y = "Suspension Rate"
    ) +
    theme_minimal(base_size = 11) +
    theme(
      axis.text.x = element_text(angle = 45, hjust = 1),
      legend.position = "bottom"
    )
}

# Plot 4: Grade level comparison for Black students in Traditional schools
p4_grade_black <- create_grade_level_plot()

# Export Results
# -------------------------------------------------------------------------

# Save plots
ggsave(file.path(OUT_DIR, "rates_by_black_quartile.png"), p1_black_q, 
       width = 12, height = 8, dpi = 300)
ggsave(file.path(OUT_DIR, "rates_by_white_quartile.png"), p2_white_q, 
       width = 12, height = 8, dpi = 300)  
ggsave(file.path(OUT_DIR, "rates_by_setting.png"), p3_setting,
       width = 12, height = 8, dpi = 300)
ggsave(file.path(OUT_DIR, "black_rates_by_grade_level.png"), p4_grade_black,
       width = 12, height = 6, dpi = 300)

# Create comprehensive Excel workbook
wb <- createWorkbook()

# Sheet 1: Overall rates by race and year
addWorksheet(wb, "rates_by_race_year")
writeData(wb, "rates_by_race_year", rates_by_race_year)

# Sheet 2: Rates by Black quartile
addWorksheet(wb, "rates_by_black_quartile")  
writeData(wb, "rates_by_black_quartile", rates_by_black_q)

# Sheet 3: Rates by White quartile
addWorksheet(wb, "rates_by_white_quartile")
writeData(wb, "rates_by_white_quartile", rates_by_white_q)

# Sheet 4: Rates by school setting
addWorksheet(wb, "rates_by_setting")
writeData(wb, "rates_by_setting", rates_by_setting)

# Sheet 5: Rates by grade level
addWorksheet(wb, "rates_by_grade_level")
writeData(wb, "rates_by_grade_level", rates_by_grade)

# Sheet 6: Comprehensive cross-tab (filtered for major races to avoid huge file)
addWorksheet(wb, "comprehensive_analysis")
rates_comprehensive_filtered <- rates_comprehensive %>%
  filter(race_ethnicity %in% major_races, n_records >= 5) # minimum threshold for reliability
writeData(wb, "comprehensive_analysis", rates_comprehensive_filtered)

# Sheet 7: Traditional schools focus analysis
addWorksheet(wb, "traditional_schools_focus")
writeData(wb, "traditional_schools_focus", rates_traditional_focus)

# Sheet 8: Summary statistics
summary_stats <- list(
  total_records = nrow(analytic_data),
  unique_schools = n_distinct(analytic_data$school_code),
  years_covered = length(unique(analytic_data$year)),
  race_categories = length(unique(analytic_data$race_ethnicity)),
  black_q_schools = analytic_data %>% count(black_q, name = "n_schools"),
  white_q_schools = analytic_data %>% count(white_q, name = "n_schools"),
  setting_schools = analytic_data %>% count(setting, name = "n_schools"),
  grade_schools = analytic_data %>% count(grade_level, name = "n_schools")
)

addWorksheet(wb, "summary_statistics")
writeData(wb, "summary_statistics", 
          data.frame(
            metric = names(summary_stats)[1:4],
            value = c(summary_stats$total_records, summary_stats$unique_schools, 
                      summary_stats$years_covered, summary_stats$race_categories)
          ), startRow = 1)

# Save Excel file
excel_path <- file.path(OUT_DIR, "comprehensive_suspension_rates_analysis.xlsx")
saveWorkbook(wb, excel_path, overwrite = TRUE)

# Create summary report
cat("COMPREHENSIVE SUSPENSION RATES ANALYSIS COMPLETE\n")
cat("===============================================\n\n")
cat("Output directory:", OUT_DIR, "\n\n")
cat("Files created:\n")
cat("- rates_by_black_quartile.png\n")
cat("- rates_by_white_quartile.png\n") 
cat("- rates_by_setting.png\n")
cat("- black_rates_by_grade_level.png\n")
cat("- comprehensive_suspension_rates_analysis.xlsx\n\n")
cat("Dataset summary:\n")
cat("- Total records:", nrow(analytic_data), "\n")
cat("- Unique schools:", n_distinct(analytic_data$school_code), "\n")
cat("- Years covered:", paste(sort(unique(analytic_data$year)), collapse = ", "), "\n")
cat("- Race/ethnicity categories:", length(unique(analytic_data$race_ethnicity)), "\n")
cat("- Black quartile distribution:\n")
print(table(analytic_data$black_q, useNA = "always"))
cat("- White quartile distribution:\n") 
print(table(analytic_data$white_q, useNA = "always"))
cat("- Setting distribution:\n")
print(table(analytic_data$setting, useNA = "always"))
cat("- Grade level distribution:\n")
print(table(analytic_data$grade_level, useNA = "always"))

message("\nAnalysis complete. Check output directory: ", OUT_DIR)