# Analysis/22_black_suspension_rates_teacher_demographics.R
# Analysis of Black student suspension rates by enrollment quartiles with teacher demographics.
#
# Purpose:
#   1. Examine suspension rates by Black enrollment quartiles across years
#      with teacher and administrator racial demographics
#   2. Identify schools with highest Black student suspension rates within each quartile
#      and examine their teacher/administrator demographics
#
# Input: susp_v6_teacher_long.parquet (merged student + teacher data)
# Output:
#   - tables/22_black_suspension_by_quartile_year_teacher.csv
#   - tables/22_high_suspension_schools_teacher_demographics.csv
#   - graphs/22_black_suspension_rates_by_quartile.png
#   - graphs/22_teacher_demographics_high_suspension_schools.png

# === 1) Setup =================================================================
suppressPackageStartupMessages({
  library(here); library(arrow); library(dplyr); library(tidyr)
  library(ggplot2); library(scales); library(patchwork); library(writexl)
})

try(here::i_am("Analysis/22_black_suspension_rates_teacher_demographics.R"), silent = TRUE)

# Load canonical definitions
source(here::here("R", "utils_keys_filters.R"))

# Safe division helper (handles zero denominators)
safe_div <- function(num, denom, replace_na_with = NA_real_) {
  ifelse(denom == 0 | is.na(denom), replace_na_with, num / denom)
}

theme_set(theme_minimal(base_size = 12))

# Color palette for quartiles (use canonical Black quartile colors)
black_quartile_colors <- setNames(
  c("#FEE5D9", "#FCAE91", "#FB6A4A", "#CB181D"),
  get_quartile_label(1:4, "Black")
)

# === 2) Load and validate data ================================================
message("=== 22: Black Suspension Rates with Teacher Demographics ===")

# Check for merged teacher-student data (try multiple possible filenames)
V6_TEACHER_PATH <- here::here("data-stage", "susp_v6_teacher_long.parquet")
if (!file.exists(V6_TEACHER_PATH)) {
  # Try alternative naming convention
  V6_TEACHER_PATH <- here::here("data-stage", "susp_v6_teacher_features.parquet")
  if (!file.exists(V6_TEACHER_PATH)) {
    stop("Missing merged teacher-student data. Expected one of:\n",
         "  - data-stage/susp_v6_teacher_long.parquet\n",
         "  - data-stage/susp_v6_teacher_features.parquet\n",
         "Run Analysis/18_merge_teacher_student.R first.")
  }
}

message(">>> Loading merged student-teacher data from: ", basename(V6_TEACHER_PATH))
df_raw <- arrow::read_parquet(V6_TEACHER_PATH) %>%
  janitor::clean_names() %>%
  build_keys()

# Apply campus filter if aggregate_level exists, otherwise just filter special codes
if ("aggregate_level" %in% names(df_raw)) {
  df_raw <- df_raw %>% filter_campus_only()
} else {
  message(">>> Note: aggregate_level not found; filtering only special school codes")
  df_raw <- df_raw %>% filter(!school_code %in% SPECIAL_SCHOOL_CODES)
}

# Check required columns
required_cols <- c(
  "academic_year", "cds_school", "subgroup", "category_type",
  "cumulative_enrollment", "total_suspensions", "black_prop_q"
)
missing <- setdiff(required_cols, names(df_raw))
if (length(missing)) {
  stop("Missing required columns: ", paste(missing, collapse = ", "))
}

# Check for unduplicated suspensions column (optional but recommended)
has_unduplicated <- "unduplicated_suspensions" %in% names(df_raw)
if (!has_unduplicated) {
  message(">>> WARNING: unduplicated_suspensions column not found.")
  message(">>> Only raw suspension rates (events) will be calculated.")
  message(">>> To include unduplicated rates, ensure data includes unduplicated_suspensions column.")
} else {
  message(">>> Found unduplicated_suspensions column - will calculate both event and student rates")
}

# Identify teacher columns
teacher_cols <- grep("^teacher_", names(df_raw), value = TRUE)
if (!length(teacher_cols)) {
  stop("No teacher_* columns found. Check merge in Analysis/18_merge_teacher_student.R")
}

message(">>> Found ", length(teacher_cols), " teacher demographic columns")

# === 3) ANALYSIS 1: Suspension rates by Black enrollment quartiles with teacher demographics ===
message("\n>>> ANALYSIS 1: Suspension rates by Black enrollment quartiles across years")

# Filter to Black students only
black_students <- df_raw %>%
  filter(
    category_type == "Race/Ethnicity",
    canon_race_label(subgroup) == "Black/African American",
    !is.na(black_prop_q),
    !is.na(cumulative_enrollment),
    cumulative_enrollment > 0
  )

# Add readable quartile labels
black_students <- black_students %>%
  mutate(black_prop_q_label = get_quartile_label(black_prop_q, "Black"))

message(">>> Filtered to ", nrow(black_students), " Black student records")

# Identify teacher race columns (both totals and by staff type)
teacher_race_cols <- grep(
  "^teacher_staff_count_(african_american|american_indian|asian|filipino|hispanic_or_latino|pacific_islander|white|two_or_more|not_reported)($|_share$)",
  names(black_students),
  value = TRUE,
  perl = TRUE
)

# Also include staff type breakdowns (teachers, administrators)
teacher_type_cols <- grep(
  "^teacher_staff_count_total_by_type_(teachers|administrators)",
  names(black_students),
  value = TRUE,
  perl = TRUE
)

# Race by staff type columns
teacher_race_by_type_cols <- grep(
  "^teacher_staff_count_by_type_(teachers|administrators)_(african_american|american_indian|asian|filipino|hispanic_or_latino|pacific_islander|white|two_or_more|not_reported)($|_share$)",
  names(black_students),
  value = TRUE,
  perl = TRUE
)

all_teacher_cols <- unique(c("teacher_staff_count_total", teacher_race_cols, teacher_type_cols, teacher_race_by_type_cols))

message(">>> Identified ", length(all_teacher_cols), " teacher demographic columns to aggregate")

# Aggregate by quartile and year
# Method: Sum counts first, then calculate rates (weighted approach)
if (has_unduplicated) {
  quartile_year_summary <- black_students %>%
    group_by(academic_year, black_prop_q, black_prop_q_label) %>%
    summarise(
      # School counts
      n_schools = n_distinct(cds_school),

      # Student metrics (aggregated)
      total_black_students = sum(cumulative_enrollment, na.rm = TRUE),
      total_black_suspensions = sum(total_suspensions, na.rm = TRUE),
      total_black_students_suspended_unduplicated = sum(unduplicated_suspensions, na.rm = TRUE),

      # Teacher metrics (aggregated counts) - only non-share columns
      across(
        all_of(grep("_share$", all_teacher_cols, value = TRUE, invert = TRUE)),
        ~ sum(.x, na.rm = TRUE),
        .names = "{.col}_sum"
      ),

      # Distribution metrics
      median_black_enrollment = median(cumulative_enrollment, na.rm = TRUE),
      mean_black_enrollment = mean(cumulative_enrollment, na.rm = TRUE),

      .groups = "drop"
    ) %>%
    mutate(
      # Calculate weighted Black student suspension rates
      # Events rate: can exceed 100% if students suspended multiple times
      black_suspension_rate_events = safe_div(total_black_suspensions, total_black_students),
      # Students rate: unduplicated, always ≤ 100%
      black_suspension_rate_students = safe_div(total_black_students_suspended_unduplicated, total_black_students)
    ) %>%
    arrange(academic_year, black_prop_q)
} else {
  # Fallback if unduplicated data not available
  quartile_year_summary <- black_students %>%
    group_by(academic_year, black_prop_q, black_prop_q_label) %>%
    summarise(
      # School counts
      n_schools = n_distinct(cds_school),

      # Student metrics (aggregated)
      total_black_students = sum(cumulative_enrollment, na.rm = TRUE),
      total_black_suspensions = sum(total_suspensions, na.rm = TRUE),

      # Teacher metrics (aggregated counts) - only non-share columns
      across(
        all_of(grep("_share$", all_teacher_cols, value = TRUE, invert = TRUE)),
        ~ sum(.x, na.rm = TRUE),
        .names = "{.col}_sum"
      ),

      # Distribution metrics
      median_black_enrollment = median(cumulative_enrollment, na.rm = TRUE),
      mean_black_enrollment = mean(cumulative_enrollment, na.rm = TRUE),

      .groups = "drop"
    ) %>%
    mutate(
      # Calculate weighted Black student suspension rate (events only)
      black_suspension_rate_events = safe_div(total_black_suspensions, total_black_students)
    ) %>%
    arrange(academic_year, black_prop_q)
}

# Calculate teacher percentages from aggregated counts
if ("teacher_staff_count_total_sum" %in% names(quartile_year_summary)) {
  # Overall teacher race percentages
  race_count_cols <- grep("^teacher_staff_count_(african_american|white|hispanic_or_latino|asian)_sum$",
                          names(quartile_year_summary), value = TRUE)

  for (col in race_count_cols) {
    pct_col <- sub("_sum$", "_pct", col)
    quartile_year_summary[[pct_col]] <-
      safe_div(quartile_year_summary[[col]], quartile_year_summary$teacher_staff_count_total_sum) * 100
  }

  # Staff type totals as percentages
  for (staff_type in c("teachers", "administrators")) {
    total_col <- paste0("teacher_staff_count_total_by_type_", staff_type, "_sum")
    if (total_col %in% names(quartile_year_summary)) {
      pct_col <- sub("_sum$", "_pct", total_col)
      quartile_year_summary[[pct_col]] <-
        safe_div(quartile_year_summary[[total_col]], quartile_year_summary$teacher_staff_count_total_sum) * 100
    }

    # Race by staff type percentages
    race_by_type_cols <- grep(
      paste0("^teacher_staff_count_by_type_", staff_type, "_(african_american|white|hispanic_or_latino|asian)_sum$"),
      names(quartile_year_summary), value = TRUE
    )

    for (col in race_by_type_cols) {
      # Calculate as percentage of that staff type
      staff_total_col <- paste0("teacher_staff_count_total_by_type_", staff_type, "_sum")
      if (staff_total_col %in% names(quartile_year_summary)) {
        pct_col <- sub("_sum$", "_pct", col)
        quartile_year_summary[[pct_col]] <-
          safe_div(quartile_year_summary[[col]], quartile_year_summary[[staff_total_col]]) * 100
      }
    }
  }
}

message(">>> Quartile-year summary computed for ", nrow(quartile_year_summary), " combinations")
message(">>> Years covered: ", paste(sort(unique(quartile_year_summary$academic_year)), collapse = ", "))

# === 4) ANALYSIS 2: High suspension rate schools by quartile and year ===
message("\n>>> ANALYSIS 2: Identifying high suspension schools within each quartile")

# Calculate school-level Black student suspension rates
if (has_unduplicated) {
  school_level <- black_students %>%
    group_by(academic_year, cds_school, black_prop_q, black_prop_q_label) %>%
    summarise(
      school_black_enrollment = sum(cumulative_enrollment, na.rm = TRUE),
      school_black_suspensions = sum(total_suspensions, na.rm = TRUE),
      school_black_students_suspended_unduplicated = sum(unduplicated_suspensions, na.rm = TRUE),

      # Keep teacher demographics (should be consistent within school-year)
      across(
        all_of(grep("_share$", all_teacher_cols, value = TRUE, invert = TRUE)),
        ~ first(.x),
        .names = "{.col}"
      ),

      .groups = "drop"
    ) %>%
    mutate(
      school_black_suspension_rate_events = safe_div(school_black_suspensions, school_black_enrollment),
      school_black_suspension_rate_students = safe_div(school_black_students_suspended_unduplicated, school_black_enrollment)
    ) %>%
    filter(
      !is.na(school_black_suspension_rate_events),
      school_black_enrollment >= 10  # Minimum threshold for stable rates
    )
} else {
  school_level <- black_students %>%
    group_by(academic_year, cds_school, black_prop_q, black_prop_q_label) %>%
    summarise(
      school_black_enrollment = sum(cumulative_enrollment, na.rm = TRUE),
      school_black_suspensions = sum(total_suspensions, na.rm = TRUE),

      # Keep teacher demographics (should be consistent within school-year)
      across(
        all_of(grep("_share$", all_teacher_cols, value = TRUE, invert = TRUE)),
        ~ first(.x),
        .names = "{.col}"
      ),

      .groups = "drop"
    ) %>%
    mutate(
      school_black_suspension_rate_events = safe_div(school_black_suspensions, school_black_enrollment)
    ) %>%
    filter(
      !is.na(school_black_suspension_rate_events),
      school_black_enrollment >= 10  # Minimum threshold for stable rates
    )
}

message(">>> Computed school-level rates for ", nrow(school_level), " school-year observations")

# For each quartile-year, identify schools in top decile of Black suspension rates
# (Q4 = schools with highest proportion of Black students)
# Use events rate for ranking (more stringent measure)
high_suspension_schools <- school_level %>%
  group_by(academic_year, black_prop_q, black_prop_q_label) %>%
  mutate(
    suspension_rate_percentile_events = percent_rank(school_black_suspension_rate_events) * 100,
    is_top_decile = suspension_rate_percentile_events >= 90
  ) %>%
  filter(is_top_decile) %>%
  ungroup() %>%
  arrange(academic_year, black_prop_q, desc(school_black_suspension_rate_events))

message(">>> Identified ", nrow(high_suspension_schools), " schools in top decile of Black suspension rates")

# Aggregate teacher demographics for high suspension schools by quartile-year
if (has_unduplicated) {
  high_suspension_teacher_summary <- high_suspension_schools %>%
    group_by(academic_year, black_prop_q, black_prop_q_label) %>%
    summarise(
      n_high_suspension_schools = n(),

      # Suspension metrics - Events (total suspensions)
      avg_black_suspension_rate_events = mean(school_black_suspension_rate_events, na.rm = TRUE),
      median_black_suspension_rate_events = median(school_black_suspension_rate_events, na.rm = TRUE),
      max_black_suspension_rate_events = max(school_black_suspension_rate_events, na.rm = TRUE),

      # Suspension metrics - Students (unduplicated)
      avg_black_suspension_rate_students = mean(school_black_suspension_rate_students, na.rm = TRUE),
      median_black_suspension_rate_students = median(school_black_suspension_rate_students, na.rm = TRUE),
      max_black_suspension_rate_students = max(school_black_suspension_rate_students, na.rm = TRUE),

      # Student metrics
      total_black_students = sum(school_black_enrollment, na.rm = TRUE),
      total_black_suspensions = sum(school_black_suspensions, na.rm = TRUE),
      total_black_students_suspended_unduplicated = sum(school_black_students_suspended_unduplicated, na.rm = TRUE),

      # Teacher demographics (aggregated counts)
      across(
        all_of(grep("_share$", all_teacher_cols, value = TRUE, invert = TRUE)),
        ~ sum(.x, na.rm = TRUE),
        .names = "{.col}_sum"
      ),

      .groups = "drop"
    ) %>%
    arrange(academic_year, black_prop_q)
} else {
  high_suspension_teacher_summary <- high_suspension_schools %>%
    group_by(academic_year, black_prop_q, black_prop_q_label) %>%
    summarise(
      n_high_suspension_schools = n(),

      # Suspension metrics - Events only
      avg_black_suspension_rate_events = mean(school_black_suspension_rate_events, na.rm = TRUE),
      median_black_suspension_rate_events = median(school_black_suspension_rate_events, na.rm = TRUE),
      max_black_suspension_rate_events = max(school_black_suspension_rate_events, na.rm = TRUE),

      # Student metrics
      total_black_students = sum(school_black_enrollment, na.rm = TRUE),
      total_black_suspensions = sum(school_black_suspensions, na.rm = TRUE),

      # Teacher demographics (aggregated counts)
      across(
        all_of(grep("_share$", all_teacher_cols, value = TRUE, invert = TRUE)),
        ~ sum(.x, na.rm = TRUE),
        .names = "{.col}_sum"
      ),

      .groups = "drop"
    ) %>%
    arrange(academic_year, black_prop_q)
}

# Calculate teacher percentages for high suspension schools
if ("teacher_staff_count_total_sum" %in% names(high_suspension_teacher_summary)) {
  # Overall teacher race percentages
  race_count_cols <- grep("^teacher_staff_count_(african_american|white|hispanic_or_latino|asian)_sum$",
                          names(high_suspension_teacher_summary), value = TRUE)

  for (col in race_count_cols) {
    pct_col <- sub("_sum$", "_pct", col)
    high_suspension_teacher_summary[[pct_col]] <-
      safe_div(high_suspension_teacher_summary[[col]], high_suspension_teacher_summary$teacher_staff_count_total_sum) * 100
  }

  # Staff type totals as percentages
  for (staff_type in c("teachers", "administrators")) {
    total_col <- paste0("teacher_staff_count_total_by_type_", staff_type, "_sum")
    if (total_col %in% names(high_suspension_teacher_summary)) {
      pct_col <- sub("_sum$", "_pct", total_col)
      high_suspension_teacher_summary[[pct_col]] <-
        safe_div(high_suspension_teacher_summary[[total_col]], high_suspension_teacher_summary$teacher_staff_count_total_sum) * 100
    }

    # Race by staff type percentages
    race_by_type_cols <- grep(
      paste0("^teacher_staff_count_by_type_", staff_type, "_(african_american|white|hispanic_or_latino|asian)_sum$"),
      names(high_suspension_teacher_summary), value = TRUE
    )

    for (col in race_by_type_cols) {
      staff_total_col <- paste0("teacher_staff_count_total_by_type_", staff_type, "_sum")
      if (staff_total_col %in% names(high_suspension_teacher_summary)) {
        pct_col <- sub("_sum$", "_pct", col)
        high_suspension_teacher_summary[[pct_col]] <-
          safe_div(high_suspension_teacher_summary[[col]], high_suspension_teacher_summary[[staff_total_col]]) * 100
      }
    }
  }
}

message(">>> High suspension school teacher demographics computed")

# === 5) Save summary tables ===================================================
message("\n>>> Saving summary tables...")

dir.create(here::here("outputs", "tables"), showWarnings = FALSE, recursive = TRUE)

# Table 1: Suspension rates by quartile-year with teacher demographics
write.csv(
  quartile_year_summary,
  here::here("outputs", "tables", "22_black_suspension_by_quartile_year_teacher.csv"),
  row.names = FALSE
)
message(">>> Saved: outputs/tables/22_black_suspension_by_quartile_year_teacher.csv")

# Table 2: High suspension schools with teacher demographics
write.csv(
  high_suspension_teacher_summary,
  here::here("outputs", "tables", "22_high_suspension_schools_teacher_demographics.csv"),
  row.names = FALSE
)
message(">>> Saved: outputs/tables/22_high_suspension_schools_teacher_demographics.csv")

# Table 3: Detailed list of high suspension schools
if (has_unduplicated) {
  write.csv(
    high_suspension_schools %>%
      select(academic_year, cds_school, black_prop_q_label,
             school_black_enrollment, school_black_suspensions,
             school_black_students_suspended_unduplicated,
             school_black_suspension_rate_events,
             school_black_suspension_rate_students,
             suspension_rate_percentile_events,
             starts_with("teacher_staff_count")),
    here::here("outputs", "tables", "22_high_suspension_schools_detailed.csv"),
    row.names = FALSE
  )
} else {
  write.csv(
    high_suspension_schools %>%
      select(academic_year, cds_school, black_prop_q_label,
             school_black_enrollment, school_black_suspensions,
             school_black_suspension_rate_events,
             suspension_rate_percentile_events,
             starts_with("teacher_staff_count")),
    here::here("outputs", "tables", "22_high_suspension_schools_detailed.csv"),
    row.names = FALSE
  )
}
message(">>> Saved: outputs/tables/22_high_suspension_schools_detailed.csv")

# Export to Excel for easier exploration
if (has_unduplicated) {
  write_xlsx(
    list(
      "Quartile_Year_Summary" = quartile_year_summary,
      "High_Suspension_Summary" = high_suspension_teacher_summary,
      "High_Suspension_Schools" = high_suspension_schools %>%
        select(academic_year, cds_school, black_prop_q_label,
               school_black_enrollment, school_black_suspensions,
               school_black_students_suspended_unduplicated,
               school_black_suspension_rate_events,
               school_black_suspension_rate_students,
               starts_with("teacher_staff_count_total"))
    ),
    here::here("outputs", "tables", "22_black_suspension_teacher_analysis.xlsx")
  )
} else {
  write_xlsx(
    list(
      "Quartile_Year_Summary" = quartile_year_summary,
      "High_Suspension_Summary" = high_suspension_teacher_summary,
      "High_Suspension_Schools" = high_suspension_schools %>%
        select(academic_year, cds_school, black_prop_q_label,
               school_black_enrollment, school_black_suspensions,
               school_black_suspension_rate_events,
               starts_with("teacher_staff_count_total"))
    ),
    here::here("outputs", "tables", "22_black_suspension_teacher_analysis.xlsx")
  )
}
message(">>> Saved: outputs/tables/22_black_suspension_teacher_analysis.xlsx")

# === 6) Visualizations ========================================================
message("\n>>> Creating visualizations...")

dir.create(here::here("outputs", "graphs"), showWarnings = FALSE, recursive = TRUE)

# Plot 1: Black suspension rates by quartile over time
# Use events rate for visualization (more conservative, can exceed 100%)
plot1_rate_col <- if (has_unduplicated) "black_suspension_rate_events" else "black_suspension_rate_events"
plot1_rate_label <- if (has_unduplicated) "Events per Student" else "Suspensions per Student"
plot1_subtitle <- if (has_unduplicated) {
  "Suspension events per student (can exceed 100% if students suspended multiple times)"
} else {
  "Weighted rates: Sum of suspensions ÷ Sum of enrollment"
}

p1 <- quartile_year_summary %>%
  filter(black_prop_q_label != "Unknown") %>%
  ggplot(aes(x = academic_year, y = .data[[plot1_rate_col]],
             color = black_prop_q_label, group = black_prop_q_label)) +
  geom_line(linewidth = 1.2) +
  geom_point(size = 2.5) +
  geom_text(
    aes(label = scales::percent(.data[[plot1_rate_col]], accuracy = 0.1)),
    color = "black", size = 2.8, vjust = -0.8,
    show.legend = FALSE
  ) +
  scale_color_manual(values = black_quartile_colors, name = "School Black Student Proportion") +
  scale_y_continuous(
    labels = percent_format(accuracy = 0.1),
    expand = expansion(mult = c(0.05, 0.15))
  ) +
  labs(
    title = "Black Student Suspension Rates by School Racial Composition",
    subtitle = plot1_subtitle,
    x = "Academic Year",
    y = paste("Black Student Suspension Rate -", plot1_rate_label),
    caption = "Note: Schools grouped by quartile of Black student enrollment share"
  ) +
  theme(
    legend.position = "bottom",
    plot.title = element_text(face = "bold", size = 14),
    axis.text.x = element_text(angle = 45, hjust = 1)
  )

ggsave(
  here::here("outputs", "graphs", "22_black_suspension_rates_by_quartile.png"),
  p1, width = 12, height = 8, dpi = 300, bg = "white"
)
message(">>> Saved: outputs/graphs/22_black_suspension_rates_by_quartile.png")

# Plot 2: Teacher demographics comparison - All schools vs High suspension schools
# Prepare data for comparison
comparison_data <- bind_rows(
  quartile_year_summary %>%
    select(academic_year, black_prop_q_label,
           matches("^teacher_staff_count_(african_american|white|hispanic_or_latino)_pct$")) %>%
    mutate(school_group = "All Schools"),
  high_suspension_teacher_summary %>%
    select(academic_year, black_prop_q_label,
           matches("^teacher_staff_count_(african_american|white|hispanic_or_latino)_pct$")) %>%
    mutate(school_group = "High Suspension Schools")
)

# Check if we have teacher race data
has_teacher_race_pct <- any(grepl("^teacher_staff_count_.*_pct$", names(comparison_data)))

if (has_teacher_race_pct) {
  # Average across years for cleaner visualization
  comparison_avg <- comparison_data %>%
    group_by(black_prop_q_label, school_group) %>%
    summarise(
      across(matches("_pct$"), ~ mean(.x, na.rm = TRUE)),
      .groups = "drop"
    ) %>%
    filter(black_prop_q_label != "Unknown") %>%
    pivot_longer(
      cols = matches("_pct$"),
      names_to = "race_group",
      values_to = "percentage"
    ) %>%
    mutate(
      race_group = case_when(
        grepl("african_american", race_group) ~ "African American",
        grepl("white", race_group) ~ "White",
        grepl("hispanic_or_latino", race_group) ~ "Hispanic/Latino",
        TRUE ~ race_group
      )
    )

  p2 <- comparison_avg %>%
    ggplot(aes(x = black_prop_q_label, y = percentage, fill = school_group)) +
    geom_col(position = "dodge", alpha = 0.8) +
    geom_text(
      aes(label = sprintf("%.1f%%", percentage)),
      position = position_dodge(width = 0.9),
      vjust = -0.5,
      size = 2.8
    ) +
    facet_wrap(~ race_group, ncol = 3) +
    scale_fill_manual(
      values = c("All Schools" = "#4DAF4A", "High Suspension Schools" = "#E41A1C"),
      name = "School Group"
    ) +
    scale_y_continuous(
      labels = function(x) paste0(x, "%"),
      expand = expansion(mult = c(0, 0.15))
    ) +
    labs(
      title = "Teacher Race/Ethnicity: All Schools vs High Suspension Schools",
      subtitle = "Averaged across 2018-19 onwards | High suspension = top 10% within quartile",
      x = "School Black Student Proportion Quartile",
      y = "Percentage of All Staff",
      caption = "Note: Weighted averages. High suspension schools are those in top decile of Black suspension rates within their quartile."
    ) +
    theme(
      legend.position = "bottom",
      plot.title = element_text(face = "bold", size = 14),
      axis.text.x = element_text(angle = 45, hjust = 1),
      strip.text = element_text(face = "bold")
    )

  ggsave(
    here::here("outputs", "graphs", "22_teacher_demographics_comparison.png"),
    p2, width = 14, height = 8, dpi = 300, bg = "white"
  )
  message(">>> Saved: outputs/graphs/22_teacher_demographics_comparison.png")
} else {
  message(">>> Skipping teacher demographics comparison plot; race percentages unavailable")
}

# Plot 3: Administrator vs Teacher demographics in high suspension schools
admin_teacher_cols <- c(
  "teacher_staff_count_by_type_administrators_african_american_pct",
  "teacher_staff_count_by_type_administrators_white_pct",
  "teacher_staff_count_by_type_teachers_african_american_pct",
  "teacher_staff_count_by_type_teachers_white_pct"
)

has_admin_teacher_data <- any(admin_teacher_cols %in% names(high_suspension_teacher_summary))

if (has_admin_teacher_data) {
  admin_teacher_data <- high_suspension_teacher_summary %>%
    select(academic_year, black_prop_q_label, any_of(admin_teacher_cols)) %>%
    filter(black_prop_q_label != "Unknown") %>%
    group_by(black_prop_q_label) %>%
    summarise(
      across(any_of(admin_teacher_cols), ~ mean(.x, na.rm = TRUE)),
      .groups = "drop"
    ) %>%
    pivot_longer(
      cols = matches("_pct$"),
      names_to = "category",
      values_to = "percentage"
    ) %>%
    mutate(
      staff_type = if_else(grepl("administrators", category), "Administrators", "Teachers"),
      race = if_else(grepl("african_american", category), "African American", "White")
    )

  p3 <- admin_teacher_data %>%
    ggplot(aes(x = black_prop_q_label, y = percentage, fill = race)) +
    geom_col(position = "dodge", alpha = 0.8) +
    geom_text(
      aes(label = sprintf("%.1f%%", percentage)),
      position = position_dodge(width = 0.9),
      vjust = -0.5,
      size = 2.8
    ) +
    facet_wrap(~ staff_type, ncol = 2) +
    scale_fill_manual(
      values = c("African American" = "#377EB8", "White" = "#E41A1C"),
      name = "Race/Ethnicity"
    ) +
    scale_y_continuous(
      labels = function(x) paste0(x, "%"),
      expand = expansion(mult = c(0, 0.15))
    ) +
    labs(
      title = "Teacher vs Administrator Demographics in High Suspension Schools",
      subtitle = "Schools in top 10% of Black suspension rates within their quartile | Averaged across years",
      x = "School Black Student Proportion Quartile",
      y = "Percentage of Staff Type",
      caption = "Note: Weighted averages across high suspension schools within each quartile."
    ) +
    theme(
      legend.position = "bottom",
      plot.title = element_text(face = "bold", size = 14),
      axis.text.x = element_text(angle = 45, hjust = 1),
      strip.text = element_text(face = "bold", size = 12)
    )

  ggsave(
    here::here("outputs", "graphs", "22_admin_teacher_demographics_high_suspension.png"),
    p3, width = 12, height = 8, dpi = 300, bg = "white"
  )
  message(">>> Saved: outputs/graphs/22_admin_teacher_demographics_high_suspension.png")
} else {
  message(">>> Skipping administrator vs teacher plot; staff type breakdowns unavailable")
}

# === 7) Summary statistics display ============================================
message("\n=== KEY FINDINGS ===")
message("\n1. SUSPENSION RATES BY QUARTILE (Most recent year):")
most_recent_year <- max(quartile_year_summary$academic_year)
recent_summary <- quartile_year_summary %>%
  filter(academic_year == most_recent_year, black_prop_q_label != "Unknown") %>%
  arrange(black_prop_q)

if (has_unduplicated) {
  for (i in 1:nrow(recent_summary)) {
    message(sprintf("   %s: %.2f%% events | %.2f%% students (%.0f schools, %.0f students)",
                    recent_summary$black_prop_q_label[i],
                    recent_summary$black_suspension_rate_events[i] * 100,
                    recent_summary$black_suspension_rate_students[i] * 100,
                    recent_summary$n_schools[i],
                    recent_summary$total_black_students[i]))
  }
  message("   Note: 'events' = suspension events per student; 'students' = % students suspended (unduplicated)")
} else {
  for (i in 1:nrow(recent_summary)) {
    message(sprintf("   %s: %.2f%% (%.0f schools, %.0f students)",
                    recent_summary$black_prop_q_label[i],
                    recent_summary$black_suspension_rate_events[i] * 100,
                    recent_summary$n_schools[i],
                    recent_summary$total_black_students[i]))
  }
}

message("\n2. TEACHER DEMOGRAPHICS IN HIGH SUSPENSION SCHOOLS:")
if ("teacher_staff_count_african_american_pct" %in% names(high_suspension_teacher_summary)) {
  recent_high <- high_suspension_teacher_summary %>%
    filter(academic_year == most_recent_year, black_prop_q_label != "Unknown") %>%
    arrange(black_prop_q)

  for (i in 1:nrow(recent_high)) {
    aa_pct <- recent_high$teacher_staff_count_african_american_pct[i]
    white_pct <- recent_high$teacher_staff_count_white_pct[i]
    message(sprintf("   %s: %.1f%% African American staff, %.1f%% White staff",
                    recent_high$black_prop_q_label[i],
                    aa_pct,
                    white_pct))
  }
} else {
  message("   Teacher race demographics not available in dataset")
}

message("\n3. HIGH SUSPENSION SCHOOL COUNTS BY QUARTILE:")
high_counts <- high_suspension_teacher_summary %>%
  filter(academic_year == most_recent_year, black_prop_q_label != "Unknown") %>%
  arrange(black_prop_q)

if (has_unduplicated) {
  for (i in 1:nrow(high_counts)) {
    message(sprintf("   %s: %d schools (avg events rate: %.2f%% | avg student rate: %.2f%%)",
                    high_counts$black_prop_q_label[i],
                    high_counts$n_high_suspension_schools[i],
                    high_counts$avg_black_suspension_rate_events[i] * 100,
                    high_counts$avg_black_suspension_rate_students[i] * 100))
  }
} else {
  for (i in 1:nrow(high_counts)) {
    message(sprintf("   %s: %d schools (avg rate: %.2f%%)",
                    high_counts$black_prop_q_label[i],
                    high_counts$n_high_suspension_schools[i],
                    high_counts$avg_black_suspension_rate_events[i] * 100))
  }
}

message("\n=== ANALYSIS COMPLETE ===")
message("\nOutputs saved to:")
message("  - outputs/tables/22_black_suspension_by_quartile_year_teacher.csv")
message("  - outputs/tables/22_high_suspension_schools_teacher_demographics.csv")
message("  - outputs/tables/22_high_suspension_schools_detailed.csv")
message("  - outputs/tables/22_black_suspension_teacher_analysis.xlsx")
message("  - outputs/graphs/22_black_suspension_rates_by_quartile.png")
message("  - outputs/graphs/22_teacher_demographics_comparison.png")
message("  - outputs/graphs/22_admin_teacher_demographics_high_suspension.png")

if (has_unduplicated) {
  message("\nSUSPENSION RATE METRICS EXPLAINED:")
  message("  - Events Rate: Total suspension events ÷ enrollment (can exceed 100%)")
  message("  - Students Rate: Unduplicated students suspended ÷ enrollment (always ≤ 100%)")
  message("  Both rates are included in all output tables for transparency and comparison.")
}

message("\nIMPORTANT: These are descriptive patterns only. Many factors influence outcomes.")

invisible(TRUE)
