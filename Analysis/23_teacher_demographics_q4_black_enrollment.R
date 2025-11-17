# Analysis/23_teacher_demographics_q4_black_enrollment.R
# Analyze teacher and administrator demographics in top quartile Black enrollment schools
#
# Purpose: Understand the staff composition (teachers and administrators) in schools
#          with the highest concentration of Black students (Q4), to contextualize
#          suspension rate patterns.
#
# Input:  susp_v6_teacher_long.parquet (merged student-teacher data)
# Output: Summary tables, school annotations, and visualizations

suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(arrow)
  library(ggplot2)
  library(here)
  library(janitor)
  library(readr)
})

try(here::i_am("Analysis/23_teacher_demographics_q4_black_enrollment.R"), silent = TRUE)

source(here("R", "utils_keys_filters.R"))
source(here("R", "00_paths.R"))

message("=== 23: Teacher Demographics in Q4 Black Enrollment Schools ===")

# Ensure output subdirectories exist
dir.create(dp_out, recursive = TRUE, showWarnings = FALSE)
tables_dir <- file.path(dp_out, "tables")
graphs_dir <- file.path(dp_out, "graphs")
dir.create(tables_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(graphs_dir, recursive = TRUE, showWarnings = FALSE)

# Read merged student-teacher data (teacher features aggregated at the school level)
TEACHER_DATA_PATH <- here("data-stage", "susp_v6_teacher_features.parquet")
FEATURES_PATH <- here("data-stage", "susp_v6_features.parquet")

if (!file.exists(TEACHER_DATA_PATH)) {
  stop("Missing merged teacher-student data: ", TEACHER_DATA_PATH,
       "\nRun Analysis/18_merge_teacher_student.R first.")
}
if (!file.exists(FEATURES_PATH)) {
  stop("Missing v6 features data: ", FEATURES_PATH,
       "\nRun run_pipeline.R first.")
}

message(">>> Loading merged student-teacher data...")
df <- read_parquet(TEACHER_DATA_PATH) %>%
  clean_names() %>%
  build_keys() %>%  # Ensure cds_school exists for joining
  mutate(
    # Standardize suspension rate column name used downstream
    suspension_rate = suspension_rate_percent_total
  )

message(">>> Loading school features (for is_traditional flag and aggregates)...")
features <- read_parquet(FEATURES_PATH) %>%
  clean_names() %>%
  build_keys() %>%  # Creates cds_school from county/district/school codes
  select(cds_school, academic_year, is_traditional, black_share, white_share, hispanic_share)

# Join school-level features from features file
# Note: susp_v6_long.parquet has race-specific rows, so we join school-level aggregates
# (is_traditional, black_share, etc.) from the features file
df <- df %>%
  left_join(
    features,
    by = c("cds_school", "academic_year")
  )

message(">>> Total rows: ", nrow(df))
message(">>> Unique schools: ", n_distinct(df$cds_school))
message(">>> Academic years: ", paste(sort(unique(df$academic_year)), collapse = ", "))
message(">>> is_traditional coverage: ", sum(!is.na(df$is_traditional)), " of ", nrow(df), " rows")

# Filter to traditional schools only (exclude alternative schools)
# Filter to top quartile Black enrollment (Q4)
# Keep only one row per school-year (aggregate across race groups for school-level summary)
message(">>> Filtering to traditional schools, Q4 Black enrollment...")

school_summary <- df %>%
  filter(
    is_traditional == TRUE,  # Traditional schools only (remove NA check since we now have the data)
    !is.na(black_prop_q),  # Must have Black proportion quartile
    black_prop_q == 4  # Top quartile only
  ) %>%
  # Get one row per school-year for school-level summaries
  distinct(academic_year, cds_school, .keep_all = TRUE) %>%
  # Keep only relevant columns
  select(
    academic_year, cds_school, county_name, district_name, school_name,
    school_level, locale_simple,
    cumulative_enrollment, black_prop_q, black_share,
    total_suspensions, suspension_rate,
    # Teacher totals
    starts_with("teacher_staff_count_total"),
    # Teacher race breakdowns
    starts_with("teacher_staff_count_african_american"),
    starts_with("teacher_staff_count_white"),
    starts_with("teacher_staff_count_hispanic_or_latino"),
    starts_with("teacher_staff_count_asian"),
    # Teacher shares
    starts_with("teacher_staff_count_african_american_share"),
    starts_with("teacher_staff_count_white_share"),
    starts_with("teacher_staff_count_hispanic_or_latino_share"),
    starts_with("teacher_staff_count_asian_share"),
    # Staff type totals (teachers, administrators)
    teacher_staff_count_total_by_type_teachers,
    teacher_staff_count_total_by_type_administrators,
    # Race breakdowns by staff type
    starts_with("teacher_staff_count_by_type_teachers_"),
    starts_with("teacher_staff_count_by_type_administrators_"),
    # Gender breakdowns
    starts_with("teacher_staff_count_by_gender_")
  )

message(">>> Filtered to ", nrow(school_summary), " school-years in Q4 Black enrollment")
message(">>> Unique schools: ", n_distinct(school_summary$cds_school))

# Check teacher data coverage
teacher_coverage <- school_summary %>%
  summarise(
    schools_with_teacher_data = sum(!is.na(teacher_staff_count_total), na.rm = TRUE),
    pct_coverage = mean(!is.na(teacher_staff_count_total), na.rm = TRUE) * 100
  )

message(">>> Teacher data coverage: ", teacher_coverage$schools_with_teacher_data,
        " of ", nrow(school_summary), " school-years (",
        round(teacher_coverage$pct_coverage, 1), "%)")

# ============================================================================
# PART 1: School-Level Annotations
# ============================================================================
message("\n>>> Generating school-level annotations...")

# Get most recent year for each school
school_annotations <- school_summary %>%
  arrange(cds_school, desc(academic_year)) %>%
  group_by(cds_school) %>%
  slice(1) %>%
  ungroup() %>%
  select(
    academic_year, cds_school, county_name, district_name, school_name,
    school_level, locale_simple,
    cumulative_enrollment, black_share, black_prop_q,
    total_suspensions, suspension_rate,
    teacher_staff_count_total,
    teacher_staff_count_total_by_type_teachers,
    teacher_staff_count_total_by_type_administrators
  ) %>%
  arrange(desc(cumulative_enrollment))

# Write school annotations
annotations_path <- file.path(dp_out, "tables", "q4_black_enrollment_schools_annotations.csv")
write_csv(school_annotations, annotations_path)
message(">>> Wrote school annotations: ", annotations_path)
message("    Total schools: ", nrow(school_annotations))

# ============================================================================
# PART 2: Aggregate Statistics - Overall Staff Demographics
# ============================================================================
message("\n>>> Calculating aggregate staff demographics...")

# Calculate statewide aggregates across all Q4 schools
overall_stats <- school_summary %>%
  filter(!is.na(teacher_staff_count_total)) %>%
  summarise(
    n_schools = n(),
    n_unique_schools = n_distinct(cds_school),

    # Student demographics
    total_students = sum(cumulative_enrollment, na.rm = TRUE),
    avg_black_share = mean(black_share, na.rm = TRUE),
    median_black_share = median(black_share, na.rm = TRUE),

    # Overall staff counts
    total_staff = sum(teacher_staff_count_total, na.rm = TRUE),
    total_teachers = sum(teacher_staff_count_total_by_type_teachers, na.rm = TRUE),
    total_administrators = sum(teacher_staff_count_total_by_type_administrators, na.rm = TRUE),

    # Staff racial composition (totals)
    total_staff_african_american = sum(teacher_staff_count_african_american, na.rm = TRUE),
    total_staff_white = sum(teacher_staff_count_white, na.rm = TRUE),
    total_staff_hispanic = sum(teacher_staff_count_hispanic_or_latino, na.rm = TRUE),
    total_staff_asian = sum(teacher_staff_count_asian, na.rm = TRUE),

    # Teacher racial composition
    teachers_african_american = sum(teacher_staff_count_by_type_teachers_african_american, na.rm = TRUE),
    teachers_white = sum(teacher_staff_count_by_type_teachers_white, na.rm = TRUE),
    teachers_hispanic = sum(teacher_staff_count_by_type_teachers_hispanic_or_latino, na.rm = TRUE),
    teachers_asian = sum(teacher_staff_count_by_type_teachers_asian, na.rm = TRUE),

    # Administrator racial composition
    admins_african_american = sum(teacher_staff_count_by_type_administrators_african_american, na.rm = TRUE),
    admins_white = sum(teacher_staff_count_by_type_administrators_white, na.rm = TRUE),
    admins_hispanic = sum(teacher_staff_count_by_type_administrators_hispanic_or_latino, na.rm = TRUE),
    admins_asian = sum(teacher_staff_count_by_type_administrators_asian, na.rm = TRUE)
  ) %>%
  mutate(
    # Calculate shares
    staff_african_american_share = total_staff_african_american / total_staff,
    staff_white_share = total_staff_white / total_staff,
    staff_hispanic_share = total_staff_hispanic / total_staff,
    staff_asian_share = total_staff_asian / total_staff,

    teachers_african_american_share = teachers_african_american / total_teachers,
    teachers_white_share = teachers_white / total_teachers,
    teachers_hispanic_share = teachers_hispanic / total_teachers,
    teachers_asian_share = teachers_asian / total_teachers,

    admins_african_american_share = admins_african_american / total_administrators,
    admins_white_share = admins_white / total_administrators,
    admins_hispanic_share = admins_hispanic / total_administrators,
    admins_asian_share = admins_asian / total_administrators
  )

# Transpose for readability
overall_stats_long <- overall_stats %>%
  pivot_longer(everything(), names_to = "metric", values_to = "value")

overall_stats_path <- file.path(dp_out, "tables", "q4_black_enrollment_overall_staff_stats.csv")
write_csv(overall_stats_long, overall_stats_path)
message(">>> Wrote overall staff statistics: ", overall_stats_path)

# Print key findings
message("\n=== KEY FINDINGS: Q4 Black Enrollment Schools (Traditional Only) ===")
message(sprintf("Schools analyzed: %d unique schools across %d school-years",
                overall_stats$n_unique_schools, overall_stats$n_schools))
message(sprintf("Total students: %s", format(overall_stats$total_students, big.mark = ",")))
message(sprintf("Average Black student share: %.1f%%", overall_stats$avg_black_share * 100))
message(sprintf("\nTotal staff: %s", format(overall_stats$total_staff, big.mark = ",")))
message(sprintf("  - Teachers: %s", format(overall_stats$total_teachers, big.mark = ",")))
message(sprintf("  - Administrators: %s", format(overall_stats$total_administrators, big.mark = ",")))
message("\nStaff Racial Composition:")
message(sprintf("  - African American: %.1f%%", overall_stats$staff_african_american_share * 100))
message(sprintf("  - White: %.1f%%", overall_stats$staff_white_share * 100))
message(sprintf("  - Hispanic/Latino: %.1f%%", overall_stats$staff_hispanic_share * 100))
message(sprintf("  - Asian: %.1f%%", overall_stats$staff_asian_share * 100))
message("\nTeacher Racial Composition:")
message(sprintf("  - African American: %.1f%%", overall_stats$teachers_african_american_share * 100))
message(sprintf("  - White: %.1f%%", overall_stats$teachers_white_share * 100))
message(sprintf("  - Hispanic/Latino: %.1f%%", overall_stats$teachers_hispanic_share * 100))
message(sprintf("  - Asian: %.1f%%", overall_stats$teachers_asian_share * 100))
message("\nAdministrator Racial Composition:")
message(sprintf("  - African American: %.1f%%", overall_stats$admins_african_american_share * 100))
message(sprintf("  - White: %.1f%%", overall_stats$admins_white_share * 100))
message(sprintf("  - Hispanic/Latino: %.1f%%", overall_stats$admins_hispanic_share * 100))
message(sprintf("  - Asian: %.1f%%", overall_stats$admins_asian_share * 100))

# ============================================================================
# PART 3: Year-over-Year Trends
# ============================================================================
message("\n>>> Calculating year-over-year trends...")

yearly_stats <- school_summary %>%
  filter(!is.na(teacher_staff_count_total)) %>%
  group_by(academic_year) %>%
  summarise(
    n_schools = n(),

    # Staff counts
    total_staff = sum(teacher_staff_count_total, na.rm = TRUE),
    total_teachers = sum(teacher_staff_count_total_by_type_teachers, na.rm = TRUE),
    total_administrators = sum(teacher_staff_count_total_by_type_administrators, na.rm = TRUE),

    # Teacher racial composition
    teachers_african_american = sum(teacher_staff_count_by_type_teachers_african_american, na.rm = TRUE),
    teachers_white = sum(teacher_staff_count_by_type_teachers_white, na.rm = TRUE),
    teachers_hispanic = sum(teacher_staff_count_by_type_teachers_hispanic_or_latino, na.rm = TRUE),
    teachers_asian = sum(teacher_staff_count_by_type_teachers_asian, na.rm = TRUE),

    # Administrator racial composition
    admins_african_american = sum(teacher_staff_count_by_type_administrators_african_american, na.rm = TRUE),
    admins_white = sum(teacher_staff_count_by_type_administrators_white, na.rm = TRUE),
    admins_hispanic = sum(teacher_staff_count_by_type_administrators_hispanic_or_latino, na.rm = TRUE),
    admins_asian = sum(teacher_staff_count_by_type_administrators_asian, na.rm = TRUE),

    .groups = "drop"
  ) %>%
  mutate(
    # Calculate shares
    teachers_african_american_share = teachers_african_american / total_teachers,
    teachers_white_share = teachers_white / total_teachers,
    teachers_hispanic_share = teachers_hispanic / total_teachers,
    teachers_asian_share = teachers_asian / total_teachers,

    admins_african_american_share = admins_african_american / total_administrators,
    admins_white_share = admins_white / total_administrators,
    admins_hispanic_share = admins_hispanic / total_administrators,
    admins_asian_share = admins_asian / total_administrators
  )

yearly_path <- file.path(dp_out, "tables", "q4_black_enrollment_yearly_staff_stats.csv")
write_csv(yearly_stats, yearly_path)
message(">>> Wrote yearly trends: ", yearly_path)

# ============================================================================
# PART 4: By School Level
# ============================================================================
message("\n>>> Calculating statistics by school level...")

by_level_stats <- school_summary %>%
  filter(!is.na(teacher_staff_count_total), !is.na(school_level)) %>%
  group_by(school_level) %>%
  summarise(
    n_schools = n(),
    n_unique_schools = n_distinct(cds_school),

    # Staff counts
    total_staff = sum(teacher_staff_count_total, na.rm = TRUE),
    total_teachers = sum(teacher_staff_count_total_by_type_teachers, na.rm = TRUE),
    total_administrators = sum(teacher_staff_count_total_by_type_administrators, na.rm = TRUE),

    # Teacher racial composition
    teachers_african_american = sum(teacher_staff_count_by_type_teachers_african_american, na.rm = TRUE),
    teachers_white = sum(teacher_staff_count_by_type_teachers_white, na.rm = TRUE),
    teachers_hispanic = sum(teacher_staff_count_by_type_teachers_hispanic_or_latino, na.rm = TRUE),
    teachers_asian = sum(teacher_staff_count_by_type_teachers_asian, na.rm = TRUE),

    # Administrator racial composition
    admins_african_american = sum(teacher_staff_count_by_type_administrators_african_american, na.rm = TRUE),
    admins_white = sum(teacher_staff_count_by_type_administrators_white, na.rm = TRUE),
    admins_hispanic = sum(teacher_staff_count_by_type_administrators_hispanic_or_latino, na.rm = TRUE),
    admins_asian = sum(teacher_staff_count_by_type_administrators_asian, na.rm = TRUE),

    .groups = "drop"
  ) %>%
  mutate(
    # Calculate shares
    teachers_african_american_share = teachers_african_american / total_teachers,
    teachers_white_share = teachers_white / total_teachers,
    teachers_hispanic_share = teachers_hispanic / total_teachers,
    teachers_asian_share = teachers_asian / total_teachers,

    admins_african_american_share = admins_african_american / total_administrators,
    admins_white_share = admins_white / total_administrators,
    admins_hispanic_share = admins_hispanic / total_administrators,
    admins_asian_share = admins_asian / total_administrators
  )

by_level_path <- file.path(dp_out, "tables", "q4_black_enrollment_by_level_staff_stats.csv")
write_csv(by_level_stats, by_level_path)
message(">>> Wrote by-level statistics: ", by_level_path)

# ============================================================================
# PART 5: Visualizations
# ============================================================================
message("\n>>> Creating visualizations...")

# Prepare data for visualization
teacher_race_data <- yearly_stats %>%
  select(academic_year,
         `African American` = teachers_african_american_share,
         `White` = teachers_white_share,
         `Hispanic/Latino` = teachers_hispanic_share,
         `Asian` = teachers_asian_share) %>%
  pivot_longer(-academic_year, names_to = "race", values_to = "share")

admin_race_data <- yearly_stats %>%
  select(academic_year,
         `African American` = admins_african_american_share,
         `White` = admins_white_share,
         `Hispanic/Latino` = admins_hispanic_share,
         `Asian` = admins_asian_share) %>%
  pivot_longer(-academic_year, names_to = "race", values_to = "share")

# Define colors (using canonical approach)
race_colors <- c(
  "African American" = "#D55E00",
  "White" = "#0072B2",
  "Hispanic/Latino" = "#009E73",
  "Asian" = "#CC79A7"
)

# Plot 1: Teacher racial composition over time
p1 <- ggplot(teacher_race_data, aes(x = academic_year, y = share, color = race, group = race)) +
  geom_line(linewidth = 1.2) +
  geom_point(size = 3) +
  scale_color_manual(values = race_colors) +
  scale_y_continuous(labels = scales::percent_format(), limits = c(0, NA)) +
  labs(
    title = "Teacher Racial Composition in Q4 Black Enrollment Schools",
    subtitle = "Traditional schools only, top quartile of Black student enrollment",
    x = "Academic Year",
    y = "Share of Teachers",
    color = "Race/Ethnicity"
  ) +
  theme_minimal(base_size = 12) +
  theme(
    axis.text.x = element_text(angle = 45, hjust = 1),
    legend.position = "bottom",
    plot.title = element_text(face = "bold"),
    panel.grid.minor = element_blank()
  )

p1_path <- file.path(dp_out, "graphs", "q4_black_enrollment_teacher_race_trends.png")
ggsave(p1_path, p1, width = 10, height = 6, dpi = 300)
message(">>> Saved: ", p1_path)

# Plot 2: Administrator racial composition over time
p2 <- ggplot(admin_race_data, aes(x = academic_year, y = share, color = race, group = race)) +
  geom_line(linewidth = 1.2) +
  geom_point(size = 3) +
  scale_color_manual(values = race_colors) +
  scale_y_continuous(labels = scales::percent_format(), limits = c(0, NA)) +
  labs(
    title = "Administrator Racial Composition in Q4 Black Enrollment Schools",
    subtitle = "Traditional schools only, top quartile of Black student enrollment",
    x = "Academic Year",
    y = "Share of Administrators",
    color = "Race/Ethnicity"
  ) +
  theme_minimal(base_size = 12) +
  theme(
    axis.text.x = element_text(angle = 45, hjust = 1),
    legend.position = "bottom",
    plot.title = element_text(face = "bold"),
    panel.grid.minor = element_blank()
  )

p2_path <- file.path(dp_out, "graphs", "q4_black_enrollment_admin_race_trends.png")
ggsave(p2_path, p2, width = 10, height = 6, dpi = 300)
message(">>> Saved: ", p2_path)

# Plot 3: Comparison bar chart (most recent year)
latest_year <- max(yearly_stats$academic_year)
comparison_data <- bind_rows(
  yearly_stats %>%
    filter(academic_year == latest_year) %>%
    select(
      `African American` = teachers_african_american_share,
      `White` = teachers_white_share,
      `Hispanic/Latino` = teachers_hispanic_share,
      `Asian` = teachers_asian_share
    ) %>%
    pivot_longer(everything(), names_to = "race", values_to = "share") %>%
    mutate(staff_type = "Teachers"),

  yearly_stats %>%
    filter(academic_year == latest_year) %>%
    select(
      `African American` = admins_african_american_share,
      `White` = admins_white_share,
      `Hispanic/Latino` = admins_hispanic_share,
      `Asian` = admins_asian_share
    ) %>%
    pivot_longer(everything(), names_to = "race", values_to = "share") %>%
    mutate(staff_type = "Administrators")
)

p3 <- ggplot(comparison_data, aes(x = race, y = share, fill = staff_type)) +
  geom_col(position = "dodge", width = 0.7) +
  scale_fill_manual(values = c("Teachers" = "#56B4E9", "Administrators" = "#E69F00")) +
  scale_y_continuous(labels = scales::percent_format()) +
  labs(
    title = sprintf("Staff Racial Composition in Q4 Black Enrollment Schools (%s)", latest_year),
    subtitle = "Traditional schools only, top quartile of Black student enrollment",
    x = "Race/Ethnicity",
    y = "Share of Staff",
    fill = "Staff Type"
  ) +
  theme_minimal(base_size = 12) +
  theme(
    axis.text.x = element_text(angle = 45, hjust = 1),
    legend.position = "bottom",
    plot.title = element_text(face = "bold"),
    panel.grid.minor = element_blank()
  )

p3_path <- file.path(dp_out, "graphs", "q4_black_enrollment_staff_comparison.png")
ggsave(p3_path, p3, width = 10, height = 6, dpi = 300)
message(">>> Saved: ", p3_path)

# Plot 4: By school level comparison
level_comparison_data <- by_level_stats %>%
  select(
    school_level,
    `Teachers\n(African American)` = teachers_african_american_share,
    `Teachers\n(White)` = teachers_white_share,
    `Administrators\n(African American)` = admins_african_american_share,
    `Administrators\n(White)` = admins_white_share
  ) %>%
  pivot_longer(-school_level, names_to = "group", values_to = "share") %>%
  filter(!is.na(share), !is.nan(share))

p4 <- ggplot(level_comparison_data, aes(x = school_level, y = share, fill = group)) +
  geom_col(position = "dodge", width = 0.7) +
  scale_fill_manual(values = c(
    "Teachers\n(African American)" = "#D55E00",
    "Teachers\n(White)" = "#0072B2",
    "Administrators\n(African American)" = "#E69F00",
    "Administrators\n(White)" = "#56B4E9"
  )) +
  scale_y_continuous(labels = scales::percent_format()) +
  labs(
    title = "Staff Racial Composition by School Level in Q4 Black Enrollment Schools",
    subtitle = "Traditional schools only, aggregated across all years",
    x = "School Level",
    y = "Share of Staff",
    fill = "Staff Group"
  ) +
  theme_minimal(base_size = 12) +
  theme(
    axis.text.x = element_text(angle = 45, hjust = 1),
    legend.position = "bottom",
    plot.title = element_text(face = "bold"),
    panel.grid.minor = element_blank()
  )

p4_path <- file.path(dp_out, "graphs", "q4_black_enrollment_staff_by_level.png")
ggsave(p4_path, p4, width = 12, height = 6, dpi = 300)
message(">>> Saved: ", p4_path)

message("\n=== Analysis complete! ===")
message("Outputs saved to:")
message("  - ", annotations_path)
message("  - ", overall_stats_path)
message("  - ", yearly_path)
message("  - ", by_level_path)
message("  - ", p1_path)
message("  - ", p2_path)
message("  - ", p3_path)
message("  - ", p4_path)
