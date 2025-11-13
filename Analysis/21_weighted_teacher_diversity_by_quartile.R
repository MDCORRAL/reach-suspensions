# Analysis/21_weighted_teacher_diversity_by_quartile.R
# Weighted analysis of teacher diversity by Black enrollment quartile
#
# Purpose: Examine the relationship between school racial composition and teacher
#          diversity using WEIGHTED aggregations (not simple averages). Larger
#          schools appropriately influence quartile estimates.
#
# Input: susp_v6_teacher_features.parquet (merged student + teacher data)
# Output:
#   - tables/21_teacher_diversity_by_quartile_*.csv (summary statistics)
#   - graphs/21_teacher_diversity_by_quartile_*.png (visualizations)
#
# Key methodological notes:
# 1. Uses weighted averages: schools are weighted by staff count
# 2. Aggregates counts first, then calculates proportions
# 3. Includes diagnostic checks for small-n schools
# 4. Non-causal interpretation: correlational patterns only

# === 1) Setup =================================================================
suppressPackageStartupMessages({
  library(here); library(arrow); library(dplyr); library(tidyr)
  library(ggplot2); library(scales); library(patchwork)
})

try(here::i_am("Analysis/21_weighted_teacher_diversity_by_quartile.R"), silent = TRUE)

# Load canonical definitions
source(here::here("R", "utils_keys_filters.R"))

theme_set(theme_minimal(base_size = 12))

# Color palette for quartiles (use canonical Black quartile colors)
black_quartile_colors <- setNames(
  c("#FEE5D9", "#FCAE91", "#FB6A4A", "#CB181D"),
  get_quartile_label(1:4, "Black")
)

# === 2) Load and validate data ================================================
message("=== 21: Weighted Teacher Diversity by Black Enrollment Quartile ===")

# Load student data (wide format with one row per school-year)
V6_FEATURES_PATH <- here::here("data-stage", "susp_v6_features.parquet")
TEACHER_PATH <- here::here("data-stage", "teacher_staff_long.parquet")

if (!file.exists(V6_FEATURES_PATH)) {
  stop("Missing susp_v6_features.parquet. Run run_pipeline.R first.")
}
if (!file.exists(TEACHER_PATH)) {
  stop("Missing teacher_staff_long.parquet. Run R/01c_ingest_teacher_demographics.R first.")
}

message(">>> Loading student suspension data (wide format)...")
df_students <- arrow::read_parquet(V6_FEATURES_PATH) %>%
  janitor::clean_names() %>%
  build_keys() %>%
  filter_campus_only()

# Verify uniqueness (should be one row per school-year)
df_students <- assert_unique_campus(df_students, campus_col = "cds_school", year_col = "academic_year")

message(">>> Loading and summarizing teacher data...")
source(here::here("R", "teacher_processing.R"))

teacher_long <- arrow::read_parquet(TEACHER_PATH) %>%
  janitor::clean_names() %>%
  build_keys()

teacher_summary <- teacher_summarise_long(teacher_long)

# Sanitize NaN/Inf in teacher data
teacher_summary <- teacher_summary %>%
  mutate(across(where(is.numeric), ~ {
    out <- .x
    out[is.nan(out)] <- NA_real_
    dplyr::na_if(out, Inf)
  }))

message(">>> Joining teacher and student data...")
df <- df_students %>%
  left_join(
    teacher_summary,
    by = c("academic_year", "cds_school"),
    relationship = "one-to-one"
  )

# Check required columns
required_cols <- c(
  "academic_year", "cds_school", "black_prop_q",
  "cumulative_enrollment", "total_suspensions"
)
missing <- setdiff(required_cols, names(df))
if (length(missing)) {
  stop("Missing required columns: ", paste(missing, collapse = ", "))
}

# Identify teacher columns
teacher_cols <- grep("^teacher_", names(df), value = TRUE)
if (!length(teacher_cols)) {
  stop("No teacher_* columns found. Check merge in Analysis/18_merge_teacher_student.R")
}

message(">>> Found ", length(teacher_cols), " teacher demographic columns")

# Add readable quartile labels
if (!"black_prop_q_label" %in% names(df)) {
  df <- df %>%
    mutate(black_prop_q_label = get_quartile_label(black_prop_q, "Black"))
}

# Calculate suspension rate at school level
df <- df %>%
  mutate(
    suspension_rate = if_else(
      cumulative_enrollment > 0,
      total_suspensions / cumulative_enrollment,
      NA_real_
    )
  )

# === 3) Diagnostic checks =====================================================
message("\n>>> Running diagnostic checks...")

# Check 1: Data coverage
coverage <- df %>%
  summarise(
    total_schools = n_distinct(cds_school),
    total_school_years = n(),
    schools_with_teacher_data = sum(!is.na(teacher_staff_count_total), na.rm = TRUE),
    coverage_pct = mean(!is.na(teacher_staff_count_total), na.rm = TRUE) * 100
  )

message(">>> Total school-year observations: ", coverage$total_school_years)
message(">>> Schools with teacher data: ", coverage$schools_with_teacher_data,
        " (", round(coverage$coverage_pct, 1), "%)")

# Check 2: Missing value patterns
missing_summary <- df %>%
  summarise(
    missing_black_quartile = sum(is.na(black_prop_q)),
    missing_enrollment = sum(is.na(cumulative_enrollment) | cumulative_enrollment == 0),
    missing_suspension_rate = sum(is.na(suspension_rate)),
    missing_teacher_count = sum(is.na(teacher_staff_count_total))
  )

message(">>> Missing data summary:")
print(missing_summary)

# Check 3: Quartile distribution
quartile_dist <- df %>%
  filter(!is.na(black_prop_q)) %>%
  count(black_prop_q, black_prop_q_label) %>%
  arrange(black_prop_q)

message("\n>>> Black enrollment quartile distribution:")
print(quartile_dist)

# Check 4: Small-n schools
small_n_summary <- df %>%
  filter(!is.na(teacher_staff_count_total)) %>%
  summarise(
    schools_under_10_staff = sum(teacher_staff_count_total < 10, na.rm = TRUE),
    schools_under_5_staff = sum(teacher_staff_count_total < 5, na.rm = TRUE),
    median_staff_count = median(teacher_staff_count_total, na.rm = TRUE),
    mean_staff_count = mean(teacher_staff_count_total, na.rm = TRUE)
  )

message("\n>>> Small-n school summary:")
print(small_n_summary)

# === 4) Filter to analysis sample ============================================
message("\n>>> Creating analysis sample...")

# Filter criteria:
# 1. Has Black enrollment quartile
# 2. Has teacher data
# 3. Has enrollment data
# 4. Exclude "Unknown" quartile
# 5. Focus on recent years (2018-19 onwards) for better teacher coverage

analysis_df <- df %>%
  filter(
    !is.na(black_prop_q),
    black_prop_q_label != "Unknown",
    !is.na(teacher_staff_count_total),
    teacher_staff_count_total > 0,
    !is.na(cumulative_enrollment),
    cumulative_enrollment > 0,
    academic_year >= "2018-19"  # Better teacher data coverage
  )

message(">>> Analysis sample: ", nrow(analysis_df), " school-year observations")
message(">>> Unique schools: ", n_distinct(analysis_df$cds_school))
message(">>> Academic years: ", paste(sort(unique(analysis_df$academic_year)), collapse = ", "))

# === 5) Weighted aggregation by quartile =====================================
message("\n>>> Computing weighted aggregations by quartile...")

# Method 1: Aggregate counts, then calculate proportions
# This is the CORRECT way to weight by school size

# First, identify key teacher race columns
teacher_race_cols <- c(
  "teacher_staff_count_african_american",
  "teacher_staff_count_american_indian_or_alaska_native",
  "teacher_staff_count_asian",
  "teacher_staff_count_filipino",
  "teacher_staff_count_hispanic_or_latino",
  "teacher_staff_count_native_hawaiian_pacific_islander",
  "teacher_staff_count_white",
  "teacher_staff_count_two_or_more_races",
  "teacher_staff_count_not_reported"
)

# Check which columns exist
teacher_race_cols <- intersect(teacher_race_cols, names(analysis_df))
message(">>> Using ", length(teacher_race_cols), " teacher race columns")

# Aggregate by quartile and year
weighted_summary <- analysis_df %>%
  group_by(academic_year, black_prop_q, black_prop_q_label) %>%
  summarise(
    # School counts
    n_schools = n(),

    # Student metrics (aggregated)
    total_students = sum(cumulative_enrollment, na.rm = TRUE),
    total_suspensions = sum(total_suspensions, na.rm = TRUE),

    # Teacher metrics (aggregated counts)
    total_teachers = sum(teacher_staff_count_total, na.rm = TRUE),
    across(
      all_of(teacher_race_cols),
      ~ sum(.x, na.rm = TRUE),
      .names = "{.col}_sum"
    ),

    # Distribution of school sizes
    median_school_size = median(cumulative_enrollment, na.rm = TRUE),
    median_teacher_count = median(teacher_staff_count_total, na.rm = TRUE),

    .groups = "drop"
  ) %>%
  mutate(
    # Calculate weighted suspension rate
    suspension_rate = if_else(
      total_students > 0,
      total_suspensions / total_students,
      NA_real_
    ),

    # Calculate teacher race proportions from aggregated counts
    pct_teachers_african_american = if_else(
      "teacher_staff_count_african_american_sum" %in% names(.) & total_teachers > 0,
      teacher_staff_count_african_american_sum / total_teachers * 100,
      NA_real_
    ),
    pct_teachers_white = if_else(
      "teacher_staff_count_white_sum" %in% names(.) & total_teachers > 0,
      teacher_staff_count_white_sum / total_teachers * 100,
      NA_real_
    ),
    pct_teachers_hispanic = if_else(
      "teacher_staff_count_hispanic_or_latino_sum" %in% names(.) & total_teachers > 0,
      teacher_staff_count_hispanic_or_latino_sum / total_teachers * 100,
      NA_real_
    ),
    pct_teachers_asian = if_else(
      "teacher_staff_count_asian_sum" %in% names(.) & total_teachers > 0,
      teacher_staff_count_asian_sum / total_teachers * 100,
      NA_real_
    ),

    # Calculate non-White teacher percentage
    # Non-White = 100% - White%
    pct_teachers_non_white = if_else(
      !is.na(pct_teachers_white),
      100 - pct_teachers_white,
      NA_real_
    )
  )

message(">>> Weighted summary computed for ", nrow(weighted_summary), " quartile-year combinations")

# === 6) Overall quartile summary (across all years) ==========================
message("\n>>> Computing overall quartile summary...")

overall_summary <- analysis_df %>%
  group_by(black_prop_q, black_prop_q_label) %>%
  summarise(
    # School counts
    n_schools = n(),
    n_unique_schools = n_distinct(cds_school),

    # Student metrics (aggregated)
    total_students = sum(cumulative_enrollment, na.rm = TRUE),
    total_suspensions = sum(total_suspensions, na.rm = TRUE),

    # Teacher metrics (aggregated counts)
    total_teachers = sum(teacher_staff_count_total, na.rm = TRUE),
    across(
      all_of(teacher_race_cols),
      ~ sum(.x, na.rm = TRUE),
      .names = "{.col}_sum"
    ),

    # Distribution metrics
    median_school_enrollment = median(cumulative_enrollment, na.rm = TRUE),
    mean_school_enrollment = mean(cumulative_enrollment, na.rm = TRUE),
    median_teacher_count = median(teacher_staff_count_total, na.rm = TRUE),
    mean_teacher_count = mean(teacher_staff_count_total, na.rm = TRUE),

    .groups = "drop"
  ) %>%
  mutate(
    # Calculate weighted rates
    suspension_rate = if_else(
      total_students > 0,
      total_suspensions / total_students * 100,  # As percentage
      NA_real_
    ),

    # Calculate teacher race proportions
    pct_teachers_african_american = if_else(
      "teacher_staff_count_african_american_sum" %in% names(.) & total_teachers > 0,
      teacher_staff_count_african_american_sum / total_teachers * 100,
      NA_real_
    ),
    pct_teachers_white = if_else(
      "teacher_staff_count_white_sum" %in% names(.) & total_teachers > 0,
      teacher_staff_count_white_sum / total_teachers * 100,
      NA_real_
    ),
    pct_teachers_hispanic = if_else(
      "teacher_staff_count_hispanic_or_latino_sum" %in% names(.) & total_teachers > 0,
      teacher_staff_count_hispanic_or_latino_sum / total_teachers * 100,
      NA_real_
    ),
    pct_teachers_asian = if_else(
      "teacher_staff_count_asian_sum" %in% names(.) & total_teachers > 0,
      teacher_staff_count_asian_sum / total_teachers * 100,
      NA_real_
    ),
    pct_teachers_non_white = if_else(
      !is.na(pct_teachers_white),
      100 - pct_teachers_white,
      NA_real_
    )
  ) %>%
  arrange(black_prop_q)

message("\n>>> Overall summary by quartile:")
print(overall_summary %>%
        select(black_prop_q_label, n_unique_schools, total_teachers,
               pct_teachers_white, pct_teachers_non_white,
               pct_teachers_african_american, suspension_rate))

# === 7) Distribution analysis within quartiles ===============================
message("\n>>> Analyzing distributions within quartiles...")

# Calculate school-level teacher diversity, then examine distribution by quartile
school_level_diversity <- analysis_df %>%
  filter(
    !is.na(teacher_staff_count_total),
    teacher_staff_count_total >= 5  # Minimum threshold for stable proportions
  ) %>%
  mutate(
    # Calculate school-level percentages
    school_pct_white = if_else(
      teacher_staff_count_total > 0 & !is.na(teacher_staff_count_white),
      teacher_staff_count_white / teacher_staff_count_total * 100,
      NA_real_
    ),
    school_pct_non_white = if_else(
      !is.na(school_pct_white),
      100 - school_pct_white,
      NA_real_
    ),
    school_pct_african_american = if_else(
      teacher_staff_count_total > 0 & !is.na(teacher_staff_count_african_american),
      teacher_staff_count_african_american / teacher_staff_count_total * 100,
      NA_real_
    )
  )

# Distribution statistics by quartile
distribution_summary <- school_level_diversity %>%
  group_by(black_prop_q, black_prop_q_label) %>%
  summarise(
    n_schools = n(),

    # White teacher distribution
    mean_pct_white = mean(school_pct_white, na.rm = TRUE),
    median_pct_white = median(school_pct_white, na.rm = TRUE),
    sd_pct_white = sd(school_pct_white, na.rm = TRUE),
    q25_pct_white = quantile(school_pct_white, 0.25, na.rm = TRUE),
    q75_pct_white = quantile(school_pct_white, 0.75, na.rm = TRUE),

    # Non-White teacher distribution
    mean_pct_non_white = mean(school_pct_non_white, na.rm = TRUE),
    median_pct_non_white = median(school_pct_non_white, na.rm = TRUE),
    sd_pct_non_white = sd(school_pct_non_white, na.rm = TRUE),

    # African American teacher distribution
    mean_pct_african_american = mean(school_pct_african_american, na.rm = TRUE),
    median_pct_african_american = median(school_pct_african_american, na.rm = TRUE),
    sd_pct_african_american = sd(school_pct_african_american, na.rm = TRUE),

    .groups = "drop"
  ) %>%
  arrange(black_prop_q)

message("\n>>> Distribution summary:")
print(distribution_summary)

# === 8) Save summary tables ===================================================
message("\n>>> Saving summary tables...")

dir.create(here::here("outputs", "tables"), showWarnings = FALSE, recursive = TRUE)

# Table 1: Weighted summary by quartile and year
write.csv(
  weighted_summary,
  here::here("outputs", "tables", "21_teacher_diversity_by_quartile_year.csv"),
  row.names = FALSE
)
message(">>> Saved: outputs/tables/21_teacher_diversity_by_quartile_year.csv")

# Table 2: Overall quartile summary
write.csv(
  overall_summary,
  here::here("outputs", "tables", "21_teacher_diversity_by_quartile_overall.csv"),
  row.names = FALSE
)
message(">>> Saved: outputs/tables/21_teacher_diversity_by_quartile_overall.csv")

# Table 3: Distribution summary
write.csv(
  distribution_summary,
  here::here("outputs", "tables", "21_teacher_diversity_distribution.csv"),
  row.names = FALSE
)
message(">>> Saved: outputs/tables/21_teacher_diversity_distribution.csv")

# === 9) Visualizations ========================================================
message("\n>>> Creating visualizations...")

dir.create(here::here("outputs", "graphs"), showWarnings = FALSE, recursive = TRUE)

# Plot 1: Teacher diversity by quartile (overall)
p1 <- overall_summary %>%
  select(black_prop_q_label, pct_teachers_white, pct_teachers_non_white,
         pct_teachers_african_american, pct_teachers_hispanic, pct_teachers_asian) %>%
  pivot_longer(
    cols = starts_with("pct_teachers_"),
    names_to = "race_group",
    values_to = "percentage"
  ) %>%
  mutate(
    race_group = case_when(
      race_group == "pct_teachers_white" ~ "White",
      race_group == "pct_teachers_non_white" ~ "Non-White (All)",
      race_group == "pct_teachers_african_american" ~ "African American",
      race_group == "pct_teachers_hispanic" ~ "Hispanic/Latino",
      race_group == "pct_teachers_asian" ~ "Asian",
      TRUE ~ race_group
    ),
    race_group = factor(race_group, levels = c(
      "White", "Non-White (All)", "African American", "Hispanic/Latino", "Asian"
    ))
  ) %>%
  ggplot(aes(x = black_prop_q_label, y = percentage, fill = race_group)) +
  geom_col(position = "dodge", alpha = 0.8) +
  geom_text(
    aes(label = sprintf("%.1f%%", percentage)),
    position = position_dodge(width = 0.9),
    vjust = -0.5,
    size = 3
  ) +
  scale_fill_brewer(palette = "Set2", name = "Teacher Race/Ethnicity") +
  scale_y_continuous(
    labels = function(x) paste0(x, "%"),
    expand = expansion(mult = c(0, 0.1))
  ) +
  labs(
    title = "Teacher Diversity by School Black Enrollment Quartile",
    subtitle = "Weighted averages (schools weighted by staff count) | 2018-19 onwards",
    x = "School Black Student Proportion Quartile",
    y = "Percentage of Teachers",
    caption = "Note: Schools with >5 teachers included. Quartiles based on Black student enrollment share."
  ) +
  theme(
    legend.position = "bottom",
    plot.title = element_text(face = "bold", size = 14),
    axis.text.x = element_text(angle = 45, hjust = 1)
  )

ggsave(
  here::here("outputs", "graphs", "21_teacher_diversity_by_quartile.png"),
  p1, width = 12, height = 8, dpi = 300, bg = "white"
)
message(">>> Saved: outputs/graphs/21_teacher_diversity_by_quartile.png")

# Plot 2: Trends over time
p2 <- weighted_summary %>%
  select(academic_year, black_prop_q_label, pct_teachers_white, pct_teachers_non_white) %>%
  pivot_longer(
    cols = c(pct_teachers_white, pct_teachers_non_white),
    names_to = "measure",
    values_to = "percentage"
  ) %>%
  mutate(
    measure = if_else(measure == "pct_teachers_white", "White Teachers", "Non-White Teachers")
  ) %>%
  ggplot(aes(x = academic_year, y = percentage, color = black_prop_q_label, group = black_prop_q_label)) +
  geom_line(linewidth = 1.2) +
  geom_point(size = 2.5) +
  facet_wrap(~ measure, ncol = 1, scales = "free_y") +
  scale_color_manual(values = black_quartile_colors, name = "School Black Student Proportion") +
  scale_y_continuous(
    labels = function(x) paste0(x, "%"),
    expand = expansion(mult = c(0.05, 0.1))
  ) +
  labs(
    title = "Teacher Diversity Trends by Black Enrollment Quartile",
    subtitle = "Weighted averages over time",
    x = "Academic Year",
    y = "Percentage of Teachers"
  ) +
  theme(
    legend.position = "bottom",
    plot.title = element_text(face = "bold", size = 14),
    axis.text.x = element_text(angle = 45, hjust = 1),
    strip.text = element_text(face = "bold")
  )

ggsave(
  here::here("outputs", "graphs", "21_teacher_diversity_trends.png"),
  p2, width = 12, height = 10, dpi = 300, bg = "white"
)
message(">>> Saved: outputs/graphs/21_teacher_diversity_trends.png")

# Plot 3: Distribution boxplots
p3 <- school_level_diversity %>%
  filter(!is.na(school_pct_non_white)) %>%
  ggplot(aes(x = black_prop_q_label, y = school_pct_non_white, fill = black_prop_q_label)) +
  geom_boxplot(alpha = 0.7, outlier.alpha = 0.3) +
  geom_jitter(width = 0.2, alpha = 0.1, size = 0.5) +
  scale_fill_manual(values = black_quartile_colors, guide = "none") +
  scale_y_continuous(
    labels = function(x) paste0(x, "%"),
    limits = c(0, 100)
  ) +
  labs(
    title = "Distribution of Non-White Teacher Percentage by Black Enrollment Quartile",
    subtitle = "Each point is a school-year | Schools with ≥5 teachers",
    x = "School Black Student Proportion Quartile",
    y = "Non-White Teachers (%)",
    caption = "Box shows median and interquartile range. Points show individual schools."
  ) +
  theme(
    plot.title = element_text(face = "bold", size = 14),
    axis.text.x = element_text(angle = 45, hjust = 1)
  )

ggsave(
  here::here("outputs", "graphs", "21_teacher_diversity_distribution.png"),
  p3, width = 10, height = 8, dpi = 300, bg = "white"
)
message(">>> Saved: outputs/graphs/21_teacher_diversity_distribution.png")

# Plot 4: Suspension rates vs teacher diversity
p4 <- overall_summary %>%
  ggplot(aes(x = pct_teachers_non_white, y = suspension_rate)) +
  geom_point(aes(color = black_prop_q_label, size = total_teachers), alpha = 0.8) +
  geom_text(
    aes(label = black_prop_q_label),
    nudge_y = 0.3,
    size = 3.5,
    fontface = "bold"
  ) +
  scale_color_manual(values = black_quartile_colors, name = "Black Student Quartile") +
  scale_size_continuous(name = "Total Teachers", labels = comma) +
  scale_x_continuous(labels = function(x) paste0(x, "%")) +
  scale_y_continuous(labels = function(x) paste0(x, "%")) +
  labs(
    title = "Suspension Rates vs Teacher Diversity by Black Enrollment Quartile",
    subtitle = "Weighted averages | Point size indicates total teacher count",
    x = "Non-White Teachers (%)",
    y = "Student Suspension Rate (%)",
    caption = "Note: Correlation does not imply causation. Many unobserved factors influence outcomes."
  ) +
  theme(
    legend.position = "bottom",
    plot.title = element_text(face = "bold", size = 14)
  )

ggsave(
  here::here("outputs", "graphs", "21_suspension_vs_diversity.png"),
  p4, width = 10, height = 8, dpi = 300, bg = "white"
)
message(">>> Saved: outputs/graphs/21_suspension_vs_diversity.png")

# === 10) Final summary report =================================================
message("\n=== ANALYSIS COMPLETE ===")
message("\nKey findings:")
message("1. Analyzed ", n_distinct(analysis_df$cds_school), " unique schools across ",
        length(unique(analysis_df$academic_year)), " academic years")
message("2. Used weighted averages (schools weighted by staff count)")
message("3. Q1 (Lowest % Black) teacher diversity: ",
        round(overall_summary$pct_teachers_non_white[1], 1), "% non-White")
message("4. Q4 (Highest % Black) teacher diversity: ",
        round(overall_summary$pct_teachers_non_white[4], 1), "% non-White")
message("5. Q1 suspension rate: ", round(overall_summary$suspension_rate[1], 2), "%")
message("6. Q4 suspension rate: ", round(overall_summary$suspension_rate[4], 2), "%")

message("\nOutputs saved to:")
message("  - outputs/tables/21_teacher_diversity_by_quartile_*.csv")
message("  - outputs/graphs/21_teacher_diversity_*.png")

message("\nIMPORTANT: These are correlational patterns only. Avoid causal interpretation.")
message("Many unobserved factors (leadership, funding, community context) influence outcomes.")

invisible(TRUE)
