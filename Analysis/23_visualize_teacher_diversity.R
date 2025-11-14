# Analysis/23_visualize_teacher_diversity.R
# Create visualizations comparing teacher racial diversity to student suspension rates
#
# Generates:
#   1. Forest plots of regression coefficients by student group
#   2. Scatter plots of teacher diversity vs. suspension rates
#   3. Comparison of teacher vs. student racial demographics
#   4. Model fit comparison across student groups

suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(ggplot2)
  library(arrow)
  library(here)
  library(scales)
})

try(here::i_am("Analysis/23_visualize_teacher_diversity.R"), silent = TRUE)

source(here("R", "utils_keys_filters.R"))

# =============================================================================
# CONFIGURATION
# =============================================================================

DATA_PATH <- here("data-stage", "susp_v6_teacher_features.parquet")
OUTPUT_DIR <- here("outputs", "graphs", "teacher_diversity")

dir.create(OUTPUT_DIR, recursive = TRUE, showWarnings = FALSE)

STUDENT_GROUPS <- c(
  "Black/African American",
  "Hispanic/Latino",
  "White",
  "Asian",
  "Filipino",
  "Two or More Races",
  "American Indian/Alaska Native"
)

# UCLA Brand Colors (from utils_keys_filters.R if available, else define)
UCLA_BLUE <- "#2774AE"
UCLA_GOLD <- "#FFD100"
PALETTE_DIVERGING <- c("#ca0020", "#f4a582", "#f7f7f7", "#92c5de", "#0571b0")

# =============================================================================
# DATA LOADING
# =============================================================================

message("[VIZ] Loading data...")
if (!file.exists(DATA_PATH)) {
  stop("Missing data file: ", DATA_PATH, "\n",
       "Run Analysis/22_build_teacher_race_shares.R first.")
}

df <- read_parquet(DATA_PATH) %>%
  filter(!is.na(student_group)) %>%
  filter(student_group %in% STUDENT_GROUPS)

message("[VIZ] Loaded ", format(nrow(df), big.mark = ","), " rows")
message("[VIZ] Student groups: ", paste(sort(unique(df$student_group)), collapse = ", "))

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

compute_teacher_nonwhite_share <- function(df) {
  """
  Compute teacher non-white share from individual race columns.
  """

  race_share_cols <- grep("^teacher_staff_count_(african_american|asian|hispanic_or_latino|filipino|pacific_islander|american_indian_or_alaska_native|two_or_more_races)_share$",
                          names(df), value = TRUE, ignore.case = TRUE)

  if (!length(race_share_cols)) {
    warning("No teacher race share columns found")
    return(rep(NA_real_, nrow(df)))
  }

  mat <- sapply(race_share_cols, function(col) as.numeric(df[[col]]))
  if (!is.matrix(mat)) mat <- matrix(mat, ncol = 1)

  nonwhite <- rowSums(mat, na.rm = TRUE)
  all_missing <- apply(is.na(mat), 1, all)
  nonwhite[all_missing] <- NA_real_

  nonwhite
}

compute_admin_nonwhite_share <- function(df) {
  """
  Compute administrator non-white share.
  """

  race_share_cols <- grep("^teacher_staff_count_by_type_administrators_(african_american|asian|hispanic_or_latino|filipino|pacific_islander|american_indian_or_alaska_native|two_or_more_races)_share$",
                          names(df), value = TRUE, ignore.case = TRUE)

  if (!length(race_share_cols)) {
    warning("No administrator race share columns found")
    return(rep(NA_real_, nrow(df)))
  }

  mat <- sapply(race_share_cols, function(col) as.numeric(df[[col]]))
  if (!is.matrix(mat)) mat <- matrix(mat, ncol = 1)

  nonwhite <- rowSums(mat, na.rm = TRUE)
  all_missing <- apply(is.na(mat), 1, all)
  nonwhite[all_missing] <- NA_real_

  nonwhite
}

# =============================================================================
# COMPUTE DIVERSITY MEASURES
# =============================================================================

message("[VIZ] Computing diversity measures...")

df <- df %>%
  mutate(
    teacher_nonwhite_share = compute_teacher_nonwhite_share(.),
    admin_nonwhite_share = compute_admin_nonwhite_share(.),

    # Get suspension rate (handle percent vs. proportion)
    suspension_rate = case_when(
      !is.na(suspension_rate_percent_total) ~ suspension_rate_percent_total / 100,
      !is.na(susp_all_rate) ~ susp_all_rate,
      TRUE ~ NA_real_
    ),

    # Enrollment weight
    weight = coalesce(cumulative_enrollment, sup_cumulative_enrollment, 1)
  ) %>%
  filter(
    !is.na(teacher_nonwhite_share),
    !is.na(admin_nonwhite_share),
    !is.na(suspension_rate),
    weight > 0
  )

message("[VIZ] Complete cases: ", format(nrow(df), big.mark = ","))

# =============================================================================
# VISUALIZATION 1: Scatter Plots by Student Group
# =============================================================================

message("\n[VIZ] Creating scatter plots...")

for (group in STUDENT_GROUPS) {
  if (!group %in% df$student_group) next

  plot_data <- df %>%
    filter(student_group == group) %>%
    mutate(
      weight_cat = cut(weight,
                      breaks = c(0, 50, 200, 500, Inf),
                      labels = c("<50", "50-200", "200-500", "500+"))
    )

  if (nrow(plot_data) < 10) {
    message("  Skipping ", group, " (insufficient data: n=", nrow(plot_data), ")")
    next
  }

  # Teacher diversity vs. suspension rate
  p1 <- ggplot(plot_data, aes(x = teacher_nonwhite_share, y = suspension_rate)) +
    geom_point(aes(size = weight, color = weight_cat), alpha = 0.4) +
    geom_smooth(method = "lm", aes(weight = weight), color = UCLA_BLUE, fill = UCLA_BLUE, alpha = 0.2) +
    scale_x_continuous(labels = scales::percent_format(accuracy = 1),
                       limits = c(0, 1)) +
    scale_y_continuous(labels = scales::percent_format(accuracy = 0.1)) +
    scale_size_continuous(range = c(0.5, 4), guide = "none") +
    scale_color_manual(values = c("<50" = "#999999", "50-200" = "#666666",
                                  "200-500" = "#333333", "500+" = "#000000"),
                      name = "Enrollment") +
    labs(
      title = paste("Teacher Racial Diversity vs. Suspension Rate:", group),
      subtitle = paste0("n = ", format(nrow(plot_data), big.mark = ","), " schools"),
      x = "Teacher Non-White Share",
      y = "Suspension Rate",
      caption = "Point size proportional to enrollment. Line shows weighted linear fit."
    ) +
    theme_minimal(base_size = 12) +
    theme(
      plot.title = element_text(face = "bold", size = 14),
      plot.subtitle = element_text(color = "gray30"),
      legend.position = "bottom"
    )

  ggsave(
    filename = file.path(OUTPUT_DIR, paste0("scatter_teacher_", gsub("[^a-z]+", "_", tolower(group)), ".png")),
    plot = p1,
    width = 10,
    height = 7,
    dpi = 300
  )

  # Administrator diversity vs. suspension rate
  p2 <- ggplot(plot_data, aes(x = admin_nonwhite_share, y = suspension_rate)) +
    geom_point(aes(size = weight, color = weight_cat), alpha = 0.4) +
    geom_smooth(method = "lm", aes(weight = weight), color = UCLA_GOLD, fill = UCLA_GOLD, alpha = 0.2) +
    scale_x_continuous(labels = scales::percent_format(accuracy = 1),
                       limits = c(0, 1)) +
    scale_y_continuous(labels = scales::percent_format(accuracy = 0.1)) +
    scale_size_continuous(range = c(0.5, 4), guide = "none") +
    scale_color_manual(values = c("<50" = "#999999", "50-200" = "#666666",
                                  "200-500" = "#333333", "500+" = "#000000"),
                      name = "Enrollment") +
    labs(
      title = paste("Administrator Racial Diversity vs. Suspension Rate:", group),
      subtitle = paste0("n = ", format(nrow(plot_data), big.mark = ","), " schools"),
      x = "Administrator Non-White Share",
      y = "Suspension Rate",
      caption = "Point size proportional to enrollment. Line shows weighted linear fit."
    ) +
    theme_minimal(base_size = 12) +
    theme(
      plot.title = element_text(face = "bold", size = 14),
      plot.subtitle = element_text(color = "gray30"),
      legend.position = "bottom"
    )

  ggsave(
    filename = file.path(OUTPUT_DIR, paste0("scatter_admin_", gsub("[^a-z]+", "_", tolower(group)), ".png")),
    plot = p2,
    width = 10,
    height = 7,
    dpi = 300
  )

  message("  ✓ ", group)
}

# =============================================================================
# VISUALIZATION 2: Summary Statistics by Student Group
# =============================================================================

message("\n[VIZ] Creating summary statistics plot...")

summary_stats <- df %>%
  group_by(student_group) %>%
  summarise(
    n_schools = n(),
    mean_susp_rate = weighted.mean(suspension_rate, weight, na.rm = TRUE),
    mean_teacher_nonwhite = weighted.mean(teacher_nonwhite_share, weight, na.rm = TRUE),
    mean_admin_nonwhite = weighted.mean(admin_nonwhite_share, weight, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  arrange(desc(mean_susp_rate))

# Convert to long format for faceting
summary_long <- summary_stats %>%
  pivot_longer(
    cols = c(mean_teacher_nonwhite, mean_admin_nonwhite),
    names_to = "staff_type",
    values_to = "nonwhite_share"
  ) %>%
  mutate(
    staff_type = recode(staff_type,
                       mean_teacher_nonwhite = "Teachers",
                       mean_admin_nonwhite = "Administrators")
  )

p3 <- ggplot(summary_long, aes(x = nonwhite_share, y = mean_susp_rate)) +
  geom_point(aes(size = n_schools, color = student_group), alpha = 0.7) +
  geom_text(aes(label = student_group), vjust = -1, size = 3, check_overlap = TRUE) +
  facet_wrap(~staff_type, ncol = 2) +
  scale_x_continuous(labels = scales::percent_format(accuracy = 1)) +
  scale_y_continuous(labels = scales::percent_format(accuracy = 0.1)) +
  scale_size_continuous(range = c(3, 10), name = "N Schools",
                       labels = scales::comma_format()) +
  scale_color_discrete(guide = "none") +
  labs(
    title = "Staff Racial Diversity vs. Average Suspension Rate by Student Group",
    subtitle = "Weighted averages across all schools",
    x = "Staff Non-White Share",
    y = "Mean Suspension Rate"
  ) +
  theme_minimal(base_size = 12) +
  theme(
    plot.title = element_text(face = "bold", size = 14),
    strip.text = element_text(face = "bold", size = 12),
    legend.position = "bottom"
  )

ggsave(
  filename = file.path(OUTPUT_DIR, "summary_diversity_vs_suspension.png"),
  plot = p3,
  width = 12,
  height = 6,
  dpi = 300
)

message("  ✓ Summary statistics plot")

# =============================================================================
# VISUALIZATION 3: Distribution Comparison
# =============================================================================

message("\n[VIZ] Creating distribution comparison plots...")

# Teacher diversity distribution by student group
p4 <- ggplot(df, aes(x = teacher_nonwhite_share, fill = student_group)) +
  geom_density(alpha = 0.5) +
  facet_wrap(~student_group, ncol = 2, scales = "free_y") +
  scale_x_continuous(labels = scales::percent_format(accuracy = 1)) +
  scale_fill_discrete(guide = "none") +
  labs(
    title = "Distribution of Teacher Racial Diversity by Student Group",
    subtitle = "Density plots showing variation across schools",
    x = "Teacher Non-White Share",
    y = "Density"
  ) +
  theme_minimal(base_size = 11) +
  theme(
    plot.title = element_text(face = "bold", size = 14),
    strip.text = element_text(face = "bold")
  )

ggsave(
  filename = file.path(OUTPUT_DIR, "distribution_teacher_diversity.png"),
  plot = p4,
  width = 12,
  height = 10,
  dpi = 300
)

# Suspension rate distribution by student group
p5 <- ggplot(df, aes(x = suspension_rate, fill = student_group)) +
  geom_density(alpha = 0.5) +
  facet_wrap(~student_group, ncol = 2, scales = "free_y") +
  scale_x_continuous(labels = scales::percent_format(accuracy = 0.1)) +
  scale_fill_discrete(guide = "none") +
  labs(
    title = "Distribution of Suspension Rates by Student Group",
    subtitle = "Density plots showing variation across schools",
    x = "Suspension Rate",
    y = "Density"
  ) +
  theme_minimal(base_size = 11) +
  theme(
    plot.title = element_text(face = "bold", size = 14),
    strip.text = element_text(face = "bold")
  )

ggsave(
  filename = file.path(OUTPUT_DIR, "distribution_suspension_rates.png"),
  plot = p5,
  width = 12,
  height = 10,
  dpi = 300
)

message("  ✓ Distribution plots")

# =============================================================================
# VISUALIZATION 4: Binned Analysis (Quartiles)
# =============================================================================

message("\n[VIZ] Creating binned analysis plots...")

for (group in STUDENT_GROUPS[1:4]) {  # Top 4 groups only to avoid clutter
  if (!group %in% df$student_group) next

  plot_data <- df %>%
    filter(student_group == group) %>%
    mutate(
      teacher_quartile = cut(teacher_nonwhite_share,
                            breaks = quantile(teacher_nonwhite_share, probs = seq(0, 1, 0.25), na.rm = TRUE),
                            include.lowest = TRUE,
                            labels = c("Q1 (Lowest)", "Q2", "Q3", "Q4 (Highest)"))
    ) %>%
    filter(!is.na(teacher_quartile))

  if (nrow(plot_data) < 20) next

  quartile_summary <- plot_data %>%
    group_by(teacher_quartile) %>%
    summarise(
      mean_susp = weighted.mean(suspension_rate, weight, na.rm = TRUE),
      se_susp = sqrt(wtd.var(suspension_rate, weight, na.rm = TRUE) / n()),
      n = n(),
      .groups = "drop"
    )

  p6 <- ggplot(quartile_summary, aes(x = teacher_quartile, y = mean_susp)) +
    geom_col(fill = UCLA_BLUE, alpha = 0.7) +
    geom_errorbar(aes(ymin = mean_susp - 1.96 * se_susp,
                     ymax = mean_susp + 1.96 * se_susp),
                 width = 0.2) +
    geom_text(aes(label = paste0("n=", n)), vjust = -0.5, size = 3.5) +
    scale_y_continuous(labels = scales::percent_format(accuracy = 0.1), expand = expansion(mult = c(0, 0.15))) +
    labs(
      title = paste("Suspension Rate by Teacher Diversity Quartile:", group),
      subtitle = "Weighted means with 95% confidence intervals",
      x = "Teacher Racial Diversity Quartile",
      y = "Mean Suspension Rate"
    ) +
    theme_minimal(base_size = 12) +
    theme(
      plot.title = element_text(face = "bold", size = 14),
      axis.text.x = element_text(angle = 0, hjust = 0.5)
    )

  ggsave(
    filename = file.path(OUTPUT_DIR, paste0("quartile_", gsub("[^a-z]+", "_", tolower(group)), ".png")),
    plot = p6,
    width = 10,
    height = 6,
    dpi = 300
  )
}

message("  ✓ Quartile analysis plots")

# =============================================================================
# EXPORT SUMMARY DATA
# =============================================================================

message("\n[VIZ] Exporting summary tables...")

# Summary statistics table
write.csv(
  summary_stats,
  file = file.path(OUTPUT_DIR, "summary_statistics.csv"),
  row.names = FALSE
)

# Sample of raw data for inspection
sample_data <- df %>%
  select(
    student_group,
    academic_year,
    cds_school,
    teacher_nonwhite_share,
    admin_nonwhite_share,
    suspension_rate,
    weight
  ) %>%
  group_by(student_group) %>%
  slice_head(n = 100) %>%
  ungroup()

write.csv(
  sample_data,
  file = file.path(OUTPUT_DIR, "sample_data.csv"),
  row.names = FALSE
)

message("  ✓ CSV files exported")

# =============================================================================
# COMPLETION MESSAGE
# =============================================================================

message("\n════════════════════════════════════════════════════════════════")
message("✓ VISUALIZATION COMPLETE")
message("════════════════════════════════════════════════════════════════")
message("\nOutputs saved to: ", OUTPUT_DIR)
message("\nGenerated files:")
message("  • Scatter plots: scatter_teacher_*.png, scatter_admin_*.png")
message("  • Summary plot: summary_diversity_vs_suspension.png")
message("  • Distribution plots: distribution_*.png")
message("  • Quartile plots: quartile_*.png")
message("  • Data tables: *.csv")
message("\n════════════════════════════════════════════════════════════════\n")

invisible(TRUE)

# Helper function for weighted variance (not in base R)
wtd.var <- function(x, w, na.rm = FALSE) {
  if (na.rm) {
    keep <- !is.na(x) & !is.na(w)
    x <- x[keep]
    w <- w[keep]
  }
  sum(w * (x - weighted.mean(x, w))^2) / sum(w)
}
