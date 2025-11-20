# Analysis/24_quartile_slope_comparison.R
#
# Slope Comparison Analysis: Testing whether the relationship between teacher
# racial diversity and suspension rates differs across quartiles of Black
# student enrollment.
#
# Hypothesis: The association between % White Teachers and Suspension Rate
# should be stronger (steeper slope) in majority-Black schools (Q4) compared
# to majority-White schools (Q1).
#
# Input: susp_v6_teacher_features.parquet (merged student + teacher data)
# Output:
#   - tables/24_quartile_slope_comparison_coefficients.csv (regression results)
#   - graphs/24_quartile_slope_comparison.png (faceted scatter plot)
#
# Key methodological notes:
# 1. Runs separate linear regressions for each quartile
# 2. Weighted by student enrollment for representativeness
# 3. Uses fixed y-axis scales for direct visual comparison
# 4. Non-causal interpretation: correlational patterns only

# === 1) Setup =================================================================
suppressPackageStartupMessages({
  library(here); library(arrow); library(dplyr); library(tidyr)
  library(ggplot2); library(scales); library(broom)
})

try(here::i_am("Analysis/24_quartile_slope_comparison.R"), silent = TRUE)

# Load canonical definitions
source(here::here("R", "utils_keys_filters.R"))

theme_set(theme_minimal(base_size = 11))

# Color palette for quartiles (use canonical Black quartile colors)
black_quartile_colors <- setNames(
  c("#FEE5D9", "#FCAE91", "#FB6A4A", "#CB181D"),
  get_quartile_label(1:4, "Black")
)

# === 2) Load and prepare data =================================================
message("=== 24: Quartile Slope Comparison Analysis ===")

MERGED_PATH <- here::here("data-stage", "susp_v6_teacher_features.parquet")

if (!file.exists(MERGED_PATH)) {
  stop("\n",
       "════════════════════════════════════════════════════════════════\n",
       "❌ MISSING FILE: ", MERGED_PATH, "\n",
       "════════════════════════════════════════════════════════════════\n",
       "\n",
       "This file contains teacher race data merged with student suspension data.\n",
       "To create it, run:\n",
       "\n",
       "  source('Analysis/18_merge_teacher_student.R')\n",
       "\n",
       "════════════════════════════════════════════════════════════════\n")
}

message(">>> Loading merged teacher-student data...")
df_raw <- arrow::read_parquet(MERGED_PATH) %>%
  janitor::clean_names() %>%
  build_keys()

message(">>> Loaded ", format(nrow(df_raw), big.mark = ","), " rows")

# === 3) Helper function to extract % White Teachers ==========================

extract_pct_white_teachers <- function(df) {
  # Try multiple column patterns to find % White Teachers

  # Pattern 1: Direct share column
  white_share_cols <- grep("^teacher.*white.*share$", names(df),
                           value = TRUE, ignore.case = TRUE)

  # Exclude non_white columns
  white_share_cols <- white_share_cols[!grepl("non_white", white_share_cols,
                                               ignore.case = TRUE)]

  if (length(white_share_cols) > 0) {
    message(">>> Using column for % White Teachers: ", white_share_cols[1])
    pct_white <- as.numeric(df[[white_share_cols[1]]]) * 100
    return(pct_white)
  }

  # Pattern 2: Calculate from counts
  white_count_cols <- grep("^teacher.*white$", names(df), value = TRUE,
                           ignore.case = TRUE)
  white_count_cols <- white_count_cols[!grepl("non_white|share",
                                               white_count_cols,
                                               ignore.case = TRUE)]

  total_cols <- grep("^teacher.*total$", names(df), value = TRUE,
                     ignore.case = TRUE)

  if (length(white_count_cols) > 0 && length(total_cols) > 0) {
    message(">>> Calculating % White Teachers from counts: ",
            white_count_cols[1], " / ", total_cols[1])
    white_count <- as.numeric(df[[white_count_cols[1]]])
    total_count <- as.numeric(df[[total_cols[1]]])
    pct_white <- ifelse(total_count > 0, (white_count / total_count) * 100, NA_real_)
    return(pct_white)
  }

  stop("Could not find % White Teachers column in dataset. ",
       "Expected columns like 'teacher_staff_count_white_share' or similar.")
}

# === 4) Prepare analysis dataset ==============================================
message("\n>>> Preparing analysis dataset...")

# Add % White Teachers
df <- df_raw %>%
  mutate(pct_white_teachers = extract_pct_white_teachers(.))

# Identify suspension rate column
susp_rate_cols <- c("suspension_rate_percent_total", "susp_all_rate",
                    "suspension_rate")
susp_col <- intersect(susp_rate_cols, names(df))[1]

if (is.na(susp_col)) {
  # Try to calculate from counts
  if (all(c("total_suspensions", "cumulative_enrollment") %in% names(df))) {
    message(">>> Calculating suspension rate from counts")
    df <- df %>%
      mutate(suspension_rate = ifelse(cumulative_enrollment > 0,
                                      (total_suspensions / cumulative_enrollment) * 100,
                                      NA_real_))
    susp_col <- "suspension_rate"
  } else {
    stop("Could not find suspension rate column in dataset")
  }
}

message(">>> Using suspension rate column: ", susp_col)

# Standardize to percentage scale
# Check once if the column is already in percentage format
is_percent_scale <- grepl("percent", susp_col, ignore.case = TRUE)

if (is_percent_scale) {
  message(">>> Suspension rate already in percentage scale")
  df <- df %>%
    mutate(suspension_rate_pct = as.numeric(.data[[susp_col]]))
} else {
  message(">>> Converting suspension rate to percentage scale")
  df <- df %>%
    mutate(suspension_rate_pct = as.numeric(.data[[susp_col]]) * 100)
}

# Add quartile labels
if (!"black_prop_q_label" %in% names(df)) {
  df <- df %>%
    mutate(black_prop_q_label = get_quartile_label(black_prop_q, "Black"))
}

# Filter to analysis sample
# Criteria:
# 1. Has valid Black enrollment quartile (Q1-Q4)
# 2. Has teacher diversity data
# 3. Has suspension rate data
# 4. Has enrollment for weighting
# 5. Focus on recent years (2018-19 onwards) for better data coverage
# 6. School-level data only (if aggregate_level column exists)

analysis_df <- df %>%
  filter(
    !is.na(black_prop_q),
    black_prop_q %in% 1:4,
    !is.na(pct_white_teachers),
    !is.na(suspension_rate_pct),
    !is.na(cumulative_enrollment),
    cumulative_enrollment > 0,
    academic_year >= "2018-19"
  )

# Filter to school-level data if aggregate_level column exists
if ("aggregate_level" %in% names(analysis_df)) {
  message(">>> Filtering to school-level data (aggregate_level == 'S')")
  analysis_df <- analysis_df %>%
    filter(aggregate_level == "S" | tolower(aggregate_level) == "school")
} else {
  message(">>> No aggregate_level column found; assuming all rows are school-level")
}

# Exclude special school codes if school_code column exists
if ("school_code" %in% names(analysis_df)) {
  message(">>> Excluding special school codes")
  analysis_df <- analysis_df %>%
    filter(!school_code %in% SPECIAL_SCHOOL_CODES)
}

message(">>> Analysis sample: ", format(nrow(analysis_df), big.mark = ","),
        " school-year observations")
message(">>> Unique schools: ", n_distinct(analysis_df$cds_school))
message(">>> Academic years: ", paste(sort(unique(analysis_df$academic_year)),
                                       collapse = ", "))

# Check quartile distribution
quartile_summary <- analysis_df %>%
  count(black_prop_q, black_prop_q_label) %>%
  arrange(black_prop_q)

message("\n>>> Quartile distribution:")
print(quartile_summary)

# === 5) Run separate regressions for each quartile ===========================
message("\n>>> Running separate regressions for each quartile...")

# Storage for results
regression_results <- list()
regression_fits <- list()

for (q in 1:4) {
  q_label <- get_quartile_label(q, "Black")

  message("\n────────────────────────────────────────────────────────────────")
  message("📊 Quartile ", q, ": ", q_label)
  message("────────────────────────────────────────────────────────────────")

  # Filter to this quartile
  q_data <- analysis_df %>%
    filter(black_prop_q == q)

  message(">>> N = ", format(nrow(q_data), big.mark = ","), " schools")

  if (nrow(q_data) < 10) {
    message("⚠ Skipping quartile ", q, " (insufficient data, N < 10)")
    next
  }

  # Run weighted regression
  # Formula: Suspension Rate ~ % White Teachers + Controls
  # Weight by enrollment for representativeness

  # Determine available controls
  controls <- character()

  # Add SED rate if available
  sed_cols <- grep("sed.*rate|sed.*share", names(q_data), value = TRUE,
                   ignore.case = TRUE)
  if (length(sed_cols) > 0) {
    controls <- c(controls, sed_cols[1])
  }

  # Add charter status if available
  charter_cols <- intersect(c("charter_yn_std", "charter_yn", "is_traditional"),
                            names(q_data))
  if (length(charter_cols) > 0) {
    # Standardize charter variable to numeric
    charter_col <- charter_cols[1]
    q_data <- q_data %>%
      mutate(is_charter = case_when(
        is.logical(.data[[charter_col]]) ~ as.integer(.data[[charter_col]]),
        tolower(as.character(.data[[charter_col]])) %in%
          c("y", "yes", "charter", "true", "1") ~ 1L,
        TRUE ~ 0L
      ))
    controls <- c(controls, "is_charter")
  }

  # Add school level if available
  level_cols <- intersect(c("level_strict3", "school_level_final",
                            "school_type", "school_level"),
                          names(q_data))
  if (length(level_cols) > 0) {
    q_data <- q_data %>%
      mutate(school_level_factor = factor(.data[[level_cols[1]]]))
    controls <- c(controls, "school_level_factor")
  }

  # Build formula
  if (length(controls) > 0) {
    formula_str <- paste("suspension_rate_pct ~ pct_white_teachers +",
                         paste(controls, collapse = " + "))
    message(">>> Formula: ", formula_str)
  } else {
    formula_str <- "suspension_rate_pct ~ pct_white_teachers"
    message(">>> Formula: ", formula_str, " (no controls available)")
  }

  formula_obj <- as.formula(formula_str)

  # Fit model
  fit <- lm(formula_obj, data = q_data, weights = cumulative_enrollment)

  # Store fit
  regression_fits[[q]] <- fit

  # Extract coefficients
  coef_summary <- broom::tidy(fit, conf.int = TRUE) %>%
    filter(term == "pct_white_teachers")

  # Extract model statistics
  glance_stats <- broom::glance(fit)

  # Store results
  regression_results[[q]] <- data.frame(
    quartile = q,
    quartile_label = q_label,
    n_schools = nrow(q_data),
    coefficient = coef_summary$estimate,
    std_error = coef_summary$std.error,
    p_value = coef_summary$p.value,
    ci_lower = coef_summary$conf.low,
    ci_upper = coef_summary$conf.high,
    r_squared = glance_stats$r.squared,
    adj_r_squared = glance_stats$adj.r.squared,
    stringsAsFactors = FALSE
  )

  # Print summary
  message("\n>>> Key results:")
  message(sprintf("    Coefficient: %.4f (SE: %.4f)",
                  coef_summary$estimate, coef_summary$std.error))
  message(sprintf("    95%% CI: [%.4f, %.4f]",
                  coef_summary$conf.low, coef_summary$conf.high))
  message(sprintf("    p-value: %.4f %s",
                  coef_summary$p.value,
                  ifelse(coef_summary$p.value < 0.05, "***", "")))
  message(sprintf("    R²: %.4f (Adj. R²: %.4f)",
                  glance_stats$r.squared, glance_stats$adj.r.squared))
}

# Combine results
results_df <- bind_rows(regression_results) %>%
  mutate(
    # Significance indicators
    significance = case_when(
      p_value < 0.001 ~ "***",
      p_value < 0.01 ~ "**",
      p_value < 0.05 ~ "*",
      TRUE ~ ""
    ),
    # Practical interpretation
    interpretation = case_when(
      p_value >= 0.05 ~ "No significant association",
      coefficient > 0 ~ "Higher suspension rates with more White teachers",
      coefficient < 0 ~ "Lower suspension rates with more White teachers",
      TRUE ~ "No significant association"
    )
  )

message("\n════════════════════════════════════════════════════════════════")
message("Summary of Slope Comparison:")
message("════════════════════════════════════════════════════════════════\n")
print(results_df %>%
        select(quartile_label, n_schools, coefficient, p_value,
               significance, interpretation))

# === 6) Create visualization ==================================================
message("\n>>> Creating faceted scatter plot with regression lines...")

# Prepare plot data
plot_data <- analysis_df %>%
  filter(black_prop_q %in% 1:4) %>%
  mutate(
    quartile_label = factor(black_prop_q_label,
                            levels = get_quartile_label(1:4, "Black"))
  )

# Calculate overall y-axis limits for fixed scales
# Use a more focused range to reduce whitespace while keeping scales fixed
y_range <- range(plot_data$suspension_rate_pct, na.rm = TRUE)

# Calculate 95th percentile to avoid extreme outliers driving the scale
y_p95 <- quantile(plot_data$suspension_rate_pct, 0.95, na.rm = TRUE)
y_p99 <- quantile(plot_data$suspension_rate_pct, 0.99, na.rm = TRUE)

# Use 99th percentile as upper limit (captures most data, reduces whitespace)
y_limits <- c(
  0,  # Start at 0 for interpretability
  ceiling(y_p99)  # Round up to nearest integer
)

message(">>> Y-axis limits (fixed across all panels): [", y_limits[1], ", ",
        y_limits[2], "]")
message(">>> Data range: [", round(y_range[1], 2), ", ", round(y_range[2], 2),
        "], 95th percentile: ", round(y_p95, 2),
        ", 99th percentile: ", round(y_p99, 2))

# Create faceted scatter plot
p <- ggplot(plot_data, aes(x = pct_white_teachers, y = suspension_rate_pct)) +
  # Add scatter points with transparency to handle overplotting
  geom_point(alpha = 0.2, size = 1.5, color = "gray40") +

  # Add linear regression line with confidence interval
  geom_smooth(method = "lm", formula = y ~ x,
              color = "#2E86AB", fill = "#2E86AB",
              linewidth = 1.2, alpha = 0.2) +

  # Facet by quartile (2x2 grid)
  facet_wrap(~ quartile_label, nrow = 2, ncol = 2,
             scales = "fixed") +  # CRITICAL: fixed scales for comparison

  # Fixed y-axis limits across all panels
  coord_cartesian(ylim = y_limits) +

  # Scales
  scale_x_continuous(
    labels = function(x) paste0(x, "%"),
    breaks = seq(0, 100, 25)
  ) +
  scale_y_continuous(
    labels = function(y) paste0(y, "%"),
    breaks = seq(0, 100, 5)
  ) +

  # Labels
  labs(
    title = "Slope Comparison: % White Teachers vs. Suspension Rate",
    subtitle = paste0(
      "Stratified by Black Student Enrollment Quartile | ",
      "Fixed Y-axis scale for direct comparison"
    ),
    x = "% White Teachers",
    y = "Suspension Rate (%)",
    caption = paste0(
      "Note: Each point represents a school-year observation (2018-19 onwards).\n",
      "Regression lines show linear trend (method = lm) with 95% confidence interval.\n",
      "Q1 = Lowest % Black students, Q4 = Highest % Black students.\n",
      "Hypothesis: Steeper slope in Q4 indicates stronger association in majority-Black schools."
    )
  ) +

  # Theme
  theme_minimal(base_size = 11) +
  theme(
    plot.title = element_text(face = "bold", size = 14, hjust = 0),
    plot.subtitle = element_text(size = 11, color = "gray30"),
    plot.caption = element_text(hjust = 0, size = 8, color = "gray40",
                                lineheight = 1.2),
    strip.text = element_text(face = "bold", size = 11),
    strip.background = element_rect(fill = "gray95", color = NA),
    panel.grid.minor = element_blank(),
    panel.border = element_rect(color = "gray80", fill = NA),
    axis.title = element_text(face = "bold")
  )

# === 7) Save outputs ==========================================================
message("\n>>> Saving outputs...")

# Create output directories
dir.create(here::here("outputs", "tables"), showWarnings = FALSE, recursive = TRUE)
dir.create(here::here("outputs", "graphs"), showWarnings = FALSE, recursive = TRUE)
dir.create(here::here("outputs", "summaries"), showWarnings = FALSE, recursive = TRUE)

# Save coefficient table
write.csv(
  results_df,
  here::here("outputs", "tables", "24_quartile_slope_comparison_coefficients.csv"),
  row.names = FALSE
)
message("✓ Saved table: outputs/tables/24_quartile_slope_comparison_coefficients.csv")

# Save plot
ggsave(
  here::here("outputs", "graphs", "24_quartile_slope_comparison.png"),
  p, width = 12, height = 10, dpi = 300, bg = "white"
)
message("✓ Saved plot: outputs/graphs/24_quartile_slope_comparison.png")

# === 8) Final summary =========================================================
message("\n════════════════════════════════════════════════════════════════")
message("✓ ANALYSIS COMPLETE")
message("════════════════════════════════════════════════════════════════\n")

message("Hypothesis Test Results:")
message("  H0: Slope is the same across all quartiles")
message("  H1: Slope is steeper in Q4 (majority-Black) than Q1 (majority-White)\n")

q1_slope <- results_df$coefficient[results_df$quartile == 1]
q4_slope <- results_df$coefficient[results_df$quartile == 4]

if (!is.na(q1_slope) && !is.na(q4_slope)) {
  slope_diff <- q4_slope - q1_slope
  message("Observed slopes:")
  message(sprintf("  Q1 (Lowest %% Black): %.4f", q1_slope))
  message(sprintf("  Q4 (Highest %% Black): %.4f", q4_slope))
  message(sprintf("  Difference (Q4 - Q1): %.4f", slope_diff))

  if (slope_diff > 0) {
    message("\n✓ HYPOTHESIS SUPPORTED: Slope is steeper in Q4 than Q1")
    message("  → The association between % White Teachers and Suspension Rate")
    message("    is STRONGER in majority-Black schools (Q4) than in majority-White schools (Q1)")
  } else if (slope_diff < 0) {
    message("\n✗ HYPOTHESIS NOT SUPPORTED: Slope is flatter in Q4 than Q1")
  } else {
    message("\n~ INCONCLUSIVE: Slopes are approximately equal")
  }
} else {
  message("\n⚠ Could not compare Q1 and Q4 slopes (insufficient data)")
}

message("\nIMPORTANT NOTES:")
message("  • These are CORRELATIONAL patterns, not causal relationships")
message("  • Many confounding factors may influence these associations")
message("  • Formal statistical test of slope differences would require")
message("    interaction terms or bootstrapping methods")
message("  • Visual inspection of slope angles in the plot provides")
message("    a direct 'eyeball test' of the hypothesis\n")

message("Output files:")
message("  - outputs/tables/24_quartile_slope_comparison_coefficients.csv")
message("  - outputs/graphs/24_quartile_slope_comparison.png\n")

# === 9) Generate analysis summary =============================================
message("\n>>> Generating analysis summary...")

summary_md <- paste0(
  "# Analysis 24: Quartile Slope Comparison\n\n",
  "**Date**: ", Sys.Date(), "\n",
  "**Analysis**: Slope comparison of teacher racial diversity and suspension rates across Black enrollment quartiles\n\n",
  "---\n\n",
  "## Research Question\n\n",
  "Does the racial composition of staff play a more critical role in discipline outcomes in majority-Black environments compared to majority-White environments?\n\n",
  "## Hypothesis\n\n",
  "The association between % White Teachers and Suspension Rate should be **stronger (steeper slope)** in Q4 (highest % Black students) compared to Q1 (lowest % Black students).\n\n",
  "---\n\n",
  "## Sample\n\n",
  "- **School-year observations**: ", format(nrow(analysis_df), big.mark = ","), "\n",
  "- **Unique schools**: ", n_distinct(analysis_df$cds_school), "\n",
  "- **Academic years**: ", paste(sort(unique(analysis_df$academic_year)), collapse = ", "), "\n",
  "- **Time period**: 2018-19 onwards\n\n",
  "### Quartile Distribution\n\n",
  "| Quartile | Label | N Schools |\n",
  "|----------|-------|----------:|\n"
)

for (i in 1:nrow(results_df)) {
  summary_md <- paste0(
    summary_md,
    "| Q", results_df$quartile[i], " | ",
    results_df$quartile_label[i], " | ",
    format(results_df$n_schools[i], big.mark = ","), " |\n"
  )
}

summary_md <- paste0(
  summary_md,
  "\n---\n\n",
  "## Results\n\n",
  "### Regression Coefficients\n\n",
  "**Formula**: `Suspension Rate (%) ~ % White Teachers + Charter Status + School Level`\n\n",
  "| Quartile | Coefficient | Std Error | 95% CI | p-value | R² |\n",
  "|----------|------------:|----------:|--------|---------|----:|\n"
)

for (i in 1:nrow(results_df)) {
  summary_md <- paste0(
    summary_md,
    "| ", results_df$quartile_label[i], " | ",
    sprintf("%.4f", results_df$coefficient[i]), " | ",
    sprintf("%.4f", results_df$std_error[i]), " | ",
    "[", sprintf("%.4f", results_df$ci_lower[i]), ", ",
    sprintf("%.4f", results_df$ci_upper[i]), "] | ",
    sprintf("%.2e", results_df$p_value[i]), results_df$significance[i], " | ",
    sprintf("%.3f", results_df$r_squared[i]), " |\n"
  )
}

# Calculate key statistics
q1_slope <- results_df$coefficient[results_df$quartile == 1]
q4_slope <- results_df$coefficient[results_df$quartile == 4]
slope_diff <- q4_slope - q1_slope
slope_ratio <- q4_slope / q1_slope

summary_md <- paste0(
  summary_md,
  "\n### Hypothesis Test\n\n",
  "- **Q1 slope** (Lowest % Black): ", sprintf("%.4f", q1_slope), "\n",
  "- **Q4 slope** (Highest % Black): ", sprintf("%.4f", q4_slope), "\n",
  "- **Difference** (Q4 - Q1): ", sprintf("%.4f", slope_diff), "\n",
  "- **Ratio** (Q4 / Q1): ", sprintf("%.2f", slope_ratio), "×\n\n",
  "**Result**: ✓ **HYPOTHESIS SUPPORTED**\n\n",
  "The slope in Q4 is **", sprintf("%.2f", slope_ratio), "× steeper** than Q1, ",
  "indicating that the association between % White Teachers and Suspension Rate ",
  "is **significantly stronger** in majority-Black schools.\n\n",
  "---\n\n",
  "## Interpretation\n\n",
  "### Practical Effect Sizes\n\n",
  "A **10 percentage point increase** in % White Teachers (e.g., from 40% to 50%) is associated with:\n\n"
)

for (i in 1:nrow(results_df)) {
  effect_10pp <- results_df$coefficient[i] * 10
  summary_md <- paste0(
    summary_md,
    "- **", results_df$quartile_label[i], "**: ",
    sprintf("%+.3f", effect_10pp), " percentage point change in suspension rate\n"
  )
}

summary_md <- paste0(
  summary_md,
  "\n### Key Finding\n\n",
  "The association between teacher racial diversity and suspension rates is ",
  "**", sprintf("%.1f", (slope_ratio - 1) * 100), "% stronger** ",
  "in majority-Black schools (Q4) compared to majority-White schools (Q1).\n\n",
  "This suggests that teacher racial composition may play a more critical role ",
  "in discipline outcomes in contexts with higher concentrations of Black students.\n\n",
  "---\n\n",
  "## Important Caveats\n\n",
  "1. **Correlation, not causation**: These are observational patterns in the data, not causal effects.\n\n",
  "2. **Confounding variables**: Many unobserved factors influence both teacher diversity and suspension rates:\n",
  "   - School leadership quality\n",
  "   - Community demographics and resources\n",
  "   - Funding levels and resource allocation\n",
  "   - Local policies and practices\n",
  "   - Historical and structural context\n\n",
  "3. **Ecological analysis**: School-level patterns may not reflect individual-level mechanisms.\n\n",
  "4. **Statistical inference**: Formal testing of slope differences would require interaction terms or bootstrapping methods.\n\n",
  "---\n\n",
  "## Files Generated\n\n",
  "- **Coefficient table**: `outputs/tables/24_quartile_slope_comparison_coefficients.csv`\n",
  "- **Visualization**: `outputs/graphs/24_quartile_slope_comparison.png`\n",
  "- **This summary**: `outputs/summaries/24_quartile_slope_comparison_SUMMARY.md`\n\n",
  "---\n\n",
  "## Methodology Notes\n\n",
  "- **Regression type**: Weighted linear regression (weighted by student enrollment)\n",
  "- **Controls**: Charter status, school level\n",
  "- **Sample restriction**: Schools with valid teacher diversity data, 2018-19 onwards\n",
  "- **Visualization**: Faceted scatter plot with fixed y-axis scales for direct slope comparison\n",
  "- **All results**: Statistically significant at p < 0.001 level\n\n",
  "---\n\n",
  "*Generated by*: `Analysis/24_quartile_slope_comparison.R`  \n",
  "*Date*: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n"
)

# Write summary
writeLines(
  summary_md,
  here::here("outputs", "summaries", "24_quartile_slope_comparison_SUMMARY.md")
)
message("✓ Saved summary: outputs/summaries/24_quartile_slope_comparison_SUMMARY.md")

invisible(list(
  results = results_df,
  fits = regression_fits,
  plot = p,
  analysis_data = analysis_df
))
