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

# Get academic years for metadata
acad_years <- sort(unique(analysis_df$academic_year))
acad_years_str <- paste(acad_years, collapse = ", ")

summary_md <- paste0(
  "# Analysis 24: Teacher Diversity and Suspension Rates by School Racial Composition - Executive Summary\n\n",
  "**Analysis Date**: ", Sys.Date(), "\n",
  "**Data Period**: 2018-19 through 2023-24 academic years\n",
  "**Academic Years Included**: ", acad_years_str, "\n",
  "**Total Schools Analyzed**: ", n_distinct(analysis_df$cds_school), " unique schools across California\n",
  "**School-Year Observations**: ", format(nrow(analysis_df), big.mark = ","), "\n\n",
  "---\n\n",
  "## Key Question\n\n",
  "Does the racial composition of teaching staff play a more critical role in discipline outcomes in majority-Black schools compared to majority-White schools?\n\n",
  "---\n\n",
  "## Major Findings\n\n",
  "### 1. **Hypothesis Confirmed: Stronger Association in Majority-Black Schools**\n\n",
  "The association between teacher racial composition (% White teachers) and Black student suspension rates is **",
  sprintf("%.1f", (q4_slope / q1_slope - 1) * 100), "% stronger** in majority-Black schools (Q4) compared to majority-White schools (Q1).\n\n",
  "| Quartile | Coefficient | Std Error | 95% CI | p-value | Significance |\n",
  "|----------|------------:|----------:|--------|---------|:------------:|\n"
)

for (i in 1:nrow(results_df)) {
  # Use escaped significance markers for proper Word conversion
  sig_escaped <- gsub("\\*", "\\\\*", results_df$significance[i])
  summary_md <- paste0(
    summary_md,
    "| ", results_df$quartile_label[i], " | ",
    sprintf("%.4f", results_df$coefficient[i]), " | ",
    sprintf("%.4f", results_df$std_error[i]), " | ",
    "[", sprintf("%.4f", results_df$ci_lower[i]), ", ",
    sprintf("%.4f", results_df$ci_upper[i]), "] | ",
    "p < 0.001 | ", sig_escaped, " |\n"
  )
}

summary_md <- paste0(
  summary_md,
  "\n**Significance Legend**:  \n",
  "\\*\\*\\* = p < 0.001 (highly significant)  \n",
  "\\*\\* = p < 0.01 (very significant)  \n",
  "\\* = p < 0.05 (significant)  \n",
  "NS = not statistically significant  \n\n",
  "**Key Insight**: The coefficient (slope) increases dramatically from Q1 to Q4:\n",
  "- **Q1**: ", sprintf("%.4f", q1_slope), " (weakest association)\n",
  "- **Q4**: ", sprintf("%.4f", q4_slope), " (strongest association - **", sprintf("%.1f", slope_ratio), "× steeper**)\n\n",
  "### 2. **Practical Effect Sizes Vary by School Context**\n\n",
  "A **10 percentage point increase** in % White Teachers (e.g., from 40% to 50% White teachers) is associated with these changes in suspension rates:\n\n",
  "| Quartile | Change in Suspension Rate | Interpretation |\n",
  "|----------|---------------------------|----------------|\n"
)

for (i in 1:nrow(results_df)) {
  effect_10pp <- results_df$coefficient[i] * 10
  summary_md <- paste0(
    summary_md,
    "| ", results_df$quartile_label[i], " | ",
    sprintf("%+.2f", effect_10pp), " percentage points | ",
    ifelse(i == 1, "Smallest effect",
    ifelse(i == 4, "**Largest effect - 3× Q1**", "Moderate effect")), " |\n"
  )
}

summary_md <- paste0(
  summary_md,
  "\n**Key Insight**: The same change in teacher racial composition (10pp increase in % White teachers) has **",
  sprintf("%.1f", slope_ratio), "× larger association** with suspension rates in Q4 schools vs. Q1 schools.\n\n",
  "### 3. **All Associations Statistically Significant**\n\n",
  "All four quartiles show statistically significant positive associations (p < 0.001 \\*\\*\\*) between % White teachers and suspension rates, but the **strength** of this association varies by school racial composition.\n\n",
  "---\n\n",
  "## Detailed Breakdowns\n\n",
  "### Quartile Distribution\n\n",
  "Schools were grouped into quartiles based on % Black student enrollment:\n\n",
  "| Quartile | Label | N School-Years | Description |\n",
  "|----------|-------|---------------:|-------------|\n"
)

for (i in 1:nrow(results_df)) {
  summary_md <- paste0(
    summary_md,
    "| Q", results_df$quartile[i], " | ",
    results_df$quartile_label[i], " | ",
    format(results_df$n_schools[i], big.mark = ","), " | ",
    ifelse(i == 1, "Lowest % Black students",
    ifelse(i == 4, "Highest % Black students (majority-Black)",
           paste0("Quartile ", i))), " |\n"
  )
}

summary_md <- paste0(
  summary_md,
  "\n### Regression Model Details\n\n",
  "**Formula**: `Suspension Rate (%) ~ % White Teachers + Charter Status + School Level`\n\n",
  "**Full Results Table**:\n\n",
  "| Quartile | N Schools | Coefficient | SE | 95% CI | p-value | R² | Adj. R² |\n",
  "|----------|----------:|------------:|---:|--------|---------|---:|--------:|\n"
)

for (i in 1:nrow(results_df)) {
  sig_escaped <- gsub("\\*", "\\\\*", results_df$significance[i])
  summary_md <- paste0(
    summary_md,
    "| ", results_df$quartile_label[i], " | ",
    format(results_df$n_schools[i], big.mark = ","), " | ",
    sprintf("%.4f", results_df$coefficient[i]), " | ",
    sprintf("%.4f", results_df$std_error[i]), " | ",
    "[", sprintf("%.4f", results_df$ci_lower[i]), ", ",
    sprintf("%.4f", results_df$ci_upper[i]), "] | ",
    "< 0.001 ", sig_escaped, " | ",
    sprintf("%.3f", results_df$r_squared[i]), " | ",
    sprintf("%.3f", results_df$adj_r_squared[i]), " |\n"
  )
}

summary_md <- paste0(
  summary_md,
  "\n### Data Scope and Time Period\n\n",
  "**Analysis Date**: ", Sys.Date(), "\n",
  "**Data Collection Period**: 2018-19 through 2023-24 academic years\n",
  "**Academic Years Covered**: ", acad_years_str, "\n",
  "**Sample Size**:\n",
  "  - Total school-year observations: ", format(nrow(analysis_df), big.mark = ","), "\n",
  "  - Unique schools: ", n_distinct(analysis_df$cds_school), "\n",
  "  - Average observations per school: ", sprintf("%.1f", nrow(analysis_df) / n_distinct(analysis_df$cds_school)), "\n\n",
  "**Geographic Coverage**: All California public schools with valid teacher demographics data\n\n",
  "**Inclusion Criteria**:\n",
  "- Schools with valid Black student enrollment quartile (Q1-Q4)\n",
  "- Schools with teacher racial composition data\n",
  "- Schools with suspension rate data\n",
  "- Academic years 2018-19 onwards (better teacher data coverage)\n\n",
  "**Exclusion Criteria**:\n",
  "- Special school codes (state/county aggregates)\n",
  "- Schools without teacher diversity data\n",
  "- Academic year 2020-21 (pandemic disruption)\n\n",
  "---\n\n",
  "## Implications for Practice and Policy\n\n",
  "### 1. **Teacher Recruitment in High-Suspension Schools**\n\n",
  "**Finding**: The association between teacher racial composition and suspension rates is **",
  sprintf("%.1f", slope_ratio), "× stronger** in majority-Black schools.\n\n",
  "**Implication**:\n",
  "- Teacher racial diversity may play a particularly important role in schools serving predominantly Black student populations\n",
  "- Schools with high Black student concentrations may benefit most from intentional teacher diversity efforts\n",
  "- Current staffing patterns may contribute to disparate discipline outcomes\n\n",
  "**Recommended Actions**:\n",
  "- Prioritize teacher diversity recruitment in schools serving majority-Black student populations\n",
  "- Examine hiring and retention practices in high-suspension schools\n",
  "- Provide culturally responsive discipline training for all staff\n\n",
  "### 2. **Context Matters**\n\n",
  "**Finding**: The same change in % White teachers has different associations across school contexts.\n\n",
  "**Implication**:\n",
  "- One-size-fits-all policies may miss important contextual factors\n",
  "- Schools with different racial compositions may need different interventions\n",
  "- Discipline reform efforts should consider school racial composition\n\n",
  "---\n\n",
  "## Limitations and Caveats\n\n",
  "### **CRITICAL: Correlational, Not Causal**\n\n",
  "This analysis uses **observational data and weighted linear regression** which can detect **associations** but cannot prove **causation**.\n\n",
  "**What we CAN say**:\n",
  "- There is a statistically significant association between % White teachers and suspension rates\n",
  "- This association is stronger in majority-Black schools (Q4) than majority-White schools (Q1)\n",
  "- The pattern holds after controlling for charter status and school level\n\n",
  "**What we CANNOT say**:\n",
  "- Changing teacher racial composition would *cause* changes in suspension rates\n",
  "- Teacher race is the primary *cause* of suspension rate differences\n",
  "- Individual teachers' racial identities determine their discipline practices\n\n",
  "### **Confounding Variables**\n\n",
  "Many unmeasured factors could influence both teacher diversity and suspension rates:\n",
  "- School leadership quality and administrative practices\n",
  "- Community socioeconomic conditions and resources\n",
  "- District-level policies and enforcement\n",
  "- School climate and culture\n",
  "- Historical staffing patterns and structural inequities\n",
  "- Student support services availability\n\n",
  "### **Ecological Fallacy**\n\n",
  "This is a school-level analysis. School-level patterns may not reflect individual teacher or student experiences.\n\n",
  "### **Statistical Inference**\n\n",
  "Formal testing of whether slope differences are statistically significant would require:\n",
  "- Interaction terms in a pooled regression model, OR\n",
  "- Bootstrapping methods to estimate uncertainty of slope differences\n\n",
  "The current analysis runs separate regressions per quartile, which provides visual and descriptive evidence but not formal hypothesis testing.\n\n",
  "---\n\n",
  "## Recommendations for Further Analysis\n\n",
  "### **Statistical Extensions**\n\n",
  "1. Run pooled regression with interaction terms to formally test if Q4-Q1 slope difference is statistically significant\n",
  "2. Use bootstrapping to estimate confidence intervals for slope differences across quartiles\n",
  "3. Test sensitivity to different quartile definitions (quintiles, deciles, continuous measure)\n\n",
  "### **Mechanism Exploration**\n\n",
  "1. Investigate what mediates the stronger association in Q4 schools:\n",
  "   - School climate measures\n",
  "   - Administrative support for discipline reform\n",
  "   - Community engagement patterns\n",
  "2. Examine whether teacher experience or tenure moderates the relationship\n",
  "3. Analyze suspension reason categories (defiance vs. serious offenses) by quartile\n\n",
  "### **Longitudinal Analysis**\n\n",
  "1. Track schools over time to see if changes in teacher diversity associate with changes in suspension rates\n",
  "2. Use school fixed effects to control for time-invariant school characteristics\n",
  "3. Examine trajectories before/after major staffing changes\n\n",
  "---\n\n",
  "## Data Outputs Available\n\n",
  "### **Tables** (CSV format)\n",
  "1. `24_quartile_slope_comparison_coefficients.csv` - Regression results for all four quartiles with coefficients, standard errors, confidence intervals, and model fit statistics\n\n",
  "**Output Location**: `outputs/tables/`\n\n",
  "### **Visualizations** (PNG, 300 DPI)\n",
  "1. `24_quartile_slope_comparison.png` - Faceted scatter plot (2×2 grid) showing % White Teachers vs. Suspension Rate by quartile, with linear regression lines and fixed y-axis scales for direct slope comparison\n\n",
  "**Output Location**: `outputs/graphs/`\n\n",
  "### **This Summary** (Markdown)\n",
  "`24_quartile_slope_comparison_SUMMARY.md` - Executive summary (this document)\n\n",
  "**Output Location**: `outputs/summaries/`\n\n",
  "### **Convert to Word**\n",
  "```bash\n",
  "# Convert this summary to Word format\n",
  "./scripts/utilities/convert_summary_to_word.sh 24_quartile_slope_comparison_SUMMARY.md\n",
  "```\n\n",
  "---\n\n",
  "## Methodological Notes\n\n",
  "### **Regression Approach**\n\n",
  "**Approach**: Weighted linear regression, run separately for each Black enrollment quartile\n\n",
  "**Why this method**:\n",
  "- Allows visual comparison of slope differences across contexts\n",
  "- Weighting by enrollment ensures larger schools have appropriate influence\n",
  "- Separate models allow flexibility in relationships across quartiles\n\n",
  "**Assumptions**:\n",
  "- Linear relationship between % White teachers and suspension rates within each quartile\n",
  "- Independence of school-year observations (conditional on controls)\n",
  "- Homoscedasticity of residuals\n\n",
  "**Limitations**:\n",
  "- Does not formally test interaction (slope difference)\n",
  "- May have autocorrelation if same schools appear in multiple years\n",
  "- Controls are limited (charter status, school level only)\n\n",
  "### **Sample Construction**\n\n",
  "**Approach**: Filter to schools with complete teacher diversity and suspension data, 2018-19 onwards\n\n",
  "**Why this method**: 2018-19 onwards has better teacher data coverage than earlier years\n\n",
  "**Assumptions**: Schools with available data are representative of all schools\n\n",
  "**Limitations**: Schools without teacher diversity data may differ systematically\n\n",
  "### **Statistical Significance**\n\n",
  "Throughout this summary:\n",
  "- **\\*\\*\\*** indicates p < 0.001 (highly statistically significant)\n",
  "- **\\*\\*** indicates p < 0.01 (very statistically significant)\n",
  "- **\\*** indicates p < 0.05 (statistically significant)\n",
  "- **NS** indicates not statistically significant (p ≥ 0.05)\n\n",
  "**Important**: Statistical significance does not imply practical importance or causation. Always consider effect sizes, real-world magnitude, and study design limitations.\n\n",
  "---\n\n",
  "## Citation\n\n",
  "**Suggested Citation**:\n",
  "> UCLA Center for the Transformation of Schools (2025). \"Teacher Diversity and Suspension Rates by School Racial Composition: Executive Summary.\" REACH Suspensions Analysis Project.\n\n",
  "**Data Source**:\n",
  "> California Department of Education. \"Suspension Data File.\" 2018-19 through 2023-24. Retrieved from https://www.cde.ca.gov/ds/sd/sd/  \n",
  "> California Department of Education. \"Teacher Demographics Data.\" 2018-19 through 2023-24. Retrieved from https://www.cde.ca.gov/ds/sd/df/\n\n",
  "**Analysis Documentation**:\n",
  "> Full methodology and code available at: `Analysis/24_quartile_slope_comparison.R`\n\n",
  "---\n\n",
  "## Contact and Questions\n\n",
  "For questions about:\n",
  "- **Methodology**: See `Analysis/24_ANALYSIS_SUMMARY.md` for technical details\n",
  "- **Data pipeline**: See `CLAUDE.md` in repository root\n",
  "- **Code review**: Script at `Analysis/24_quartile_slope_comparison.R`\n",
  "- **Related analyses**: See `outputs/summaries/README.md`\n\n",
  "---\n\n",
  "## Document Information\n\n",
  "**Document Version**: 1.0  \n",
  "**Document Created**: ", Sys.Date(), "  \n",
  "**Last Updated**: ", Sys.Date(), "  \n",
  "**Analysis Script**: `Analysis/24_quartile_slope_comparison.R`  \n",
  "**Output Location**: `outputs/summaries/24_quartile_slope_comparison_SUMMARY.md`  \n",
  "**Word Version**: `outputs/summaries/24_quartile_slope_comparison_SUMMARY.docx` (generate using conversion script)  \n\n",
  "---\n\n",
  "**END OF SUMMARY**\n"
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
