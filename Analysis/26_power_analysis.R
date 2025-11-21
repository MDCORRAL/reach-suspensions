# Analysis/26_power_analysis.R
#
# Power Analysis for Teacher Diversity Regressions
#
# This script conducts power analyses for the regression models in Analysis/21
# to determine:
# 1. What effect sizes we can reliably detect (sensitivity analysis)
# 2. Whether our sample sizes provide adequate power for meaningful effects
# 3. Effective sample sizes after weighting adjustments
#
# Input: susp_v6_teacher_features.parquet (merged student + teacher data)
# Output:
#   - tables/26_power_analysis_results.csv (power calculations)
#   - graphs/26_power_curves.png (visualization)
#
# Key methodological notes:
# - Uses pwr package for power calculations
# - Adjusts for weighted regression effective sample size
# - Accounts for multiple comparisons (8 racial groups)
# - Conservative approach: reports minimum detectable effect sizes

# === 1) Setup =================================================================
suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
  library(pwr)  # install.packages("pwr") if not available
  library(ggplot2)
  library(writexl)
})

try(here::i_am("Analysis/26_power_analysis.R"), silent = TRUE)

# Load canonical definitions
source(here::here("R", "utils_keys_filters.R"))

# Helper functions
format_number <- function(x) {
  format(x, big.mark = ",", scientific = FALSE, trim = TRUE)
}

safe_div <- function(num, denom, replace_na_with = NA_real_) {
  ifelse(denom == 0 | is.na(denom), replace_na_with, num / denom)
}

# === 2) Load data =============================================================
message("\n════════════════════════════════════════════════════════════════")
message("=== 26: Power Analysis for Teacher Diversity Regressions ===")
message("════════════════════════════════════════════════════════════════\n")

MERGED_PATH <- here::here("data-stage", "susp_v6_teacher_features.parquet")

if (!file.exists(MERGED_PATH)) {
  stop("\nMissing file: ", MERGED_PATH,
       "\nRun Analysis/18_merge_teacher_student.R first.")
}

message(">>> Loading merged teacher-student data...")
df_raw <- arrow::read_parquet(MERGED_PATH)
message(">>> Loaded ", format_number(nrow(df_raw)), " rows")

# === 3) Canonicalize race labels =============================================
canonicalize_race_label <- function(x) {
  labels <- rep(NA_character_, length(x))
  clean <- tolower(trimws(as.character(x)))

  labels[clean %in% c("ra", "asian")] <- "Asian"
  labels[clean %in% c("rb", "black", "african american", "black/african american")] <- "Black/African American"
  labels[clean %in% c("rf", "filipino")] <- "Filipino"
  labels[clean %in% c("rh", "rl", "hispanic", "latino", "hispanic/latino")] <- "Hispanic/Latino"
  labels[clean %in% c("ri", "american indian", "alaska native", "american indian/alaska native")] <- "American Indian/Alaska Native"
  labels[clean %in% c("rp", "pacific islander", "native hawaiian", "native hawaiian/pacific islander")] <- "Native Hawaiian/Pacific Islander"
  labels[clean %in% c("rt", "two or more", "two or more races")] <- "Two or More Races"
  labels[clean %in% c("rw", "white")] <- "White"

  labels
}

# Check for student_group column
if ("student_group" %in% names(df_raw)) {
  df_raw$student_group <- canonicalize_race_label(df_raw$student_group)
} else if ("reporting_category" %in% names(df_raw)) {
  df_raw$student_group <- canonicalize_race_label(df_raw$reporting_category)
} else {
  stop("No student_group or reporting_category column found")
}

# Filter to recent years for efficiency (and better teacher data coverage)
message("\n>>> Filtering to recent years (2018-19 onwards)...")
message("    Rows before filter: ", format_number(nrow(df_raw)))

df_raw <- df_raw %>%
  filter(academic_year >= "2018-19")

message("    Rows after filter: ", format_number(nrow(df_raw)))
message("    Data reduction: ", sprintf("%.1f%%", 100 * (1 - nrow(df_raw) / 3402282)))

# === 4) Aggregate to school-year-race level ==================================
message("\n>>> Aggregating to school-year-race level...")

aggregate_to_school_year_race <- function(df) {
  message(">>> Starting aggregation...")
  message("    Input rows: ", format_number(nrow(df)))

  group_vars <- c("cds_school", "academic_year", "student_group")

  suspension_cols <- grep("^total_suspensions$", names(df), value = TRUE)
  message("    Suspension columns: ", length(suspension_cols))

  # CRITICAL: Only select SPECIFIC teacher diversity columns we actually need
  # This prevents processing hundreds of unnecessary columns
  teacher_race_pattern <- paste0("^teacher_staff_count_(",
                                 "african_american|asian|filipino|hispanic_or_latino|",
                                 "american_indian_or_alaska_native|",
                                 "native_hawaiian_pacific_islander|pacific_islander|",
                                 "white|two_or_more_races|not_reported)_share$")

  admin_race_pattern <- paste0("^teacher_staff_count_by_type_administrators_(",
                               "african_american|asian|filipino|hispanic_or_latino|",
                               "american_indian_or_alaska_native|",
                               "native_hawaiian_pacific_islander|pacific_islander|",
                               "white|two_or_more_races|not_reported)_share$")

  teacher_cols <- grep(teacher_race_pattern, names(df), value = TRUE, ignore.case = TRUE)
  admin_cols <- grep(admin_race_pattern, names(df), value = TRUE, ignore.case = TRUE)

  message("    Teacher race share columns: ", length(teacher_cols))
  message("    Admin race share columns: ", length(admin_cols))

  enrollment_cols <- intersect(c("cumulative_enrollment", "sup_cumulative_enrollment"), names(df))
  charter_cols <- intersect(c("charter_yn", "charter_yn_std", "is_traditional"), names(df))
  level_cols <- intersect(c("level_strict3", "school_level_final", "school_level"), names(df))
  sed_cols <- intersect(c("sed_rate", "socioeconomically_disadvantaged_rate"), names(df))

  constant_cols <- unique(c(enrollment_cols, teacher_cols, admin_cols,
                           charter_cols, level_cols, sed_cols))
  constant_cols <- intersect(constant_cols, names(df))

  message("    Total columns to preserve: ", length(constant_cols))
  message("    Aggregating...")

  agg_df <- df %>%
    group_by(across(all_of(group_vars))) %>%
    summarise(
      across(any_of(suspension_cols), ~sum(.x, na.rm = TRUE)),
      across(any_of(constant_cols), ~first(.x)),
      n_reasons_aggregated = n(),
      .groups = "drop"
    )

  message("    Recalculating suspension rates...")

  # Recalculate suspension rate
  if ("total_suspensions" %in% names(agg_df) && "cumulative_enrollment" %in% names(agg_df)) {
    agg_df <- agg_df %>%
      mutate(suspension_rate_percent_total = safe_div(total_suspensions,
                                                       cumulative_enrollment) * 100)
  }

  message(">>> Aggregated to ", format_number(nrow(agg_df)), " school-year-race observations")
  return(agg_df)
}

df <- aggregate_to_school_year_race(df_raw)

# === 5) Extract teacher diversity measures ===================================

TEACHER_RACE_SLUGS <- c(
  "african_american", "asian", "filipino", "hispanic_or_latino",
  "american_indian_or_alaska_native", "native_hawaiian_pacific_islander",
  "pacific_islander", "white", "two_or_more_races", "not_reported"
)

extract_teacher_race_nonwhite_share <- function(df) {
  race_share_pattern <- paste0("^teacher.*_(",
                               paste(TEACHER_RACE_SLUGS, collapse = "|"),
                               ")_share$")
  race_share_cols <- grep(race_share_pattern, names(df), value = TRUE, ignore.case = TRUE)

  if (!length(race_share_cols)) return(NULL)

  white_cols <- grep("_white_share$", race_share_cols, value = TRUE, ignore.case = TRUE)
  white_cols <- white_cols[!grepl("non_white", white_cols, ignore.case = TRUE)]
  not_reported_cols <- grep("_(not_reported|unknown)_share$", race_share_cols,
                           value = TRUE, ignore.case = TRUE)
  non_white_cols <- setdiff(race_share_cols, c(white_cols, not_reported_cols))

  if (length(non_white_cols) > 0) {
    mat <- sapply(non_white_cols, function(col) as.numeric(df[[col]]))
    if (!is.matrix(mat)) mat <- matrix(mat, ncol = 1)
    values <- rowSums(mat, na.rm = TRUE)
    all_missing <- apply(is.na(mat), 1, all)
    values[all_missing] <- NA_real_
    return(values)
  }

  return(NULL)
}

extract_admin_race_nonwhite_share <- function(df) {
  admin_pattern <- paste0("^teacher.*by_type_administrators.*_(",
                         paste(TEACHER_RACE_SLUGS, collapse = "|"),
                         ")_share$")
  admin_race_cols <- grep(admin_pattern, names(df), value = TRUE, ignore.case = TRUE)

  if (!length(admin_race_cols)) return(NULL)

  white_cols <- grep("_white_share$", admin_race_cols, value = TRUE, ignore.case = TRUE)
  white_cols <- white_cols[!grepl("non_white", white_cols, ignore.case = TRUE)]
  not_reported_cols <- grep("_(not_reported|unknown)_share$", admin_race_cols,
                           value = TRUE, ignore.case = TRUE)
  non_white_cols <- setdiff(admin_race_cols, c(white_cols, not_reported_cols))

  if (length(non_white_cols) > 0) {
    mat <- sapply(non_white_cols, function(col) as.numeric(df[[col]]))
    if (!is.matrix(mat)) mat <- matrix(mat, ncol = 1)
    values <- rowSums(mat, na.rm = TRUE)
    all_missing <- apply(is.na(mat), 1, all)
    values[all_missing] <- NA_real_
    return(values)
  }

  return(NULL)
}

# === 6) Calculate sample sizes and effective N for each group ================
message("\n>>> Calculating sample sizes by racial/ethnic group...")

ALLOWED_RACE_GROUPS <- c(
  "Black/African American", "White", "Hispanic/Latino",
  "American Indian/Alaska Native", "Asian", "Filipino",
  "Native Hawaiian/Pacific Islander", "Two or More Races"
)

power_analysis_results <- list()

for (group in ALLOWED_RACE_GROUPS) {
  message("\n────────────────────────────────────────────────────────────────")
  message("📊 Analyzing: ", group)
  message("────────────────────────────────────────────────────────────────")

  # Filter to this group
  group_df <- df %>%
    filter(student_group == group, !is.na(student_group))

  if (nrow(group_df) == 0) {
    message("⚠ Skipping: No data for this group")
    next
  }

  # Extract diversity and outcome measures
  teacher_nonwhite <- extract_teacher_race_nonwhite_share(group_df)
  admin_nonwhite <- extract_admin_race_nonwhite_share(group_df)

  outcome_col <- "suspension_rate_percent_total"
  suspension_rate <- suppressWarnings(as.numeric(group_df[[outcome_col]]) / 100)
  enrollment <- suppressWarnings(as.numeric(group_df$cumulative_enrollment))

  # Filter to complete cases (matching Analysis/21)
  keep <- !is.na(suspension_rate) &
    !is.na(teacher_nonwhite) &
    !is.na(admin_nonwhite) &
    !is.na(enrollment) &
    enrollment > 0

  n_complete <- sum(keep)

  if (n_complete < 10) {
    message("⚠ Skipping: Insufficient complete cases (N = ", n_complete, ")")
    next
  }

  # Calculate effective sample size for weighted regression
  # Effective N = (sum of weights)^2 / sum of weights^2
  weights <- enrollment[keep]
  n_effective <- (sum(weights)^2) / sum(weights^2)

  message(">>> Sample size (unweighted): ", format_number(n_complete))
  message(">>> Effective sample size (weighted): ", format_number(round(n_effective)))
  message(">>> Weight efficiency: ", sprintf("%.2f%%", 100 * n_effective / n_complete))

  # === 7) Power analysis for multiple regression =============================
  # For multiple regression with 2 predictors of interest (teacher, admin)
  # plus controls (sed_rate, is_charter, school_level), we have:
  # - u = # of predictors tested (2: teacher + admin diversity)
  # - v = # of other predictors in model (varies, typically 3-4 controls)

  # Conservative assumption: 4 total controls (sed, charter, + 2 levels dummies)
  u <- 2  # Teacher and admin diversity (predictors of interest)
  v <- 4  # Controls

  # Standard power levels to test
  alpha <- 0.05  # Significance level
  power_target <- 0.80  # Conventional target

  # Calculate minimum detectable effect size (Cohen's f²) given our sample size
  # Using effective N for weighted regression

  # Sensitivity analysis: What effect can we detect with 80% power?
  min_f2 <- tryCatch({
    pwr.f2.test(u = u, v = n_effective - u - v - 1,
                sig.level = alpha, power = power_target)$f2
  }, error = function(e) NA_real_)

  # Convert Cohen's f² to R²
  # f² = R² / (1 - R²)
  # So: R² = f² / (1 + f²)
  min_r2 <- if (!is.na(min_f2)) min_f2 / (1 + min_f2) else NA_real_

  # Calculate power for small, medium, and large effects (Cohen's conventions)
  # Small: f² = 0.02, Medium: f² = 0.15, Large: f² = 0.35

  power_small <- tryCatch({
    pwr.f2.test(u = u, v = n_effective - u - v - 1,
                sig.level = alpha, f2 = 0.02)$power
  }, error = function(e) NA_real_)

  power_medium <- tryCatch({
    pwr.f2.test(u = u, v = n_effective - u - v - 1,
                sig.level = alpha, f2 = 0.15)$power
  }, error = function(e) NA_real_)

  power_large <- tryCatch({
    pwr.f2.test(u = u, v = n_effective - u - v - 1,
                sig.level = alpha, f2 = 0.35)$power
  }, error = function(e) NA_real_)

  message("\n>>> Power Analysis Results:")
  message("    Minimum detectable f² (80% power): ", sprintf("%.4f", min_f2))
  message("    Minimum detectable R²: ", sprintf("%.4f", min_r2))
  message("    Power for small effect (f² = 0.02): ", sprintf("%.2f%%", power_small * 100))
  message("    Power for medium effect (f² = 0.15): ", sprintf("%.2f%%", power_medium * 100))
  message("    Power for large effect (f² = 0.35): ", sprintf("%.2f%%", power_large * 100))

  # === 8) Bonferroni adjustment for multiple comparisons =====================
  # We're testing 8 racial groups → adjust alpha for family-wise error rate
  n_comparisons <- length(ALLOWED_RACE_GROUPS)
  alpha_bonferroni <- alpha / n_comparisons

  # Recalculate with adjusted alpha
  min_f2_bonf <- tryCatch({
    pwr.f2.test(u = u, v = n_effective - u - v - 1,
                sig.level = alpha_bonferroni, power = power_target)$f2
  }, error = function(e) NA_real_)

  min_r2_bonf <- if (!is.na(min_f2_bonf)) min_f2_bonf / (1 + min_f2_bonf) else NA_real_

  message("\n>>> With Bonferroni Correction (α = ", sprintf("%.4f", alpha_bonferroni), "):")
  message("    Minimum detectable f² (80% power): ", sprintf("%.4f", min_f2_bonf))
  message("    Minimum detectable R²: ", sprintf("%.4f", min_r2_bonf))

  # Store results
  power_analysis_results[[group]] <- data.frame(
    student_group = group,
    n_complete_cases = n_complete,
    n_effective = round(n_effective),
    weight_efficiency_pct = round(100 * n_effective / n_complete, 1),

    # Uncorrected
    min_f2_80power = min_f2,
    min_r2_80power = min_r2,
    power_small_effect = power_small,
    power_medium_effect = power_medium,
    power_large_effect = power_large,

    # Bonferroni corrected
    alpha_bonferroni = alpha_bonferroni,
    min_f2_80power_bonf = min_f2_bonf,
    min_r2_80power_bonf = min_r2_bonf,

    stringsAsFactors = FALSE
  )
}

# === 9) Compile results =======================================================
message("\n════════════════════════════════════════════════════════════════")
message("📊 COMPILING POWER ANALYSIS RESULTS")
message("════════════════════════════════════════════════════════════════\n")

results_df <- bind_rows(power_analysis_results)

if (nrow(results_df) == 0) {
  stop("No power analysis results generated. Check data availability.")
}

# Add interpretation flags
results_df <- results_df %>%
  mutate(
    adequate_power_medium = power_medium_effect >= 0.80,
    adequate_power_small = power_small_effect >= 0.80,
    interpretation = case_when(
      power_medium_effect >= 0.80 ~ "Adequate power for medium effects",
      power_small_effect >= 0.80 ~ "Adequate power for small effects",
      power_small_effect < 0.80 ~ "Underpowered for small effects",
      TRUE ~ "Needs review"
    )
  )

# Print summary table
message("Summary of Power Analysis Results:")
message("────────────────────────────────────────────────────────────────\n")
print(results_df %>%
        select(student_group, n_effective, min_f2_80power,
               power_medium_effect, interpretation))

# === 10) Create visualization =================================================
message("\n>>> Creating power curve visualization...")

# Generate power curves for each group
power_curves <- list()

for (i in 1:nrow(results_df)) {
  row <- results_df[i, ]
  group <- row$student_group
  n_eff <- row$n_effective

  # Generate sequence of effect sizes
  f2_seq <- seq(0.001, 0.50, by = 0.001)

  # Calculate power for each effect size
  powers <- sapply(f2_seq, function(f2) {
    tryCatch({
      pwr.f2.test(u = 2, v = n_eff - 2 - 4 - 1,
                  sig.level = 0.05, f2 = f2)$power
    }, error = function(e) NA_real_)
  })

  power_curves[[i]] <- data.frame(
    student_group = group,
    f2 = f2_seq,
    power = powers,
    stringsAsFactors = FALSE
  )
}

power_curves_df <- bind_rows(power_curves)

# Create plot
p <- ggplot(power_curves_df, aes(x = f2, y = power, color = student_group)) +
  geom_line(linewidth = 1) +

  # Add reference lines
  geom_hline(yintercept = 0.80, linetype = "dashed", color = "gray40", linewidth = 0.5) +
  geom_vline(xintercept = c(0.02, 0.15, 0.35), linetype = "dotted",
             color = "gray60", linewidth = 0.5) +

  # Add annotations for effect size benchmarks
  annotate("text", x = 0.02, y = 0.05, label = "Small\n(f²=0.02)",
           size = 3, color = "gray40", hjust = 0) +
  annotate("text", x = 0.15, y = 0.05, label = "Medium\n(f²=0.15)",
           size = 3, color = "gray40", hjust = 0) +
  annotate("text", x = 0.35, y = 0.05, label = "Large\n(f²=0.35)",
           size = 3, color = "gray40", hjust = 0) +
  annotate("text", x = 0.45, y = 0.82, label = "80% Power",
           size = 3, color = "gray40") +

  # Scales
  scale_x_continuous(
    limits = c(0, 0.50),
    breaks = seq(0, 0.50, 0.10)
  ) +
  scale_y_continuous(
    limits = c(0, 1),
    breaks = seq(0, 1, 0.20),
    labels = scales::percent_format()
  ) +

  # Labels
  labs(
    title = "Statistical Power Curves by Student Racial/Ethnic Group",
    subtitle = "Multiple regression with 2 predictors of interest + 4 controls (α = 0.05)",
    x = "Effect Size (Cohen's f²)",
    y = "Statistical Power",
    color = "Student Group",
    caption = paste0(
      "Note: Power curves show the probability of detecting an effect of a given size.\n",
      "Dashed line = 80% power threshold (conventional target).\n",
      "Dotted lines = Small (f²=0.02), Medium (f²=0.15), and Large (f²=0.35) effect sizes (Cohen 1988).\n",
      "Sample sizes are effective N accounting for enrollment weighting."
    )
  ) +

  # Theme
  theme_minimal(base_size = 11) +
  theme(
    plot.title = element_text(face = "bold", size = 14),
    plot.subtitle = element_text(size = 11, color = "gray30"),
    plot.caption = element_text(hjust = 0, size = 8, color = "gray40", lineheight = 1.2),
    legend.position = "right",
    legend.title = element_text(face = "bold"),
    panel.grid.minor = element_blank(),
    panel.border = element_rect(color = "gray80", fill = NA)
  )

# === 11) Save outputs =========================================================
message("\n>>> Saving outputs...")

dir.create(here::here("outputs", "tables"), showWarnings = FALSE, recursive = TRUE)
dir.create(here::here("outputs", "graphs"), showWarnings = FALSE, recursive = TRUE)

# Save results table (CSV)
write.csv(
  results_df,
  here::here("outputs", "tables", "26_power_analysis_results.csv"),
  row.names = FALSE
)
message("✓ Saved: outputs/tables/26_power_analysis_results.csv")

# Save Excel version with additional sheets
write_xlsx(
  list(
    "Summary" = results_df,
    "Power_Curves" = power_curves_df,
    "Interpretation_Guide" = data.frame(
      Metric = c(
        "f² (Cohen's f-squared)",
        "R² (R-squared)",
        "Small effect",
        "Medium effect",
        "Large effect",
        "Adequate power",
        "Effective N",
        "Bonferroni correction"
      ),
      Description = c(
        "Effect size for multiple regression. f² = R²/(1-R²)",
        "Proportion of variance explained by predictors of interest",
        "f² = 0.02 (small but meaningful association)",
        "f² = 0.15 (moderate association)",
        "f² = 0.35 (strong association)",
        "Power ≥ 0.80 (80% chance of detecting true effect)",
        "Sample size adjusted for unequal weighting (enrollment weights)",
        "Adjusted significance level (α/8) for 8 simultaneous tests"
      )
    )
  ),
  path = here::here("outputs", "tables", "26_power_analysis_results.xlsx")
)
message("✓ Saved: outputs/tables/26_power_analysis_results.xlsx")

# Save plot
ggsave(
  here::here("outputs", "graphs", "26_power_curves.png"),
  p, width = 12, height = 8, dpi = 300, bg = "white"
)
message("✓ Saved: outputs/graphs/26_power_curves.png")

# === 12) Final interpretation =================================================
message("\n════════════════════════════════════════════════════════════════")
message("✓ POWER ANALYSIS COMPLETE")
message("════════════════════════════════════════════════════════════════\n")

message("Key Findings:")
message("────────────────────────────────────────────────────────────────\n")

# Find groups with inadequate power
underpowered <- results_df %>%
  filter(power_medium_effect < 0.80)

if (nrow(underpowered) > 0) {
  message("⚠ Groups with INADEQUATE power for medium effects:")
  for (i in 1:nrow(underpowered)) {
    message("  • ", underpowered$student_group[i],
            " (N_eff = ", format_number(underpowered$n_effective[i]),
            ", power = ", sprintf("%.1f%%", underpowered$power_medium_effect[i] * 100), ")")
  }
  message("")
}

adequately_powered <- results_df %>%
  filter(power_medium_effect >= 0.80)

if (nrow(adequately_powered) > 0) {
  message("✓ Groups with ADEQUATE power for medium effects:")
  for (i in 1:nrow(adequately_powered)) {
    message("  • ", adequately_powered$student_group[i],
            " (N_eff = ", format_number(adequately_powered$n_effective[i]),
            ", power = ", sprintf("%.1f%%", adequately_powered$power_medium_effect[i] * 100), ")")
  }
  message("")
}

message("\nInterpretation Guidance:")
message("────────────────────────────────────────────────────────────────")
message("1. Minimum Detectable Effect:")
message("   - This is the SMALLEST effect you can reliably detect with 80% power")
message("   - If true effects are smaller, you'll likely get non-significant results")
message("   - Use this to interpret null findings (absence of evidence ≠ evidence of absence)")
message("")
message("2. Cohen's Benchmarks (f² / R²):")
message("   - Small: f² = 0.02 (R² ≈ 0.02)")
message("   - Medium: f² = 0.15 (R² ≈ 0.13)")
message("   - Large: f² = 0.35 (R² ≈ 0.26)")
message("")
message("3. Multiple Comparisons:")
message("   - You're testing 8 racial groups simultaneously")
message("   - Bonferroni correction: α = 0.05/8 = 0.00625")
message("   - This increases minimum detectable effects (more conservative)")
message("")
message("4. Practical Significance:")
message("   - Statistical power addresses DETECTION, not IMPORTANCE")
message("   - Even if powered to detect small effects, focus on MEANINGFUL effects")
message("   - Report effect sizes alongside p-values")
message("")

message("\nRecommendations:")
message("────────────────────────────────────────────────────────────────")
message("1. Report minimum detectable effects for all analyses")
message("2. Interpret non-significant results with caution (may be underpowered)")
message("3. Consider pooling small groups or using Bayesian methods")
message("4. Focus discussion on effect sizes, not just p-values")
message("5. For underpowered groups, report as exploratory analyses")
message("")

message("Output files:")
message("  - outputs/tables/26_power_analysis_results.csv")
message("  - outputs/tables/26_power_analysis_results.xlsx")
message("  - outputs/graphs/26_power_curves.png\n")

invisible(list(
  power_results = results_df,
  power_curves = power_curves_df,
  plot = p
))
