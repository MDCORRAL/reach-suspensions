# Analysis/27_power_analysis_multiscript.R
#
# Multi-analysis power diagnostics for key teacher diversity studies.
#
# This script mirrors the structure of Analysis/26_power_analysis.R but extends
# the workflow to the other flagship analyses:
#   - 21_teacher_diversity_regression.R / .md
#   - 22_black_suspension_rates_teacher_demographics.R
#   - 23_teacher_demographics_q4_black_enrollment.R
#   - 24_quartile_slope_comparison.R
#   - 25_interaction_term_regression.R
#
# Purpose:
#   1) Provide consistent, reproducible power calculations across the analyses.
#   2) Surface effective sample sizes after weighting for each analytic subset.
#   3) Estimate minimum detectable effect sizes (Cohen's f² and R² equivalents).
#   4) Adjust for multiple comparisons using Bonferroni thresholds per analysis.
#
# Inputs:
#   - data-stage/susp_v6_teacher_features.parquet (preferred)
#   - data-stage/susp_v6_teacher_long.parquet (fallback)
#
# Outputs:
#   - outputs/tables/27_power_analysis_by_group.csv (group-level power stats)
#   - outputs/tables/27_power_analysis_overview.csv (analysis-level summary)
#   - Console messages describing each analysis block
#
# Methodological alignment:
#   - Uses Kish effective sample size to reflect enrollment weighting
#   - Defaults to the predictor/control counts documented in the underlying
#     analysis scripts (see analysis_plan below)
#   - Uses pwr::pwr.f2.test to compute sensitivity (minimum detectable effects)
#     and achieved power for small/medium/large benchmarks
#   - Applies Bonferroni correction sized to the number of tests each analysis
#     typically reports (for conservative inference)

suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
  library(tidyr)
  library(pwr)
  library(readr)
})

try(here::i_am("Analysis/27_power_analysis_multiscript.R"), silent = TRUE)

# Load shared helpers
source(here::here("R", "utils_keys_filters.R"))

# === Helper functions ==========================================================
format_number <- function(x) {
  format(x, big.mark = ",", scientific = FALSE, trim = TRUE)
}

safe_div <- function(num, denom, replace_na_with = NA_real_) {
  ifelse(denom == 0 | is.na(denom), replace_na_with, num / denom)
}

canonicalize_race_label <- function(x) {
  raw <- toupper(trimws(x))

  out <- dplyr::recode(raw,
    # Two-letter codes
    "AA" = "Black/African American",
    "AS" = "Asian",
    "PI" = "Native Hawaiian/Pacific Islander",
    "NA" = "American Indian/Alaska Native",
    "WH" = "White",
    "HI" = "Hispanic/Latino",
    "FI" = "Filipino",
    "TR" = "Two or More Races",

    # Full labels (case-insensitive after toupper)
    "BLACK/AFRICAN AMERICAN" = "Black/African American",
    "BLACK OR AFRICAN AMERICAN" = "Black/African American",
    "ASIAN" = "Asian",
    "NATIVE HAWAIIAN/PACIFIC ISLANDER" = "Native Hawaiian/Pacific Islander",
    "PACIFIC ISLANDER" = "Native Hawaiian/Pacific Islander",
    "AMERICAN INDIAN/ALASKA NATIVE" = "American Indian/Alaska Native",
    "WHITE" = "White",
    "HISPANIC/LATINO" = "Hispanic/Latino",
    "HISPANIC OR LATINO" = "Hispanic/Latino",
    "LATINO" = "Hispanic/Latino",
    "FILIPINO" = "Filipino",
    "TWO OR MORE RACES" = "Two or More Races",

    # Exclusions / aggregate buckets
    "TA" = NA_character_,
    "RD" = NA_character_,
    "ALL STUDENTS" = NA_character_,
    .default = NA_character_
  )

  out
}

resolve_race_column <- function(df) {
  candidate_cols <- c("student_group", "subgroup", "race", "race_label")
  chosen <- intersect(candidate_cols, names(df))
  if (length(chosen) == 0) {
    stop("No race column found. Expected one of: ", paste(candidate_cols, collapse = ", "))
  }
  df %>% mutate(race_clean = canonicalize_race_label(.data[[chosen[1]]]))
}

resolve_black_quartile <- function(df) {
  quartile_cols <- c("black_prop_q", "black_prop_quartile", "black_quartile")
  existing <- intersect(quartile_cols, names(df))

  if (length(existing)) {
    df <- df %>% mutate(black_quartile = as.integer(.data[[existing[1]]]))
  } else if ("black_prop" %in% names(df)) {
    df <- df %>% mutate(black_quartile = ntile(black_prop, 4))
  } else {
    df <- df %>% mutate(black_quartile = NA_integer_)
  }

  df %>% mutate(black_quartile = ifelse(is.na(black_quartile), NA_integer_, black_quartile))
}

compute_effective_n <- function(weights) {
  sum_w <- sum(weights)
  sum_w2 <- sum(weights^2)
  if (sum_w == 0 || sum_w2 == 0) return(list(neff = 0, efficiency = 0))
  neff <- (sum_w^2) / sum_w2
  list(neff = neff, efficiency = neff / length(weights))
}

compute_power_block <- function(df, group_vars, analysis_id, predictors, controls,
                                bonferroni_tests = 1, alpha = 0.05) {
  results <- list()
  grouped <- df %>% drop_na(cumulative_enrollment) %>% filter(cumulative_enrollment > 0)

  if (!length(group_vars)) {
    grouped <- grouped %>% mutate(.group_label = "overall")
    grouping_syms <- ".group_label"
  } else {
    grouping_syms <- group_vars
  }

  grouped <- grouped %>% group_by(across(all_of(grouping_syms)))

  summary_df <- grouped %>% summarise(
    n_raw = n(),
    n_weighted = sum(cumulative_enrollment, na.rm = TRUE),
    weights = list(cumulative_enrollment),
    .groups = "drop"
  )

  for (i in seq_len(nrow(summary_df))) {
    row <- summary_df[i, ]
    weight_list <- row$weights[[1]]
    eff <- compute_effective_n(weight_list)

    residual_df <- eff$neff - predictors - controls - 1
    alpha_uncorrected <- alpha
    alpha_bonf <- alpha / max(1, bonferroni_tests)

    # Sensitivity and achieved power
    min_f2 <- tryCatch({
      pwr.f2.test(u = predictors, v = residual_df, sig.level = alpha_uncorrected, power = 0.80)$f2
    }, error = function(e) NA_real_)

    min_f2_bonf <- tryCatch({
      pwr.f2.test(u = predictors, v = residual_df, sig.level = alpha_bonf, power = 0.80)$f2
    }, error = function(e) NA_real_)

    power_small <- tryCatch({
      pwr.f2.test(u = predictors, v = residual_df, f2 = 0.02, sig.level = alpha_uncorrected)$power
    }, error = function(e) NA_real_)

    power_medium <- tryCatch({
      pwr.f2.test(u = predictors, v = residual_df, f2 = 0.13, sig.level = alpha_uncorrected)$power
    }, error = function(e) NA_real_)

    power_large <- tryCatch({
      pwr.f2.test(u = predictors, v = residual_df, f2 = 0.26, sig.level = alpha_uncorrected)$power
    }, error = function(e) NA_real_)

    power_small_b <- tryCatch({
      pwr.f2.test(u = predictors, v = residual_df, f2 = 0.02, sig.level = alpha_bonf)$power
    }, error = function(e) NA_real_)

    power_medium_b <- tryCatch({
      pwr.f2.test(u = predictors, v = residual_df, f2 = 0.13, sig.level = alpha_bonf)$power
    }, error = function(e) NA_real_)

    power_large_b <- tryCatch({
      pwr.f2.test(u = predictors, v = residual_df, f2 = 0.26, sig.level = alpha_bonf)$power
    }, error = function(e) NA_real_)

    group_label <- if (length(group_vars)) {
      paste(group_vars, row[group_vars], sep = "=", collapse = "; ")
    } else {
      "overall"
    }

    results[[length(results) + 1]] <- tibble(
      analysis_id = analysis_id,
      group_label = group_label,
      n_raw = row$n_raw,
      n_effective = eff$neff,
      efficiency = eff$efficiency,
      predictors = predictors,
      controls = controls,
      residual_df = residual_df,
      alpha_uncorrected = alpha_uncorrected,
      alpha_bonferroni = alpha_bonf,
      bonferroni_tests = bonferroni_tests,
      min_detectable_f2 = min_f2,
      min_detectable_r2 = min_f2 / (1 + min_f2),
      min_detectable_f2_bonf = min_f2_bonf,
      min_detectable_r2_bonf = min_f2_bonf / (1 + min_f2_bonf),
      power_small = power_small,
      power_medium = power_medium,
      power_large = power_large,
      power_small_bonf = power_small_b,
      power_medium_bonf = power_medium_b,
      power_large_bonf = power_large_b
    )
  }

  bind_rows(results)
}

# === Output directories =======================================================
message("\n════════════════════════════════════════════════════════════════")
message("=== 27: Multi-Analysis Power Diagnostics ===")
message("════════════════════════════════════════════════════════════════\n")

tables_dir <- here::here("outputs", "tables")
if (!dir.exists(tables_dir)) dir.create(tables_dir, recursive = TRUE)

# === Data load ===============================================================
MERGED_PATHS <- c(
  here::here("data-stage", "susp_v6_teacher_features.parquet"),
  here::here("data-stage", "susp_v6_teacher_long.parquet")
)

MERGED_PATH <- MERGED_PATHS[file.exists(MERGED_PATHS)][1]
if (is.na(MERGED_PATH)) {
  stop("No merged student-teacher dataset found. Expected one of: \n",
       paste(MERGED_PATHS, collapse = "\n"))
}

message(">>> Loading merged student-teacher data from: ", basename(MERGED_PATH))
ds <- arrow::open_dataset(MERGED_PATH)
all_cols <- names(ds)

needed_cols <- unique(c(
  "cds_school", "academic_year", "student_group", "subgroup",
  "total_suspensions", "cumulative_enrollment", "suspension_rate_percent_total",
  "black_prop", "black_prop_q", "black_prop_quartile", "black_quartile"
))

cols_to_use <- intersect(needed_cols, all_cols)
missing_cols <- setdiff(c("cds_school", "academic_year", "cumulative_enrollment"), cols_to_use)
if (length(missing_cols)) {
  stop("Missing required columns: ", paste(missing_cols, collapse = ", "))
}

message(">>> Selecting ", length(cols_to_use), " columns for power calculations")

raw_df <- ds %>%
  select(all_of(cols_to_use)) %>%
  filter(academic_year >= "2018-19") %>%
  collect() %>%
  janitor::clean_names()

message(">>> Loaded ", format_number(nrow(raw_df)), " rows × ", ncol(raw_df), " columns")

# Harmonise race labels and quartiles
analytic_df <- raw_df %>%
  resolve_race_column() %>%
  resolve_black_quartile() %>%
  filter(!is.na(race_clean))

# Fallback when cumulative_enrollment is missing
if (!"cumulative_enrollment" %in% names(analytic_df)) {
  analytic_df <- analytic_df %>% mutate(cumulative_enrollment = 1)
}

# === Analysis plan ===========================================================
analysis_plan <- list(
  list(
    analysis_id = "21_teacher_diversity_regression",
    description = "Student-race regressions with teacher/admin diversity",
    subset = function(df) df,
    group_vars = c("race_clean"),
    predictors = 2,          # teacher + admin diversity main effects
    controls = 6,            # SED, charter, grade level (4 df), intercept
    bonferroni_tests = 8
  ),
  list(
    analysis_id = "22_black_suspension_teacher_demographics",
    description = "Black suspension rates by enrollment quartile with teacher demographics",
    subset = function(df) df %>% filter(race_clean == "Black/African American"),
    group_vars = c("black_quartile"),
    predictors = 3,          # teacher, admin, quartile effect
    controls = 5,            # year + structural controls
    bonferroni_tests = 4     # four Black enrollment quartiles
  ),
  list(
    analysis_id = "23_teacher_demographics_q4_black_enrollment",
    description = "Teacher demographics within Q4 Black enrollment schools",
    subset = function(df) df %>% filter(black_quartile == 4),
    group_vars = c("race_clean"),
    predictors = 2,          # teacher/admin diversity main effects
    controls = 5,
    bonferroni_tests = 4
  ),
  list(
    analysis_id = "24_quartile_slope_comparison",
    description = "Slope differences across Black enrollment quartiles",
    subset = function(df) df,
    group_vars = c("black_quartile", "race_clean"),
    predictors = 4,          # quartile main + slope interactions
    controls = 5,
    bonferroni_tests = 16    # 4 quartiles × 4 focal slopes
  ),
  list(
    analysis_id = "25_interaction_term_regression",
    description = "Teacher diversity × quartile interaction regressions",
    subset = function(df) df,
    group_vars = c("race_clean"),
    predictors = 4,          # teacher, admin, quartile, interaction
    controls = 6,
    bonferroni_tests = 8
  )
)

# === Run power diagnostics ====================================================
all_results <- list()

for (plan in analysis_plan) {
  message("\n────────────────────────────────────────────────────────────────")
  message("📊 Analysis ", plan$analysis_id)
  message(plan$description)
  message("────────────────────────────────────────────────────────────────")

  df_subset <- plan$subset(analytic_df)

  if (nrow(df_subset) == 0) {
    warning("No rows available for ", plan$analysis_id, " after filtering")
    next
  }

  res <- compute_power_block(
    df = df_subset,
    group_vars = plan$group_vars,
    analysis_id = plan$analysis_id,
    predictors = plan$predictors,
    controls = plan$controls,
    bonferroni_tests = plan$bonferroni_tests
  )

  all_results[[length(all_results) + 1]] <- res
}

results_df <- bind_rows(all_results)
if (!nrow(results_df)) {
  stop("No power results were generated. Check filters and inputs.")
}

# Aggregate overview
overview_df <- results_df %>%
  group_by(analysis_id) %>%
  summarise(
    groups_evaluated = n(),
    min_effective_n = min(n_effective, na.rm = TRUE),
    median_effective_n = median(n_effective, na.rm = TRUE),
    min_detectable_r2_median = median(min_detectable_r2, na.rm = TRUE),
    min_detectable_r2_bonf_median = median(min_detectable_r2_bonf, na.rm = TRUE),
    .groups = "drop"
  )

# === Persist results =========================================================
readr::write_csv(results_df, file.path(tables_dir, "27_power_analysis_by_group.csv"))
readr::write_csv(overview_df, file.path(tables_dir, "27_power_analysis_overview.csv"))

message("\n>>> Saved group-level results to outputs/tables/27_power_analysis_by_group.csv")
message(">>> Saved overview to outputs/tables/27_power_analysis_overview.csv")
message(">>> Add these artifacts to the relevant analysis summaries once reviewed.")
