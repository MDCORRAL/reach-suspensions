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
#   - tables/26_power_analysis_diagnostics.csv (data quality checks)
#   - graphs/26_power_curves.png (visualization)
#
# Key methodological notes:
# - Uses pwr package for power calculations
# - Adjusts for weighted regression effective sample size
# - Accounts for multiple comparisons (8 racial groups)
# - Conservative approach: reports minimum detectable effect sizes
# - CRITICAL FIX (v2.0): u=2, v=6 (not v=4) to match Analysis 21 exactly
#
# Version History:
# - v2.0 (2025-11-21): Comprehensive fix addressing all review concerns
#   * Fixed v=6 (sed_rate + is_charter + grade_level[4df])
#   * Added diagnostics for unmapped labels, dropped records, missingness
#   * Added within-group variability checks
#   * Tightened regex patterns for exact column matching
#   * Added defensive directory creation
# - v1.0 (2025-11-20): Initial implementation

# === 1) Setup =================================================================
suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
  library(pwr)  # install.packages("pwr") if not available
  library(ggplot2)
  library(writexl)
  library(tidyr)
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

# === 2) Output directory setup (DEFENSIVE) ===================================
message("\n════════════════════════════════════════════════════════════════")
message("=== 26: Power Analysis for Teacher Diversity Regressions ===")
message("════════════════════════════════════════════════════════════════\n")

# Ensure output directories exist
output_tables <- here::here("outputs", "tables")
output_graphs <- here::here("outputs", "graphs")

if (!dir.exists(output_tables)) {
  dir.create(output_tables, recursive = TRUE, showWarnings = FALSE)
  message(">>> Created directory: ", output_tables)
}

if (!dir.exists(output_graphs)) {
  dir.create(output_graphs, recursive = TRUE, showWarnings = FALSE)
  message(">>> Created directory: ", output_graphs)
}

# === 3) Load data =============================================================
MERGED_PATH <- here::here("data-stage", "susp_v6_teacher_features.parquet")

if (!file.exists(MERGED_PATH)) {
  stop("\nMissing file: ", MERGED_PATH,
       "\nRun Analysis/18_merge_teacher_student.R first.")
}

message(">>> Loading merged teacher-student data (MEMORY-EFFICIENT MODE)...")
message("    Step 1: Opening parquet file (not loading yet)...")

# Open dataset without loading into memory
ds <- arrow::open_dataset(MERGED_PATH)
all_cols <- names(ds)
message("    Available columns: ", length(all_cols))

# Check for Arrow metadata warnings - investigate if present
arrow_version <- tryCatch(
  as.character(packageVersion("arrow")),
  error = function(e) "unknown"
)
message("    Arrow package version: ", arrow_version)

# Define minimal columns needed (prevents loading all 377 columns)
required_base_cols <- c(
  "cds_school", "academic_year", "student_group", "reporting_category",
  "total_suspensions", "cumulative_enrollment", "suspension_rate_percent_total",
  "sed_rate", "charter_yn", "charter_yn_std", "is_traditional",
  "level_strict3", "school_level_final", "school_level"
)

# TIGHTENED REGEX: Exact column name matching for race shares
# This prevents accidentally matching unexpected columns
teacher_race_slugs <- c(
  "african_american", "asian", "filipino", "hispanic_or_latino",
  "american_indian_or_alaska_native", "native_hawaiian_pacific_islander",
  "pacific_islander", "white", "two_or_more_races", "not_reported"
)

teacher_pattern <- paste0(
  "^teacher_staff_count_(",
  paste(teacher_race_slugs, collapse = "|"),
  ")_share$"
)

admin_pattern <- paste0(
  "^teacher_staff_count_by_type_administrators_(",
  paste(teacher_race_slugs, collapse = "|"),
  ")_share$"
)

teacher_cols <- grep(teacher_pattern, all_cols, value = TRUE, ignore.case = TRUE)
admin_cols <- grep(admin_pattern, all_cols, value = TRUE, ignore.case = TRUE)

message("    Found ", length(teacher_cols), " teacher race share columns")
message("    Found ", length(admin_cols), " admin race share columns")

cols_to_load <- unique(c(required_base_cols, teacher_cols, admin_cols))
cols_to_load <- intersect(cols_to_load, all_cols)

missing_required <- setdiff(required_base_cols, cols_to_load)
if (length(missing_required) > 0) {
  warning("Missing required columns: ", paste(missing_required, collapse = ", "))
}

message("    Step 2: Selecting ", length(cols_to_load), " columns (",
        sprintf("%.0f%%", 100 * length(cols_to_load) / length(all_cols)), " of total)")
message("    Step 3: Filtering to academic_year >= '2018-19' ON DISK...")
message("    Step 4: Loading into memory...")

# Filter and select ON DISK, then load
df_raw <- ds %>%
  filter(academic_year >= "2018-19") %>%
  select(all_of(cols_to_load)) %>%
  collect()

message(">>> Loaded ", format_number(nrow(df_raw)), " rows × ", ncol(df_raw), " columns")
message("    Memory: ~", sprintf("%.1f MB", object.size(df_raw) / 1024^2))

initial_rows <- nrow(df_raw)

# === 4) Canonicalize race labels with DIAGNOSTICS ============================
message("\n>>> Canonicalizing race labels...")

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
  # Note: RD (Not Reported) and TA (Total/All) return NA and are filtered out

  labels
}

# Try student_group first, fall back to reporting_category
if ("student_group" %in% names(df_raw)) {
  df_raw$race_clean <- canonicalize_race_label(df_raw$student_group)
  source_col <- "student_group"
} else if ("reporting_category" %in% names(df_raw)) {
  df_raw$race_clean <- canonicalize_race_label(df_raw$reporting_category)
  source_col <- "reporting_category"
} else {
  stop("No student race column found (student_group or reporting_category)")
}

# DIAGNOSTIC: Report unmapped labels
unmapped_count <- sum(is.na(df_raw$race_clean))
unmapped_pct <- 100 * unmapped_count / nrow(df_raw)

message(">>> Race label mapping from '", source_col, "':")
message("    Successfully mapped: ", format_number(nrow(df_raw) - unmapped_count),
        " rows (", sprintf("%.1f%%", 100 - unmapped_pct), ")")
message("    Unmapped (will be dropped): ", format_number(unmapped_count),
        " rows (", sprintf("%.1f%%", unmapped_pct), ")")

if (unmapped_count > 0) {
  # Show what labels couldn't be mapped
  unmapped_labels <- df_raw %>%
    filter(is.na(race_clean)) %>%
    group_by(!!sym(source_col)) %>%
    summarise(n = n(), .groups = "drop") %>%
    arrange(desc(n))

  message("\n    Unmapped label breakdown:")
  for (i in 1:min(5, nrow(unmapped_labels))) {
    message("      '", unmapped_labels[[source_col]][i], "': ",
            format_number(unmapped_labels$n[i]), " rows")
  }
  if (nrow(unmapped_labels) > 5) {
    message("      ... and ", nrow(unmapped_labels) - 5, " more")
  }
}

# Filter to valid races
df_raw <- df_raw %>% filter(!is.na(race_clean))
message("\n>>> After filtering to valid races: ", format_number(nrow(df_raw)), " rows")

# === 5) Extract diversity measures with PARTIAL MISSINGNESS CHECK ============
message("\n>>> Extracting diversity measures...")

# Helper to check for partial missingness
extract_nonwhite_share <- function(df, race_cols, label) {
  if (length(race_cols) == 0) {
    stop("No ", label, " race share columns found!")
  }

  # Separate white and non-white columns
  white_cols <- grep("_white_share$", race_cols, value = TRUE, ignore.case = TRUE)
  white_cols <- white_cols[!grepl("non_white", white_cols, ignore.case = TRUE)]

  not_reported_cols <- grep("_(not_reported|unknown)_share$", race_cols,
                            value = TRUE, ignore.case = TRUE)

  non_white_cols <- setdiff(race_cols, c(white_cols, not_reported_cols))

  message("    ", label, ":")
  message("      Non-white race columns: ", length(non_white_cols))
  message("      White columns: ", length(white_cols))
  message("      Not reported columns: ", length(not_reported_cols))

  # Check for partial missingness
  if (length(non_white_cols) > 0) {
    mat <- sapply(non_white_cols, function(col) as.numeric(df[[col]]))
    if (!is.matrix(mat)) mat <- matrix(mat, ncol = 1)

    # Count rows with partial missingness (some but not all missing)
    na_counts <- rowSums(is.na(mat))
    all_missing <- (na_counts == ncol(mat))
    some_missing <- (na_counts > 0) & (na_counts < ncol(mat))

    partial_missing_count <- sum(some_missing)
    if (partial_missing_count > 0) {
      message("      ⚠ WARNING: ", format_number(partial_missing_count),
              " rows have partial missingness (some but not all race shares missing)")
      message("         These will be summed with na.rm=TRUE, which may underestimate totals")
    }

    # Sum non-white shares
    values <- rowSums(mat, na.rm = TRUE)
    values[all_missing] <- NA_real_

    return(values)
  } else {
    stop("No non-white race share columns found for ", label)
  }
}

df_raw$teacher_nonwhite_share <- extract_nonwhite_share(
  df_raw, teacher_cols, "Teacher"
)
df_raw$admin_nonwhite_share <- extract_nonwhite_share(
  df_raw, admin_cols, "Administrator"
)

# === 6) Aggregate to school-year-race with VARIABILITY CHECK =================
message("\n>>> Aggregating to school-year-race level...")
message("    Initial rows: ", format_number(nrow(df_raw)))

# Identify covariates that should be constant within school-year-race
constant_check_vars <- c(
  "sed_rate", "charter_yn_std", "is_traditional",
  "level_strict3", "school_level_final", "cumulative_enrollment",
  "teacher_nonwhite_share", "admin_nonwhite_share"
)
constant_check_vars <- intersect(constant_check_vars, names(df_raw))

# CHECK: Verify covariates don't vary within school-year-race groups
message("    Checking within-group variability of covariates...")

variability_issues <- df_raw %>%
  group_by(cds_school, academic_year, race_clean) %>%
  summarise(
    across(
      any_of(constant_check_vars),
      list(
        n_distinct = ~n_distinct(.x, na.rm = TRUE),
        has_variation = ~(n_distinct(.x, na.rm = TRUE) > 1)
      )
    ),
    n_obs = n(),
    .groups = "drop"
  )

# Report any variables with within-group variation
for (var in constant_check_vars) {
  has_var_col <- paste0(var, "_has_variation")
  if (has_var_col %in% names(variability_issues)) {
    n_vary <- sum(variability_issues[[has_var_col]], na.rm = TRUE)
    if (n_vary > 0) {
      pct_vary <- 100 * n_vary / nrow(variability_issues)
      message("      ⚠ '", var, "' varies within group for ", format_number(n_vary),
              " school-year-race combinations (", sprintf("%.2f%%", pct_vary), ")")
      message("         Using first() may not be appropriate - consider weighted aggregation")
    }
  }
}

# Aggregate
agg_df <- df_raw %>%
  group_by(cds_school, academic_year, race_clean) %>%
  summarise(
    # Sum suspensions
    total_suspensions = sum(total_suspensions, na.rm = TRUE),

    # Take first value of school-level variables (VALIDATED above)
    cumulative_enrollment = first(cumulative_enrollment),
    teacher_nonwhite_share = first(teacher_nonwhite_share),
    admin_nonwhite_share = first(admin_nonwhite_share),
    sed_rate = first(sed_rate),
    charter_yn_std = first(charter_yn_std),
    is_traditional = first(is_traditional),
    level_strict3 = first(level_strict3),
    school_level_final = first(school_level_final),

    # Count reasons aggregated
    n_reasons = n(),

    .groups = "drop"
  )

# Recalculate suspension rate
agg_df <- agg_df %>%
  mutate(
    suspension_rate = safe_div(total_suspensions, cumulative_enrollment)
  )

message("    Aggregated rows: ", format_number(nrow(agg_df)))
message("    Average reasons per school-year-race: ",
        round(nrow(df_raw) / nrow(agg_df), 1))

# === 7) Filter to complete cases with DIAGNOSTICS ============================
message("\n>>> Filtering to complete cases...")

before_filter <- nrow(agg_df)

# Track missing data by variable
missing_summary <- agg_df %>%
  summarise(
    across(
      c(suspension_rate, teacher_nonwhite_share, admin_nonwhite_share,
        cumulative_enrollment, sed_rate, charter_yn_std, level_strict3),
      ~sum(is.na(.x))
    )
  )

message("    Missing data summary:")
for (var in names(missing_summary)) {
  n_miss <- missing_summary[[var]]
  if (n_miss > 0) {
    pct_miss <- 100 * n_miss / before_filter
    message("      ", var, ": ", format_number(n_miss),
            " (", sprintf("%.1f%%", pct_miss), ")")
  }
}

# Filter to complete cases
df_final <- agg_df %>%
  filter(
    !is.na(suspension_rate),
    !is.na(teacher_nonwhite_share),
    !is.na(admin_nonwhite_share),
    !is.na(cumulative_enrollment),
    cumulative_enrollment > 0
  )

after_filter <- nrow(df_final)
dropped <- before_filter - after_filter
dropped_pct <- 100 * dropped / before_filter

message("\n    Dropped ", format_number(dropped), " rows due to missing data ",
        "(", sprintf("%.1f%%", dropped_pct), ")")
message("    Final analysis sample: ", format_number(after_filter), " rows")

# === 8) Power analysis by racial group =======================================
message("\n>>> Conducting power analysis by racial/ethnic group...")

# CRITICAL FIX: Match Analysis 21 specification EXACTLY
# From Analysis/21_teacher_diversity_regression.R (lines 810-819):
#   suspension_rate ~ teacher_non_white_share + admin_non_white_share +
#                     sed_rate + is_charter + grade_level
# Where:
#   - teacher_non_white_share: 1 df
#   - admin_non_white_share: 1 df
#   - sed_rate: 1 df (continuous)
#   - is_charter: 1 df (binary)
#   - grade_level: 4 df (factor with 5 levels: Elementary, Middle, High, Other, Alternative)
# Total: u=2 (predictors of interest), v=6 (controls)

u_predictors <- 2  # teacher + admin diversity
v_controls <- 6    # sed_rate (1) + is_charter (1) + grade_level (4)

message("    Regression specification:")
message("      u (predictors of interest): ", u_predictors,
        " (teacher_nonwhite_share + admin_nonwhite_share)")
message("      v (controls): ", v_controls,
        " (sed_rate [1] + is_charter [1] + grade_level [4 df for 5 levels])")
message("      Total model df: ", u_predictors + v_controls, " + 1 intercept = ",
        u_predictors + v_controls + 1)

# Cohen's f² benchmarks for reference
cohen_small <- 0.02
cohen_medium <- 0.15
cohen_large <- 0.35

# Bonferroni correction for 8 groups
n_groups <- 8
alpha_uncorrected <- 0.05
alpha_bonferroni <- alpha_uncorrected / n_groups

message("\n    Multiple comparisons adjustment:")
message("      Testing ", n_groups, " racial/ethnic groups")
message("      Uncorrected α = ", alpha_uncorrected)
message("      Bonferroni-corrected α = ", sprintf("%.5f", alpha_bonferroni))

# Calculate power for each racial group
race_groups <- sort(unique(df_final$race_clean))
power_results <- list()

for (race in race_groups) {
  message("\n────────────────────────────────────────────────────────────────")
  message("📊 ", race)
  message("────────────────────────────────────────────────────────────────")

  race_df <- df_final %>% filter(race_clean == race)

  # Get enrollment weights
  enrollment <- race_df$cumulative_enrollment
  keep <- !is.na(enrollment) & enrollment > 0
  enrollment <- enrollment[keep]

  n_schools <- length(enrollment)

  # Calculate effective sample size using Kish's formula
  # N_eff = (Σw)² / Σw²
  sum_weights <- sum(enrollment)
  sum_weights_sq <- sum(enrollment^2)
  n_effective <- (sum_weights^2) / sum_weights_sq
  efficiency <- n_effective / n_schools

  message(">>> Unweighted N: ", format_number(n_schools), " school-year-race observations")
  message(">>> Effective N (Kish): ", format_number(round(n_effective)))
  message(">>> Efficiency: ", sprintf("%.1f%%", 100 * efficiency))
  message("    (proportion of statistical information retained after weighting)")

  # Check if sufficient df for regression
  residual_df <- n_effective - u_predictors - v_controls - 1
  message(">>> Residual df: ", format_number(round(residual_df)))

  if (residual_df <= 0) {
    message("⚠ WARNING: Insufficient degrees of freedom for regression!")
    message("  Need at least ", u_predictors + v_controls + 2, " effective observations")

    power_results[[race]] <- data.frame(
      race = race,
      n_schools = n_schools,
      n_effective = round(n_effective),
      efficiency = efficiency,
      residual_df = round(residual_df),
      min_detectable_f2 = NA,
      power_small = NA,
      power_medium = NA,
      power_large = NA,
      min_detectable_f2_bonf = NA,
      power_small_bonf = NA,
      power_medium_bonf = NA,
      power_large_bonf = NA,
      warning = "Insufficient df"
    )
    next
  }

  # Sensitivity analysis: What effect can we detect with 80% power?
  min_f2 <- tryCatch({
    pwr.f2.test(
      u = u_predictors,
      v = residual_df,
      sig.level = alpha_uncorrected,
      power = 0.80
    )$f2
  }, error = function(e) NA)

  # Power for standard benchmarks
  power_small <- if (!is.na(min_f2)) {
    tryCatch(
      pwr.f2.test(
        u = u_predictors,
        v = residual_df,
        f2 = cohen_small,
        sig.level = alpha_uncorrected
      )$power,
      error = function(e) NA
    )
  } else NA

  power_medium <- if (!is.na(min_f2)) {
    tryCatch(
      pwr.f2.test(
        u = u_predictors,
        v = residual_df,
        f2 = cohen_medium,
        sig.level = alpha_uncorrected
      )$power,
      error = function(e) NA
    )
  } else NA

  power_large <- if (!is.na(min_f2)) {
    tryCatch(
      pwr.f2.test(
        u = u_predictors,
        v = residual_df,
        f2 = cohen_large,
        sig.level = alpha_uncorrected
      )$power,
      error = function(e) NA
    )
  } else NA

  # Bonferroni-adjusted power
  min_f2_bonf <- tryCatch({
    pwr.f2.test(
      u = u_predictors,
      v = residual_df,
      sig.level = alpha_bonferroni,
      power = 0.80
    )$f2
  }, error = function(e) NA)

  power_small_bonf <- if (!is.na(min_f2_bonf)) {
    tryCatch(
      pwr.f2.test(
        u = u_predictors,
        v = residual_df,
        f2 = cohen_small,
        sig.level = alpha_bonferroni
      )$power,
      error = function(e) NA
    )
  } else NA

  power_medium_bonf <- if (!is.na(min_f2_bonf)) {
    tryCatch(
      pwr.f2.test(
        u = u_predictors,
        v = residual_df,
        f2 = cohen_medium,
        sig.level = alpha_bonferroni
      )$power,
      error = function(e) NA
    )
  } else NA

  power_large_bonf <- if (!is.na(min_f2_bonf)) {
    tryCatch(
      pwr.f2.test(
        u = u_predictors,
        v = residual_df,
        f2 = cohen_large,
        sig.level = alpha_bonferroni
      )$power,
      error = function(e) NA
    )
  } else NA

  # Report results
  if (!is.na(min_f2)) {
    min_r2 <- min_f2 / (1 + min_f2)
    message("\n>>> Minimum detectable effect (α=", alpha_uncorrected, ", power=80%):")
    message("    Cohen's f² = ", sprintf("%.4f", min_f2))
    message("    Equivalent R² = ", sprintf("%.4f", min_r2))
    message("    (This is the smallest effect we can reliably detect)")

    message("\n>>> Power for standard effect sizes (α=", alpha_uncorrected, "):")
    message("    Small (f²=", cohen_small, ", R²≈0.02): ", sprintf("%.1f%%", 100*power_small))
    message("    Medium (f²=", cohen_medium, ", R²≈0.13): ", sprintf("%.1f%%", 100*power_medium))
    message("    Large (f²=", cohen_large, ", R²≈0.26): ", sprintf("%.1f%%", 100*power_large))
  }

  if (!is.na(min_f2_bonf)) {
    min_r2_bonf <- min_f2_bonf / (1 + min_f2_bonf)
    message("\n>>> With Bonferroni correction (α=", sprintf("%.5f", alpha_bonferroni), ", power=80%):")
    message("    Minimum detectable f² = ", sprintf("%.4f", min_f2_bonf))
    message("    Equivalent R² = ", sprintf("%.4f", min_r2_bonf))
    message("\n>>> Power for standard effect sizes (Bonferroni-adjusted):")
    message("    Small: ", sprintf("%.1f%%", 100*power_small_bonf))
    message("    Medium: ", sprintf("%.1f%%", 100*power_medium_bonf))
    message("    Large: ", sprintf("%.1f%%", 100*power_large_bonf))
  }

  # Store results
  power_results[[race]] <- data.frame(
    race = race,
    n_schools = n_schools,
    n_effective = round(n_effective),
    efficiency = efficiency,
    residual_df = round(residual_df),
    min_detectable_f2 = min_f2,
    min_detectable_r2 = if (!is.na(min_f2)) min_f2 / (1 + min_f2) else NA,
    power_small = power_small,
    power_medium = power_medium,
    power_large = power_large,
    min_detectable_f2_bonf = min_f2_bonf,
    min_detectable_r2_bonf = if (!is.na(min_f2_bonf)) min_f2_bonf / (1 + min_f2_bonf) else NA,
    power_small_bonf = power_small_bonf,
    power_medium_bonf = power_medium_bonf,
    power_large_bonf = power_large_bonf,
    warning = NA_character_
  )
}

# Combine results
power_df <- bind_rows(power_results)

# === 9) Save results ==========================================================
message("\n════════════════════════════════════════════════════════════════")
message("=== Saving Results ===")
message("════════════════════════════════════════════════════════════════\n")

# Save to CSV
csv_path <- file.path(output_tables, "26_power_analysis_results.csv")
write.csv(power_df, csv_path, row.names = FALSE)
message("✓ Saved: ", csv_path)

# Save to Excel with better formatting
xlsx_path <- file.path(output_tables, "26_power_analysis_results.xlsx")
write_xlsx(
  list(
    "Power Analysis" = power_df,
    "Metadata" = data.frame(
      Parameter = c(
        "Analysis Date",
        "Data File",
        "Academic Years",
        "Initial Rows",
        "Final Sample Size",
        "u (predictors)",
        "v (controls)",
        "Alpha (uncorrected)",
        "Alpha (Bonferroni)",
        "Target Power",
        "Cohen Small f²",
        "Cohen Medium f²",
        "Cohen Large f²",
        "Script Version"
      ),
      Value = c(
        as.character(Sys.Date()),
        basename(MERGED_PATH),
        "2018-19 through 2023-24",
        format_number(initial_rows),
        format_number(nrow(df_final)),
        as.character(u_predictors),
        as.character(v_controls),
        as.character(alpha_uncorrected),
        sprintf("%.5f", alpha_bonferroni),
        "0.80",
        as.character(cohen_small),
        as.character(cohen_medium),
        as.character(cohen_large),
        "2.0 (2025-11-21)"
      )
    )
  ),
  path = xlsx_path
)
message("✓ Saved: ", xlsx_path)

# Save diagnostics summary
diagnostics_df <- data.frame(
  stage = c(
    "1. Initial load",
    "2. After race canonicalization",
    "3. After aggregation",
    "4. Final analysis sample"
  ),
  n_rows = c(
    initial_rows,
    initial_rows - unmapped_count,
    nrow(agg_df),
    nrow(df_final)
  ),
  pct_retained = c(
    100,
    100 * (initial_rows - unmapped_count) / initial_rows,
    100 * nrow(agg_df) / initial_rows,
    100 * nrow(df_final) / initial_rows
  )
)

diag_path <- file.path(output_tables, "26_power_analysis_diagnostics.csv")
write.csv(diagnostics_df, diag_path, row.names = FALSE)
message("✓ Saved: ", diag_path)

# === 10) Create power curve visualization ====================================
message("\n>>> Creating power curve visualization...")

# Create power curves for each group
f2_seq <- seq(0.001, 0.10, length.out = 100)

power_curves <- lapply(race_groups, function(race) {
  race_info <- power_df %>% filter(race == !!race)

  if (nrow(race_info) == 0 || is.na(race_info$residual_df)) {
    return(NULL)
  }

  v_resid <- race_info$residual_df

  curve_data <- data.frame(
    f2 = f2_seq,
    power_uncorrected = sapply(f2_seq, function(f2) {
      tryCatch(
        pwr.f2.test(u = u_predictors, v = v_resid, f2 = f2,
                   sig.level = alpha_uncorrected)$power,
        error = function(e) NA
      )
    }),
    power_bonferroni = sapply(f2_seq, function(f2) {
      tryCatch(
        pwr.f2.test(u = u_predictors, v = v_resid, f2 = f2,
                   sig.level = alpha_bonferroni)$power,
        error = function(e) NA
      )
    }),
    race = race,
    n_effective = race_info$n_effective
  )

  curve_data
})

power_curves_df <- bind_rows(power_curves)

if (nrow(power_curves_df) > 0) {
  p <- ggplot(power_curves_df, aes(x = f2, y = power_uncorrected, color = race)) +
    geom_line(linewidth = 1) +
    geom_hline(yintercept = 0.80, linetype = "dashed", color = "gray40") +
    geom_vline(xintercept = cohen_small, linetype = "dotted", color = "gray60") +
    annotate("text", x = cohen_small, y = 0.05, label = "Small\n(0.02)",
             size = 3, hjust = -0.1) +
    scale_x_continuous(
      limits = c(0, 0.10),
      breaks = seq(0, 0.10, 0.02)
    ) +
    scale_y_continuous(
      limits = c(0, 1),
      breaks = seq(0, 1, 0.2),
      labels = scales::percent
    ) +
    labs(
      title = "Statistical Power by Effect Size and Student Race/Ethnicity",
      subtitle = paste0("Uncorrected α = ", alpha_uncorrected,
                       " | Target power = 80% | u=", u_predictors, ", v=", v_controls),
      x = "Effect Size (Cohen's f²)",
      y = "Statistical Power",
      color = "Student Race/Ethnicity",
      caption = "Note: Curves based on effective sample sizes after enrollment weighting.\nDashed line indicates 80% power threshold; dotted line shows Cohen's 'small' effect."
    ) +
    theme_minimal(base_size = 11) +
    theme(
      legend.position = "right",
      plot.title = element_text(face = "bold", size = 13),
      plot.caption = element_text(hjust = 0, size = 8, color = "gray40")
    )

  plot_path <- file.path(output_graphs, "26_power_curves.png")
  ggsave(plot_path, p, width = 12, height = 7, dpi = 300)
  message("✓ Saved: ", plot_path)
}

# === 11) Summary =============================================================
message("\n════════════════════════════════════════════════════════════════")
message("=== Summary ===")
message("════════════════════════════════════════════════════════════════\n")

message("Power analysis complete for ", nrow(power_df), " racial/ethnic groups")
message("\nKey findings:")
message("  • Specification: u=", u_predictors, ", v=", v_controls, " (MATCHES Analysis 21)")
message("  • All groups have effective N ranging from ",
        format_number(min(power_df$n_effective, na.rm = TRUE)), " to ",
        format_number(max(power_df$n_effective, na.rm = TRUE)))
message("  • Minimum detectable effects (80% power) range from f²=",
        sprintf("%.4f", min(power_df$min_detectable_f2, na.rm = TRUE)), " to f²=",
        sprintf("%.4f", max(power_df$min_detectable_f2, na.rm = TRUE)))

well_powered <- power_df %>%
  filter(power_small >= 0.80) %>%
  nrow()

message("  • ", well_powered, "/", nrow(power_df),
        " groups have ≥80% power to detect 'small' effects")

message("\n⚠ IMPORTANT NOTES:")
message("  • v=6 (not v=4) - includes grade_level with 4 df")
message("  • Power calculations assume enrollment weighting (as in Analysis 21)")
message("  • Bonferroni correction accounts for testing 8 groups")
message("  • Non-significant findings in well-powered groups can be interpreted as true nulls")

message("\n════════════════════════════════════════════════════════════════")
message("=== Analysis Complete ===")
message("════════════════════════════════════════════════════════════════\n")
