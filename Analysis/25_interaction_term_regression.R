# Analysis/25_interaction_term_regression.R
#
# Interaction Term Regression: Testing the "Mismatch Hypothesis"
#
# Hypothesis: The effect of % White Teachers on suspension rates is moderated
# by % Black Student Enrollment. Specifically, we expect a POSITIVE interaction
# coefficient, indicating that the "White Teacher Effect" is amplified as Black
# enrollment increases.
#
# This analysis uses a pooled regression model with an interaction term to
# formally test what Analysis 24 examined descriptively (separate regressions
# per quartile).
#
# Input: susp_v6_teacher_features.parquet (merged student + teacher data)
# Output:
#   - tables/25_interaction_regression_results.csv (regression coefficients)
#   - graphs/25_interaction_marginal_effects.png (interaction plot)
#
# Key methodological notes:
# 1. Uses interaction term: % White Teachers * % Black Students
# 2. Weighted by student enrollment for representativeness
# 3. Marginal effects plot shows predicted rates at different Black enrollment levels
# 4. Non-causal interpretation: correlational patterns only
#
# CRITICAL: SUSPENSION RATE DEFINITION
# =====================================
# This analysis uses TOTAL SUSPENSION INCIDENTS (not unduplicated student count).
#
# - Numerator: total_suspensions = Total count of suspension incidents/events
#   (If a student is suspended multiple times, each incident is counted)
#
# - Denominator: cumulative_enrollment = Total student enrollment
#
# - Rate = total_suspensions / cumulative_enrollment
#
# - Interpretation: Average number of suspension incidents per enrolled student
#   (Can exceed 1.0 if students experience multiple suspensions)
#
# ALTERNATIVE MEASURE (not used here):
# - unduplicated_suspensions = Count of unique students suspended at least once
# - Unduplicated rate = unduplicated_suspensions / cumulative_enrollment
# - Interpretation: Percentage of students who experienced at least one suspension
#   (Always between 0-100%)

# === 1) Setup =================================================================
suppressPackageStartupMessages({
  library(here); library(arrow); library(dplyr); library(tidyr)
  library(ggplot2); library(scales); library(broom); library(writexl)
})

try(here::i_am("Analysis/25_interaction_term_regression.R"), silent = TRUE)

# Load canonical definitions
source(here::here("R", "utils_keys_filters.R"))

theme_set(theme_minimal(base_size = 11))

# Helper function
format_number <- function(x) {
  format(x, big.mark = ",", scientific = FALSE, trim = TRUE)
}

safe_div <- function(num, denom, replace_na_with = NA_real_) {
  ifelse(denom == 0 | is.na(denom), replace_na_with, num / denom)
}

# === 2) Load and prepare data =================================================
message("\n════════════════════════════════════════════════════════════════")
message("=== 25: Interaction Term Regression Analysis ===")
message("════════════════════════════════════════════════════════════════\n")

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
  janitor::clean_names()

# Build keys if function available
if (exists("build_keys")) {
  df_raw <- build_keys(df_raw)
}

message(">>> Loaded ", format_number(nrow(df_raw)), " rows")

# === 3) Aggregate to school-year level =========================================
message("\n>>> Aggregating to school-year level...")
message(">>> Initial rows (school-year-subgroup-reason): ", format_number(nrow(df_raw)))

# CRITICAL: Use CDE's "All Students" subgroup for accurate totals
# Raw data uses "subgroup" column (not "race") for demographic categories
# Filtering to subgroup == "All Students" gives us CDE's pre-calculated totals

# STEP 1: Filter to "All Students" subgroup
if (!"subgroup" %in% names(df_raw)) {
  stop("Column 'subgroup' not found. Cannot aggregate using 'All Students' total.")
}

if (!"All Students" %in% unique(df_raw$subgroup)) {
  stop("'All Students' not found in subgroup column. Cannot use CDE's pre-calculated totals.")
}

message(">>> Filtering to subgroup == 'All Students' (CDE's pre-calculated total)...")
df_all_students <- df_raw %>%
  filter(subgroup == "All Students")

message(">>> After filtering: ", format_number(nrow(df_all_students)), " rows")
message(">>> Data reduction: ", round(nrow(df_raw) / nrow(df_all_students), 1), "x")

# STEP 2: Aggregate across suspension reasons
aggregate_to_school_year <- function(df) {
  # Identify suspension and enrollment columns
  susp_cols <- grep("^total_suspensions|^suspension_count", names(df), value = TRUE)
  enrollment_cols <- intersect(c("cumulative_enrollment", "sup_cumulative_enrollment"), names(df))

  # Identify school-level variables to preserve
  teacher_cols <- grep("^teacher_", names(df), value = TRUE)
  charter_cols <- grep("^charter_|^is_traditional", names(df), value = TRUE)
  level_cols <- grep("^level_strict|^school_level", names(df), value = TRUE)
  sed_cols <- grep("^sed_rate|^socio", names(df), value = TRUE, ignore.case = TRUE)
  black_cols <- grep("prop_black|black_share", names(df), value = TRUE)
  school_cols <- c("cds_school", "school_code", "academic_year", "aggregate_level")

  constant_cols <- unique(c(
    enrollment_cols,  # Now truly constant (not subgroup-specific)
    teacher_cols,
    charter_cols,
    level_cols,
    sed_cols,
    black_cols
  ))
  constant_cols <- intersect(constant_cols, names(df))

  # Group by school-year and aggregate across reasons
  agg_df <- df %>%
    group_by(cds_school, academic_year) %>%
    summarise(
      # Take first value of total_suspensions (constant across reason rows)
      # NOTE: total_suspensions is the SAME on all reason rows, so first() = the correct total
      across(any_of(susp_cols), ~first(.x)),

      # Take first value of enrollment (truly constant now with "All Students")
      across(any_of(enrollment_cols), ~first(.x)),

      # Take first value of school-level variables (should be constant)
      across(any_of(constant_cols), ~first(.x)),

      # Additional metadata columns
      across(any_of(c("school_code", "aggregate_level")), ~first(.x)),

      # Count observations aggregated (should be ~6 reasons)
      n_reasons_aggregated = n(),

      .groups = "drop"
    )

  message(">>> Aggregated to ", format_number(nrow(agg_df)), " school-year observations")
  message(">>> Average reasons per school-year: ", round(nrow(df) / nrow(agg_df), 1))

  return(agg_df)
}

df_aggregated <- aggregate_to_school_year(df_all_students)

# === 4) Extract key variables =================================================

# Helper function to extract % White Teachers
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
    pct_white <- safe_div(white_count, total_count, 0) * 100
    return(pct_white)
  }

  stop("Could not find % White Teachers column in dataset. ",
       "Expected columns like 'teacher_staff_count_white_share' or similar.")
}

# Helper function to extract % Black Students
extract_pct_black_students <- function(df) {
  # Try to find black_share, prop_black, or calculate from enrollment

  # Pattern 1: Direct proportion column
  black_prop_cols <- grep("^prop_black$|black.*share$", names(df), value = TRUE,
                          ignore.case = TRUE)

  if (length(black_prop_cols) > 0) {
    message(">>> Using column for % Black Students: ", black_prop_cols[1])
    pct_black <- as.numeric(df[[black_prop_cols[1]]]) * 100
    return(pct_black)
  }

  # Pattern 2: Try enrollment columns
  if (all(c("enroll_Black", "enroll_All") %in% names(df))) {
    message(">>> Calculating % Black Students from enroll_Black / enroll_All")
    pct_black <- safe_div(as.numeric(df$enroll_Black),
                          as.numeric(df$enroll_All), 0) * 100
    return(pct_black)
  }

  # If nothing found, provide helpful error
  stop("Could not find % Black Students column.\n",
       "  Expected columns: 'prop_black', 'black_share', or 'enroll_Black' + 'enroll_All'\n",
       "  Available columns: ", paste(head(names(df), 50), collapse = ", "))
}

# === 5) Prepare analysis dataset ==============================================
message("\n>>> Preparing analysis dataset...")

# Add % White Teachers
df <- df_aggregated %>%
  mutate(pct_white_teachers = extract_pct_white_teachers(.))

# Add % Black Students
# Check for available columns (prop_black from v3+ pipeline, or black_share)
if ("prop_black" %in% names(df_aggregated)) {
  df <- df %>%
    mutate(pct_black_students = as.numeric(prop_black) * 100)
  message(">>> Using prop_black column for % Black Students")
} else if ("black_share" %in% names(df_aggregated)) {
  df <- df %>%
    mutate(pct_black_students = as.numeric(black_share) * 100)
  message(">>> Using black_share column for % Black Students")
} else {
  # Try to extract using helper function
  df <- df %>%
    mutate(pct_black_students = extract_pct_black_students(.))
}

# Identify suspension rate column
susp_rate_cols <- c("suspension_rate_percent_total", "susp_all_rate",
                    "suspension_rate")
susp_col <- intersect(susp_rate_cols, names(df))[1]

if (is.na(susp_col)) {
  # Try to calculate from counts
  if (all(c("total_suspensions", "cumulative_enrollment") %in% names(df))) {
    message(">>> Calculating suspension rate from counts")
    df <- df %>%
      mutate(suspension_rate = safe_div(total_suspensions, cumulative_enrollment, 0))
    susp_col <- "suspension_rate"
  } else {
    stop("Could not find suspension rate column in dataset")
  }
}

message(">>> Using suspension rate column: ", susp_col)

# Standardize to percentage scale
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

# === 6) Add controls and filter to analysis sample ===========================

# Add control variables

# SED rate (Socioeconomically Disadvantaged - NOT Special Education)
# Try multiple patterns to find SED-related columns
sed_cols <- grep("^sed_rate$|sed.*rate|sed.*share|socio.*disadv.*rate|economic.*disadv.*rate",
                 names(df), value = TRUE, ignore.case = TRUE)

if (length(sed_cols) > 0) {
  sed_col <- sed_cols[1]
  df <- df %>%
    mutate(sed_rate = as.numeric(.data[[sed_col]]))
  message(">>> Added SED rate control: ", sed_col)
} else {
  # If not found, check if it already exists
  if ("sed_rate" %in% names(df)) {
    message(">>> SED rate already exists in dataset")
  } else {
    message("⚠ Warning: SED rate not found. Available columns with 'sed' or 'socio':")
    sed_related <- grep("sed|socio|economic", names(df), value = TRUE, ignore.case = TRUE)
    if (length(sed_related) > 0) {
      message("    ", paste(sed_related, collapse = ", "))
    } else {
      message("    None found - SED rate will not be included as a control")
    }
  }
}

# Charter status
charter_cols <- intersect(c("charter_yn_std", "charter_yn", "is_traditional"),
                          names(df))
if (length(charter_cols) > 0) {
  charter_col <- charter_cols[1]
  df <- df %>%
    mutate(is_charter = case_when(
      is.logical(.data[[charter_col]]) ~ as.integer(.data[[charter_col]]),
      tolower(as.character(.data[[charter_col]])) %in%
        c("y", "yes", "charter", "true", "1") ~ 1L,
      TRUE ~ 0L
    ))
  message(">>> Added charter status control: ", charter_col)
}

# School level
level_cols <- intersect(c("level_strict3", "school_level_final",
                          "school_type", "school_level"),
                        names(df))
if (length(level_cols) > 0) {
  df <- df %>%
    mutate(school_level_factor = factor(.data[[level_cols[1]]]))
  message(">>> Added school level control: ", level_cols[1])
}

# Filter to analysis sample
# Criteria:
# 1. Has teacher diversity data
# 2. Has Black student percentage data
# 3. Has suspension rate data
# 4. Has enrollment for weighting
# 5. Focus on recent years (2018-19 onwards) for better data coverage
# 6. School-level data only

analysis_df <- df %>%
  filter(
    !is.na(pct_white_teachers),
    !is.na(pct_black_students),
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
}

# Exclude special school codes if school_code column exists
if ("school_code" %in% names(analysis_df)) {
  message(">>> Excluding special school codes")
  analysis_df <- analysis_df %>%
    filter(!school_code %in% SPECIAL_SCHOOL_CODES)
}

message("\n>>> Analysis sample: ", format_number(nrow(analysis_df)),
        " school-year observations")
message(">>> Unique schools: ", format_number(n_distinct(analysis_df$cds_school)))
message(">>> Academic years: ", paste(sort(unique(analysis_df$academic_year)),
                                       collapse = ", "))

# Summary statistics
message("\n>>> Summary statistics:")
message("  % White Teachers: Mean = ", sprintf("%.1f%%", mean(analysis_df$pct_white_teachers, na.rm = TRUE)),
        ", Range = [", sprintf("%.1f", min(analysis_df$pct_white_teachers, na.rm = TRUE)),
        ", ", sprintf("%.1f", max(analysis_df$pct_white_teachers, na.rm = TRUE)), "]")
message("  % Black Students: Mean = ", sprintf("%.1f%%", mean(analysis_df$pct_black_students, na.rm = TRUE)),
        ", Range = [", sprintf("%.1f", min(analysis_df$pct_black_students, na.rm = TRUE)),
        ", ", sprintf("%.1f", max(analysis_df$pct_black_students, na.rm = TRUE)), "]")
message("  Suspension Rate: Mean = ", sprintf("%.2f%%", mean(analysis_df$suspension_rate_pct, na.rm = TRUE)),
        ", Range = [", sprintf("%.2f", min(analysis_df$suspension_rate_pct, na.rm = TRUE)),
        ", ", sprintf("%.2f", max(analysis_df$suspension_rate_pct, na.rm = TRUE)), "]")

# Add SED rate summary if available
if ("sed_rate" %in% names(analysis_df)) {
  sed_mean <- mean(analysis_df$sed_rate, na.rm = TRUE)
  sed_min <- min(analysis_df$sed_rate, na.rm = TRUE)
  sed_max <- max(analysis_df$sed_rate, na.rm = TRUE)
  # Convert to percentage if in [0,1] range
  if (sed_mean <= 1) {
    message("  % SED (Socioeconomically Disadvantaged): Mean = ", sprintf("%.1f%%", sed_mean * 100),
            ", Range = [", sprintf("%.1f%%", sed_min * 100),
            ", ", sprintf("%.1f%%", sed_max * 100), "]")
  } else {
    message("  % SED (Socioeconomically Disadvantaged): Mean = ", sprintf("%.1f%%", sed_mean),
            ", Range = [", sprintf("%.1f%%", sed_min),
            ", ", sprintf("%.1f%%", sed_max), "]")
  }
}

# === 7) Run interaction term regression ======================================
message("\n════════════════════════════════════════════════════════════════")
message("📈 RUNNING INTERACTION TERM REGRESSION")
message("════════════════════════════════════════════════════════════════\n")

# Build formula with interaction term
# The * operator in R automatically includes:
#   - Main effect of pct_white_teachers
#   - Main effect of pct_black_students
#   - Interaction: pct_white_teachers:pct_black_students

predictors <- c("pct_white_teachers * pct_black_students")

# Add available controls
controls <- character()
if ("sed_rate" %in% names(analysis_df)) {
  controls <- c(controls, "sed_rate")
  message("✓ Including SED rate as control")
} else {
  message("⚠ SED rate NOT available in analysis sample")
}
if ("is_charter" %in% names(analysis_df)) {
  controls <- c(controls, "is_charter")
  message("✓ Including charter status as control")
}
if ("school_level_factor" %in% names(analysis_df)) {
  controls <- c(controls, "school_level_factor")
  message("✓ Including school level as control")
}

if (length(controls) > 0) {
  formula_str <- paste("suspension_rate_pct ~",
                       paste(c(predictors, controls), collapse = " + "))
} else {
  formula_str <- paste("suspension_rate_pct ~", predictors)
}

message("\n>>> Regression formula:")
message("    ", formula_str)
message("\n>>> Controls included: ",
        if (length(controls) > 0) paste(controls, collapse = ", ") else "None")
message("")

formula_obj <- as.formula(formula_str)

# Fit weighted regression
fit <- lm(formula_obj, data = analysis_df, weights = cumulative_enrollment)

message("\n════════════════════════════════════════════════════════════════")
message("📊 REGRESSION RESULTS")
message("════════════════════════════════════════════════════════════════\n")

print(summary(fit))

# === 8) Extract and interpret key coefficients ===============================
message("\n════════════════════════════════════════════════════════════════")
message("🔍 KEY COEFFICIENTS (with 95% CI)")
message("════════════════════════════════════════════════════════════════\n")

coef_summary <- broom::tidy(fit, conf.int = TRUE)
glance_stats <- broom::glance(fit)

# Focus on key variables
key_vars <- c("pct_white_teachers", "pct_black_students",
              "pct_white_teachers:pct_black_students")

for (var in key_vars) {
  if (var %in% coef_summary$term) {
    row <- coef_summary[coef_summary$term == var, ]

    sig <- if (row$p.value < 0.001) "***" else if (row$p.value < 0.01) "**" else if (row$p.value < 0.05) "*" else ""

    message(sprintf("%-40s: %8.6f", var, row$estimate))
    message(sprintf("  %38s  SE: %8.6f", "", row$std.error))
    message(sprintf("  %38s  95%% CI: [%8.6f, %8.6f]", "", row$conf.low, row$conf.high))
    message(sprintf("  %38s  p = %6.4f %s", "", row$p.value, sig))
    message("")
  }
}

# Extract interaction coefficient for interpretation
interaction_coef <- coef_summary$estimate[coef_summary$term == "pct_white_teachers:pct_black_students"]
interaction_p <- coef_summary$p.value[coef_summary$term == "pct_white_teachers:pct_black_students"]
interaction_se <- coef_summary$std.error[coef_summary$term == "pct_white_teachers:pct_black_students"]
interaction_ci_low <- coef_summary$conf.low[coef_summary$term == "pct_white_teachers:pct_black_students"]
interaction_ci_high <- coef_summary$conf.high[coef_summary$term == "pct_white_teachers:pct_black_students"]

message("════════════════════════════════════════════════════════════════")
message("🎯 HYPOTHESIS TEST: INTERACTION TERM")
message("════════════════════════════════════════════════════════════════\n")

message("H0: Interaction coefficient = 0 (no moderation)")
message("H1: Interaction coefficient > 0 (positive moderation)\n")

message("Result:")
if (interaction_p < 0.05 && interaction_coef > 0) {
  message("  ✓ HYPOTHESIS SUPPORTED")
  message("    The interaction term is POSITIVE and SIGNIFICANT (p < 0.05)")
  message("    → The effect of % White Teachers on suspension rates is")
  message("      AMPLIFIED as % Black Student enrollment increases")
} else if (interaction_p >= 0.05) {
  message("  ✗ HYPOTHESIS NOT SUPPORTED")
  message("    The interaction term is NOT statistically significant (p >= 0.05)")
  message("    → No evidence that the effect varies by Black enrollment")
} else if (interaction_coef < 0) {
  message("  ✗ HYPOTHESIS CONTRADICTED")
  message("    The interaction term is NEGATIVE (opposite direction)")
  message("    → The effect is WEAKER at higher Black enrollment")
}

message("\nModel fit:")
message(sprintf("  R² = %.4f  |  Adj. R² = %.4f  |  N = %s",
                glance_stats$r.squared, glance_stats$adj.r.squared,
                format_number(stats::nobs(fit))))

# === 9) Create marginal effects plot =========================================
message("\n════════════════════════════════════════════════════════════════")
message("📊 CREATING INTERACTION PLOT (MARGINAL EFFECTS)")
message("════════════════════════════════════════════════════════════════\n")

# Calculate marginal effects at different levels of % Black Students
# Use 10th, 50th, and 90th percentiles for "Low", "Medium", "High"

black_percentiles <- quantile(analysis_df$pct_black_students,
                               probs = c(0.10, 0.50, 0.90), na.rm = TRUE)

message(">>> Plotting marginal effects at:")
message("    Low Black Enrollment (10th percentile): ", sprintf("%.1f%%", black_percentiles[1]))
message("    Medium Black Enrollment (50th percentile): ", sprintf("%.1f%%", black_percentiles[2]))
message("    High Black Enrollment (90th percentile): ", sprintf("%.1f%%", black_percentiles[3]))

# Create prediction grid
white_teacher_seq <- seq(0, 100, by = 1)

# Build prediction data frames for each level
pred_data_list <- list()

for (i in 1:3) {
  level_name <- c("Low", "Medium", "High")[i]
  black_pct <- black_percentiles[i]

  pred_df <- data.frame(
    pct_white_teachers = white_teacher_seq,
    pct_black_students = black_pct,
    level = level_name,
    level_label = sprintf("%s (%.1f%% Black)", level_name, black_pct)
  )

  # Add control variable means/modes
  if ("sed_rate" %in% names(analysis_df)) {
    pred_df$sed_rate <- mean(analysis_df$sed_rate, na.rm = TRUE)
  }
  if ("is_charter" %in% names(analysis_df)) {
    pred_df$is_charter <- 0  # Mode: traditional schools
  }
  if ("school_level_factor" %in% names(analysis_df)) {
    # Use most common level
    mode_level <- names(sort(table(analysis_df$school_level_factor), decreasing = TRUE))[1]
    pred_df$school_level_factor <- factor(mode_level,
                                          levels = levels(analysis_df$school_level_factor))
  }

  pred_data_list[[i]] <- pred_df
}

pred_data <- bind_rows(pred_data_list)

# Make predictions
pred_data$predicted_suspension_rate <- predict(fit, newdata = pred_data)

# Order levels for plotting
pred_data$level <- factor(pred_data$level, levels = c("Low", "Medium", "High"))
pred_data$level_label <- factor(pred_data$level_label,
                                levels = sprintf("%s (%.1f%% Black)",
                                                c("Low", "Medium", "High"),
                                                black_percentiles))

# Create interaction plot
# Build color and linetype vectors with dynamic names
color_values <- c("#00A5E0", "#FFB81C", "#C4820E")  # Blue, Gold, Dark gold
names(color_values) <- sprintf("%s (%.1f%% Black)",
                               c("Low", "Medium", "High"),
                               black_percentiles)

linetype_values <- c("solid", "dashed", "solid")
names(linetype_values) <- sprintf("%s (%.1f%% Black)",
                                  c("Low", "Medium", "High"),
                                  black_percentiles)

p <- ggplot(pred_data, aes(x = pct_white_teachers, y = predicted_suspension_rate,
                           color = level_label, linetype = level_label)) +
  geom_line(linewidth = 1.2) +

  # Add reference line at y = 0
  geom_hline(yintercept = 0, linetype = "dotted", color = "gray50") +

  # Color palette (UCLA-inspired)
  scale_color_manual(values = color_values) +
  scale_linetype_manual(values = linetype_values) +

  # Scales
  scale_x_continuous(
    labels = function(x) paste0(x, "%"),
    breaks = seq(0, 100, 20)
  ) +
  scale_y_continuous(
    labels = function(y) paste0(y, "%")
  ) +

  # Labels
  labs(
    title = "Marginal Effects Plot: Interaction Between % White Teachers and % Black Students",
    subtitle = paste0(
      "Predicted Suspension Rates at Different Levels of Black Student Enrollment\n",
      "Hypothesis: Steeper slope for 'High' line indicates positive interaction"
    ),
    x = "% White Teachers",
    y = "Predicted Suspension Rate (%)",
    color = "Black Student Enrollment",
    linetype = "Black Student Enrollment",
    caption = paste0(
      "Note: Predictions from weighted linear regression with interaction term.\n",
      "Controls held at: ",
      if ("sed_rate" %in% names(analysis_df)) {
        sed_mean_val <- mean(analysis_df$sed_rate, na.rm = TRUE)
        # Handle both proportion [0,1] and percentage scales
        sed_pct <- if (sed_mean_val <= 1) sed_mean_val * 100 else sed_mean_val
        paste0("SED rate = ", sprintf("%.1f%%", sed_pct), ", ")
      } else "",
      "Traditional schools (non-charter)",
      if ("school_level_factor" %in% names(analysis_df)) {
        paste0(", ", names(sort(table(analysis_df$school_level_factor), decreasing = TRUE))[1], " schools")
      } else "",
      ".\n",
      "Interaction coefficient: ", sprintf("%.4f", interaction_coef),
      " (p = ", sprintf("%.4f", interaction_p), ")\n\n",
      "SUSPENSION RATE DEFINITION: Total suspension incidents divided by enrollment (can exceed 100% if students ",
      "experience multiple suspensions). This is NOT the percentage of students suspended (unduplicated count)."
    )
  ) +

  # Theme
  theme_minimal(base_size = 11) +
  theme(
    plot.title = element_text(face = "bold", size = 14, hjust = 0),
    plot.subtitle = element_text(size = 11, color = "gray30", lineheight = 1.2),
    plot.caption = element_text(hjust = 0, size = 8, color = "gray40",
                                lineheight = 1.2),
    legend.position = "bottom",
    legend.title = element_text(face = "bold"),
    panel.grid.minor = element_blank(),
    panel.border = element_rect(color = "gray80", fill = NA),
    axis.title = element_text(face = "bold")
  )

# === 10) Save outputs =========================================================
message("\n>>> Saving outputs...")

# Create output directories
dir.create(here::here("outputs", "tables"), showWarnings = FALSE, recursive = TRUE)
dir.create(here::here("outputs", "graphs"), showWarnings = FALSE, recursive = TRUE)
dir.create(here::here("outputs", "summaries"), showWarnings = FALSE, recursive = TRUE)

# Save regression results table
results_table <- coef_summary %>%
  mutate(
    significance = case_when(
      p.value < 0.001 ~ "***",
      p.value < 0.01 ~ "**",
      p.value < 0.05 ~ "*",
      TRUE ~ ""
    )
  )

write.csv(
  results_table,
  here::here("outputs", "tables", "25_interaction_regression_results.csv"),
  row.names = FALSE
)
message("✓ Saved table: outputs/tables/25_interaction_regression_results.csv")

# Also save Excel version with model statistics
write_xlsx(
  list(
    "Coefficients" = results_table,
    "Model_Statistics" = data.frame(
      Statistic = c("R-squared", "Adjusted R-squared", "N", "Residual SE", "F-statistic"),
      Value = c(
        glance_stats$r.squared,
        glance_stats$adj.r.squared,
        stats::nobs(fit),
        glance_stats$sigma,
        glance_stats$statistic
      )
    )
  ),
  path = here::here("outputs", "tables", "25_interaction_regression_results.xlsx")
)
message("✓ Saved Excel: outputs/tables/25_interaction_regression_results.xlsx")

# Save plot
ggsave(
  here::here("outputs", "graphs", "25_interaction_marginal_effects.png"),
  p, width = 12, height = 8, dpi = 300, bg = "white"
)
message("✓ Saved plot: outputs/graphs/25_interaction_marginal_effects.png")


# === 11) Final message ========================================================
message("\n════════════════════════════════════════════════════════════════")
message("✓ ANALYSIS 25 COMPLETE")
message("════════════════════════════════════════════════════════════════\n")

message("Output files:")
message("  - outputs/tables/25_interaction_regression_results.csv")
message("  - outputs/tables/25_interaction_regression_results.xlsx")
message("  - outputs/graphs/25_interaction_marginal_effects.png\n")

message("Next steps:")
message("  1. Review the interaction plot to visually assess the hypothesis")
message("  2. Compare results with Analysis 24 (quartile slope comparison)")
message("  3. Consider robustness checks (different model specifications)")
message("  4. Examine residuals and model diagnostics\n")

invisible(list(
  model = fit,
  coefficients = coef_summary,
  model_stats = glance_stats,
  plot = p,
  predictions = pred_data
))
