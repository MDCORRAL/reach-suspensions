# Analysis/21_teacher_diversity_regression.R
#
# Analyzes associations between teacher/administrator racial diversity
# and student suspension rates, stratified by student race/ethnicity.
#
# Key features:
# - Uses teacher RACIAL diversity (proportion non-white staff)
# - Explicit race column detection with validation
# - Weighted linear regressions (weighted by student enrollment)
# - Stratified by student racial/ethnic group
# - Controls for SED rate, charter status, and school level

suppressPackageStartupMessages({
  library(dplyr)
  library(arrow)
  library(here)
  library(ggplot2)
  library(tidyr)
  library(writexl)
})

# =============================================================================
# CONFIGURATION
# =============================================================================

TEACHER_PATH <- here("data-stage", "susp_v6_teacher_features.parquet")
FALLBACK_PATH <- here("data-stage", "susp_v6_features.parquet")

# Race patterns to detect (matching teacher_slugify() output)
TEACHER_RACE_SLUGS <- c(
  "african_american",
  "asian",
  "filipino",
  "hispanic_or_latino",
  "american_indian_or_alaska_native",
  "native_hawaiian_pacific_islander",
  "pacific_islander",  # legacy slug still appears in some historical files
  "white",
  "two_or_more_races",
  "not_reported"
)

ALLOWED_RACE_GROUPS <- c(
  "Black/African American",
  "White",
  "Hispanic/Latino",
  "American Indian/Alaska Native",
  "Asian",
  "Filipino",
  "Native Hawaiian/Pacific Islander",
  "Two or More Races"
)

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

format_number <- function(x) {
  format(x, big.mark = ",", scientific = FALSE, trim = TRUE)
}

safe_div <- function(num, denom) {
  ifelse(denom == 0 | is.na(denom), NA_real_, num / denom)
}

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

# =============================================================================
# IMPROVED RACE DETECTION
# =============================================================================

extract_teacher_race_nonwhite_share <- function(df, prefix = "^teacher") {
  # Extract teacher non-white share from race-specific columns.
  #
  # Returns list with:
  #   - values: numeric vector of non-white shares
  #   - meta: metadata about which columns were used
  #   - NULL if no race columns found
  
  # Step 1: Find ALL teacher race share columns
  race_share_pattern <- paste0(prefix, ".*_(",
                               paste(TEACHER_RACE_SLUGS, collapse = "|"),
                               ")_share$")
  
  race_share_cols <- grep(race_share_pattern, names(df), value = TRUE, ignore.case = TRUE)
  
  if (!length(race_share_cols)) {
    message(">>> No teacher race share columns found matching pattern: ", race_share_pattern)
    return(NULL)
  }
  
  message(">>> Found ", length(race_share_cols), " teacher race share columns")
  
  # Step 2: Separate white and non-white columns
  white_cols <- grep("_white_share$", race_share_cols, value = TRUE, ignore.case = TRUE)
  white_cols <- white_cols[!grepl("non_white", white_cols, ignore.case = TRUE)]
  
  not_reported_cols <- grep("_(not_reported|unknown)_share$", race_share_cols,
                            value = TRUE, ignore.case = TRUE)
  
  non_white_cols <- setdiff(race_share_cols, c(white_cols, not_reported_cols))
  
  message(">>>   Non-white race columns: ", length(non_white_cols))
  message(">>>   White columns: ", length(white_cols))
  message(">>>   Not reported columns: ", length(not_reported_cols))
  
  if (!length(non_white_cols) && !length(white_cols)) {
    message(">>> No usable race columns")
    return(NULL)
  }
  
  # Step 3: Compute non-white share
  if (length(non_white_cols) > 0) {
    # Method 1: Sum non-white race shares
    mat <- sapply(non_white_cols, function(col) as.numeric(df[[col]]))
    if (!is.matrix(mat)) mat <- matrix(mat, ncol = 1)
    
    values <- rowSums(mat, na.rm = TRUE)
    all_missing <- apply(is.na(mat), 1, all)
    values[all_missing] <- NA_real_
    
    return(list(
      values = values,
      meta = list(
        type = "race_share",
        method = "sum_of_non_white_races",
        columns = non_white_cols,
        n_races = length(non_white_cols)
      )
    ))
    
  } else if (length(white_cols) > 0) {
    # Method 2: 1 - white_share
    white_share <- as.numeric(df[[white_cols[1]]])
    values <- 1 - white_share
    
    # Adjust for not_reported if available
    if (length(not_reported_cols) > 0) {
      not_reported <- as.numeric(df[[not_reported_cols[1]]])
      values <- values - not_reported
    }
    
    values[!is.finite(values)] <- NA_real_
    
    return(list(
      values = values,
      meta = list(
        type = "race_share",
        method = if (length(not_reported_cols)) {
          "1_minus_white_minus_not_reported"
        } else {
          "1_minus_white"
        },
        columns = c(white_cols[1], if (length(not_reported_cols)) not_reported_cols[1]),
        n_races = 1 + length(not_reported_cols)
      )
    ))
  }
  
  return(NULL)
}

extract_admin_race_nonwhite_share <- function(df) {
  # Extract administrator non-white share from race-specific columns.
  # Looks specifically for _by_type_administrators_ columns.
  
  extract_teacher_race_nonwhite_share(df, prefix = "^teacher.*by_type_administrators")
}

describe_diversity_source <- function(meta, label) {
  if (is.null(meta)) {
    return(paste0("❌ ", label, " diversity: NOT FOUND"))
  }
  
  if (meta$type == "race_share") {
    cols_preview <- if (length(meta$columns) <= 3) {
      paste0("`", meta$columns, "`", collapse = ", ")
    } else {
      paste0("`", meta$columns[1], "`, `", meta$columns[2], "`, ... (",
             length(meta$columns), " total)")
    }
    
    return(paste0("✓ ", label, " RACIAL diversity: ", meta$method,
                  " (", meta$n_races, " race categories)\n    Columns: ", cols_preview))
  }
  
  if (meta$type == "gender_share") {
    return(paste0("⚠ ", label, " GENDER diversity: ", meta$method,
                  " (FALLBACK - race data missing)"))
  }
  
  return(paste0("? ", label, " diversity: unknown type"))
}

# =============================================================================
# DATA AGGREGATION
# =============================================================================

aggregate_to_school_year_race <- function(df) {
  # Aggregate reason-level data to school-year-race level
  # This ensures we have one observation per school-year-race combination
  # and properly handles clustering

  message("\n>>> Aggregating to school-year-race level...")
  message(">>> Initial rows: ", format_number(nrow(df)))

  # Identify grouping variables (school, year, race)
  group_vars <- c("cds_school", "academic_year", "student_group")

  # For each school-year-race, we want to:
  # 1. Sum total suspensions (across all reasons)
  # 2. Take enrollment (should be same across reasons)
  # 3. Recalculate suspension rate
  # 4. Take first value of school-level variables (teacher diversity, charter, etc.)

  # Identify numeric columns to sum (suspensions)
  suspension_cols <- grep("^total_suspensions", names(df), value = TRUE)

  # Identify columns to take first value (should be constant within group)
  constant_cols <- c(
    "cumulative_enrollment",
    grep("^teacher_", names(df), value = TRUE),
    grep("^charter_", names(df), value = TRUE),
    grep("^is_", names(df), value = TRUE),
    grep("level", names(df), value = TRUE, ignore.case = TRUE),
    grep("sed", names(df), value = TRUE, ignore.case = TRUE)
  )
  constant_cols <- unique(constant_cols)
  constant_cols <- intersect(constant_cols, names(df))

  # Build aggregation expression
  agg_df <- df %>%
    group_by(across(all_of(group_vars))) %>%
    summarise(
      # Sum suspensions
      across(any_of(suspension_cols), ~sum(.x, na.rm = TRUE)),

      # Take first value of constant columns
      across(any_of(constant_cols), ~first(.x)),

      # Count how many reason-level rows were aggregated
      n_reasons_aggregated = n(),

      .groups = "drop"
    )

  # Recalculate suspension rate if we have both suspensions and enrollment
  if ("total_suspensions" %in% names(agg_df) && "cumulative_enrollment" %in% names(agg_df)) {
    agg_df <- agg_df %>%
      mutate(
        suspension_rate_percent_total = safe_div(total_suspensions, cumulative_enrollment) * 100
      )
  }

  message(">>> Aggregated rows: ", format_number(nrow(agg_df)))
  message(">>> Average reasons per school-year-race: ",
          round(nrow(df) / nrow(agg_df), 1))

  # Verify aggregation worked
  if (nrow(agg_df) >= nrow(df)) {
    warning("Aggregation did not reduce rows - check grouping variables")
  }

  return(agg_df)
}

# =============================================================================
# DATA LOADING
# =============================================================================

load_features <- function() {
  if (!file.exists(TEACHER_PATH)) {
    stop("\n",
         "════════════════════════════════════════════════════════════════\n",
         "❌ MISSING FILE: ", TEACHER_PATH, "\n",
         "════════════════════════════════════════════════════════════════\n",
         "\n",
         "This file contains teacher race/ethnicity data merged with student\n",
         "suspension data. To create it, run:\n",
         "\n",
         "  source('Analysis/22_build_teacher_race_shares.R')\n",
         "\n",
         "This script will:\n",
         "  1. Load teacher demographics (teacher_staff_long.parquet)\n",
         "  2. Compute race and gender shares\n",
         "  3. Merge with student suspension data\n",
         "  4. Create the required file\n",
         "\n",
         "════════════════════════════════════════════════════════════════\n")
  }
  
  message("\n════════════════════════════════════════════════════════════════")
  message("📊 Loading Teacher-Student Merged Data")
  message("════════════════════════════════════════════════════════════════\n")
  message("File: ", basename(TEACHER_PATH))
  
  df <- as.data.frame(arrow::read_parquet(TEACHER_PATH))
  
  message("Dimensions: ", format_number(nrow(df)), " rows × ", ncol(df), " columns")
  
  # Check for student_group column and canonicalize race codes
  if ("student_group" %in% names(df)) {
    # Canonicalize the student_group column (converts CDE codes like RA, RB to full labels)
    df$student_group <- canonicalize_race_label(df$student_group)
    groups <- sort(unique(df$student_group[!is.na(df$student_group)]))
    message("Student groups: ", paste(groups, collapse = ", "))
  } else if ("reporting_category" %in% names(df)) {
    df$student_group <- canonicalize_race_label(df$reporting_category)
    groups <- sort(unique(df$student_group[!is.na(df$student_group)]))
    message("Student groups (from reporting_category): ", paste(groups, collapse = ", "))
  } else {
    message("⚠ No student_group column found - will run aggregate analysis only")
    df$student_group <- "All Students"
  }
  
  # Check for suspension outcomes
  outcome_cols <- intersect(
    c("suspension_rate_percent_total", "susp_all_rate", "total_suspensions", "cumulative_enrollment"),
    names(df)
  )
  message("Suspension outcome columns: ", paste(outcome_cols, collapse = ", "))
  
  # Check for teacher columns
  teacher_cols <- grep("^teacher_", names(df), value = TRUE)
  message("Teacher columns: ", length(teacher_cols), " found")
  
  race_share_cols <- grep("_(african_american|asian|hispanic|white|filipino).*_share",
                          teacher_cols, value = TRUE, ignore.case = TRUE)
  message("  - Teacher race share columns: ", length(race_share_cols))
  
  gender_share_cols <- grep("_gender_.*_share", teacher_cols, value = TRUE, ignore.case = TRUE)
  message("  - Teacher gender share columns: ", length(gender_share_cols))
  
  message("\n════════════════════════════════════════════════════════════════\n")
  
  list(data = df, source = basename(TEACHER_PATH))
}

# =============================================================================
# REGRESSION PREPARATION
# =============================================================================

prepare_regression_frame <- function(df, student_group = NULL) {
  if (!is.null(student_group)) {
    if (!"student_group" %in% names(df)) {
      return(NULL)
    }
    df <- df[df$student_group == student_group & !is.na(df$student_group), , drop = FALSE]
    if (!nrow(df)) {
      return(NULL)
    }
    message("\n────────────────────────────────────────────────────────────────")
    message("📌 Student Group: ", student_group)
    message("────────────────────────────────────────────────────────────────")
  }
  
  # Extract diversity measures - PRIORITIZE RACE
  teacher_race <- extract_teacher_race_nonwhite_share(df, prefix = "^teacher")
  admin_race <- extract_admin_race_nonwhite_share(df)
  
  # Diagnostic messages
  message("\n", describe_diversity_source(teacher_race$meta, "Teacher"))
  message(describe_diversity_source(admin_race$meta, "Administrator"))
  
  # CRITICAL CHECK: Ensure we're using RACE, not gender
  if (is.null(teacher_race) || is.null(admin_race)) {
    message("\n❌ FATAL: Missing teacher/administrator RACE diversity columns!")
    message("   The regression CANNOT proceed without racial diversity data.")
    message("   Run Analysis/22_build_teacher_race_shares.R to create the required columns.")
    return(NULL)
  }
  
  if (teacher_race$meta$type != "race_share" || admin_race$meta$type != "race_share") {
    message("\n❌ FATAL: Detected non-race diversity measures!")
    message("   Teacher type: ", teacher_race$meta$type)
    message("   Admin type: ", admin_race$meta$type)
    return(NULL)
  }
  
  message("\n✓ Confirmed: Using RACIAL diversity for both teachers and administrators\n")
  
  # Get suspension rate
  outcome_col <- intersect(
    c("suspension_rate_percent_total", "susp_all_rate"),
    names(df)
  )[1]
  
  if (is.na(outcome_col)) {
    message("❌ No suspension rate column found")
    return(NULL)
  }
  
  divisor <- if (grepl("percent", outcome_col)) 100 else 1
  suspension_rate <- suppressWarnings(as.numeric(df[[outcome_col]]) / divisor)
  
  # Build model data frame
  model_df <- data.frame(
    suspension_rate = suspension_rate,
    teacher_non_white_share = as.numeric(teacher_race$values),
    admin_non_white_share = as.numeric(admin_race$values),
    stringsAsFactors = FALSE
  )
  
  # Add controls
  sed_cols <- grep("sed_rate|economic", names(df), value = TRUE)
  if (length(sed_cols)) {
    model_df$sed_rate <- suppressWarnings(as.numeric(df[[sed_cols[1]]]))
  }
  
  charter_cols <- intersect(c("charter_yn_std", "charter_yn", "is_traditional"), names(df))
  if (length(charter_cols)) {
    charter_vec <- df[[charter_cols[1]]]
    if (is.logical(charter_vec)) {
      model_df$is_charter <- as.integer(charter_vec)
    } else {
      charter_str <- tolower(as.character(charter_vec))
      model_df$is_charter <- as.integer(charter_str %in% c("y", "yes", "charter", "true", "1"))
    }
  }
  
  grade_cols <- intersect(
    c("level_strict3", "school_level_final", "school_type"),
    names(df)
  )
  if (length(grade_cols)) {
    model_df$grade_level <- factor(df[[grade_cols[1]]])
  }
  
  enrollment_cols <- intersect(
    c("cumulative_enrollment", "sup_cumulative_enrollment"),
    names(df)
  )
  if (length(enrollment_cols)) {
    model_df$enrollment <- suppressWarnings(as.numeric(df[[enrollment_cols[1]]]))
  }
  
  # Filter complete cases
  keep <- !is.na(model_df$suspension_rate) &
    !is.na(model_df$teacher_non_white_share) &
    !is.na(model_df$admin_non_white_share)
  
  model_df <- model_df[keep, , drop = FALSE]
  
  if ("enrollment" %in% names(model_df)) {
    positive <- !is.na(model_df$enrollment) & model_df$enrollment > 0
    model_df <- model_df[positive, , drop = FALSE]
    model_df$weights <- model_df$enrollment
    small_n <- sum(model_df$enrollment < 50, na.rm = TRUE)
    message("Schools with <50 students: ", format_number(small_n), " / ",
            format_number(nrow(model_df)), " (",
            round(100 * small_n / nrow(model_df), 1), "%)")
  } else {
    model_df$weights <- 1
  }
  
  if (!nrow(model_df)) {
    message("❌ No complete observations after filtering")
    return(NULL)
  }

  message("Final sample size: ", format_number(nrow(model_df)),
          " school-year-race combinations")
  
  list(
    data = model_df,
    diversity_meta = list(teacher = teacher_race$meta, administrator = admin_race$meta),
    student_group = student_group
  )
}

# =============================================================================
# RESULTS EXTRACTION AND INTERPRETATION
# =============================================================================

extract_regression_results <- function(fit, model_info) {
  # Extract key coefficients and statistics from regression model

  group_label <- model_info$student_group %||% "All Students"
  coef_mat <- summary(fit)$coefficients
  conf <- suppressWarnings(confint(fit))
  s <- summary(fit)

  # Extract teacher diversity results
  teacher_results <- if ("teacher_non_white_share" %in% rownames(coef_mat)) {
    list(
      coefficient = coef_mat["teacher_non_white_share", "Estimate"],
      std_error = coef_mat["teacher_non_white_share", "Std. Error"],
      p_value = coef_mat["teacher_non_white_share", "Pr(>|t|)"],
      ci_lower = conf["teacher_non_white_share", 1],
      ci_upper = conf["teacher_non_white_share", 2]
    )
  } else {
    list(coefficient = NA, std_error = NA, p_value = NA, ci_lower = NA, ci_upper = NA)
  }

  # Extract admin diversity results
  admin_results <- if ("admin_non_white_share" %in% rownames(coef_mat)) {
    list(
      coefficient = coef_mat["admin_non_white_share", "Estimate"],
      std_error = coef_mat["admin_non_white_share", "Std. Error"],
      p_value = coef_mat["admin_non_white_share", "Pr(>|t|)"],
      ci_lower = conf["admin_non_white_share", 1],
      ci_upper = conf["admin_non_white_share", 2]
    )
  } else {
    list(coefficient = NA, std_error = NA, p_value = NA, ci_lower = NA, ci_upper = NA)
  }

  data.frame(
    student_group = group_label,
    n_observations = stats::nobs(fit),  # school-year-race combinations
    r_squared = s$r.squared,
    adj_r_squared = s$adj.r.squared,

    teacher_coefficient = teacher_results$coefficient,
    teacher_std_error = teacher_results$std_error,
    teacher_p_value = teacher_results$p_value,
    teacher_ci_lower = teacher_results$ci_lower,
    teacher_ci_upper = teacher_results$ci_upper,

    admin_coefficient = admin_results$coefficient,
    admin_std_error = admin_results$std_error,
    admin_p_value = admin_results$p_value,
    admin_ci_lower = admin_results$ci_lower,
    admin_ci_upper = admin_results$ci_upper,

    stringsAsFactors = FALSE
  )
}

calculate_practical_effects <- function(results_df) {
  # Calculate practical interpretations of effect sizes
  # Effect of 10 percentage point increase in diversity (0.10)

  results_df %>%
    mutate(
      # Convert to percentage point change in suspension rate for 10pp diversity increase
      teacher_effect_10pp = teacher_coefficient * 0.10 * 100,
      teacher_effect_10pp_lower = teacher_ci_lower * 0.10 * 100,
      teacher_effect_10pp_upper = teacher_ci_upper * 0.10 * 100,

      admin_effect_10pp = admin_coefficient * 0.10 * 100,
      admin_effect_10pp_lower = admin_ci_lower * 0.10 * 100,
      admin_effect_10pp_upper = admin_ci_upper * 0.10 * 100,

      # Significance indicators
      teacher_sig = case_when(
        teacher_p_value < 0.001 ~ "***",
        teacher_p_value < 0.01 ~ "**",
        teacher_p_value < 0.05 ~ "*",
        TRUE ~ ""
      ),
      admin_sig = case_when(
        admin_p_value < 0.001 ~ "***",
        admin_p_value < 0.01 ~ "**",
        admin_p_value < 0.05 ~ "*",
        TRUE ~ ""
      ),

      # Direction labels
      teacher_direction = case_when(
        teacher_p_value >= 0.05 ~ "No significant effect",
        teacher_coefficient < 0 ~ "Lower suspension rates",
        teacher_coefficient > 0 ~ "Higher suspension rates",
        TRUE ~ "No significant effect"
      ),
      admin_direction = case_when(
        admin_p_value >= 0.05 ~ "No significant effect",
        admin_coefficient < 0 ~ "Lower suspension rates",
        admin_coefficient > 0 ~ "Higher suspension rates",
        TRUE ~ "No significant effect"
      )
    )
}

generate_interpretation_text <- function(results_df) {
  # Generate plain-language interpretations for each student group

  interpretations <- lapply(1:nrow(results_df), function(i) {
    row <- results_df[i, ]

    # Teacher interpretation
    teacher_text <- if (row$teacher_p_value >= 0.05) {
      sprintf(
        "Teacher diversity shows NO statistically significant association with suspension rates (p=%.3f).",
        row$teacher_p_value
      )
    } else {
      direction <- ifelse(row$teacher_coefficient < 0, "DECREASE", "INCREASE")
      abs_effect <- abs(row$teacher_effect_10pp)

      sprintf(
        "A 10 percentage point increase in teacher diversity (e.g., from 40%% to 50%% non-white teachers) is associated with a %.3f percentage point %s in suspension rates (95%% CI: %.3f to %.3f, p<%.3f). This is a %s but statistically significant effect.",
        abs_effect,
        direction,
        abs(row$teacher_effect_10pp_lower),
        abs(row$teacher_effect_10pp_upper),
        row$teacher_p_value,
        ifelse(abs_effect < 0.1, "VERY SMALL", ifelse(abs_effect < 0.5, "SMALL", "MODERATE"))
      )
    }

    # Admin interpretation
    admin_text <- if (row$admin_p_value >= 0.05) {
      sprintf(
        "Administrator diversity shows NO statistically significant association with suspension rates (p=%.3f).",
        row$admin_p_value
      )
    } else {
      direction <- ifelse(row$admin_coefficient < 0, "DECREASE", "INCREASE")
      abs_effect <- abs(row$admin_effect_10pp)

      sprintf(
        "A 10 percentage point increase in administrator diversity is associated with a %.3f percentage point %s in suspension rates (95%% CI: %.3f to %.3f, p<%.3f). This is a %s but statistically significant effect.",
        abs_effect,
        direction,
        abs(row$admin_effect_10pp_lower),
        abs(row$admin_effect_10pp_upper),
        row$admin_p_value,
        ifelse(abs_effect < 0.1, "VERY SMALL", ifelse(abs_effect < 0.5, "SMALL", "MODERATE"))
      )
    }

    # Practical example
    example_text <- if (row$teacher_p_value < 0.05) {
      baseline_rate <- 5.0  # Assume 5% baseline suspension rate
      new_rate <- baseline_rate + row$teacher_effect_10pp

      sprintf(
        "\nPRACTICAL EXAMPLE: In a school where %s students have a 5%% suspension rate, increasing teacher diversity from 40%% to 50%% non-white would be associated with a suspension rate of approximately %.2f%% (a change of %.2f%%).",
        row$student_group,
        new_rate,
        row$teacher_effect_10pp
      )
    } else {
      ""
    }

    data.frame(
      student_group = row$student_group,
      teacher_interpretation = teacher_text,
      admin_interpretation = admin_text,
      practical_example = example_text,
      stringsAsFactors = FALSE
    )
  })

  bind_rows(interpretations)
}

create_coefficient_plot <- function(results_df, output_dir) {
  # Create forest plot showing coefficients with confidence intervals

  # Prepare data for plotting
  plot_data <- results_df %>%
    select(student_group, teacher_coefficient, teacher_ci_lower, teacher_ci_upper,
           admin_coefficient, admin_ci_lower, admin_ci_upper,
           teacher_p_value, admin_p_value) %>%
    tidyr::pivot_longer(
      cols = c(teacher_coefficient, admin_coefficient),
      names_to = "variable",
      values_to = "coefficient"
    ) %>%
    mutate(
      ci_lower = ifelse(variable == "teacher_coefficient", teacher_ci_lower, admin_ci_lower),
      ci_upper = ifelse(variable == "teacher_coefficient", teacher_ci_upper, admin_ci_upper),
      p_value = ifelse(variable == "teacher_coefficient", teacher_p_value, admin_p_value),
      significant = p_value < 0.05,
      variable_label = ifelse(variable == "teacher_coefficient",
                             "Teacher Diversity",
                             "Administrator Diversity")
    )

  # Create forest plot
  p <- ggplot(plot_data, aes(x = coefficient, y = student_group, color = variable_label)) +
    geom_vline(xintercept = 0, linetype = "dashed", color = "gray40") +
    geom_errorbarh(aes(xmin = ci_lower, xmax = ci_upper),
                   height = 0.2, position = position_dodge(width = 0.5)) +
    geom_point(aes(shape = significant, size = significant),
               position = position_dodge(width = 0.5)) +
    scale_shape_manual(values = c("TRUE" = 16, "FALSE" = 1),
                      labels = c("TRUE" = "p < 0.05", "FALSE" = "Not significant")) +
    scale_size_manual(values = c("TRUE" = 3, "FALSE" = 2),
                     labels = c("TRUE" = "p < 0.05", "FALSE" = "Not significant")) +
    scale_color_manual(values = c("Teacher Diversity" = "#2E86AB",
                                  "Administrator Diversity" = "#A23B72")) +
    labs(
      title = "Association Between Staff Racial Diversity and Student Suspension Rates",
      subtitle = "Coefficients with 95% Confidence Intervals",
      x = "Coefficient (change in suspension rate per 1-unit increase in diversity)",
      y = "Student Racial/Ethnic Group",
      color = "Staff Type",
      shape = "Significance",
      size = "Significance",
      caption = "Note: Negative coefficients indicate lower suspension rates with more diverse staff.\nWeighted by student enrollment. Controls: charter status, school level."
    ) +
    theme_minimal(base_size = 11) +
    theme(
      legend.position = "bottom",
      plot.title = element_text(face = "bold", size = 13),
      plot.caption = element_text(hjust = 0, size = 8, color = "gray40")
    )

  ggsave(
    file.path(output_dir, "teacher_diversity_coefficients_forest_plot.png"),
    p, width = 10, height = 7, dpi = 300
  )

  message("✓ Saved forest plot: teacher_diversity_coefficients_forest_plot.png")

  invisible(p)
}

create_practical_effects_plot <- function(results_df, output_dir) {
  # Create bar chart showing practical effect sizes

  plot_data <- results_df %>%
    filter(teacher_p_value < 0.05 | admin_p_value < 0.05) %>%
    select(student_group, teacher_effect_10pp, admin_effect_10pp,
           teacher_p_value, admin_p_value) %>%
    tidyr::pivot_longer(
      cols = c(teacher_effect_10pp, admin_effect_10pp),
      names_to = "variable",
      values_to = "effect"
    ) %>%
    mutate(
      variable_label = ifelse(variable == "teacher_effect_10pp",
                             "Teacher Diversity",
                             "Administrator Diversity"),
      p_value = ifelse(variable == "teacher_effect_10pp", teacher_p_value, admin_p_value),
      significant = p_value < 0.05
    ) %>%
    filter(significant)

  if (nrow(plot_data) == 0) {
    message("⚠ No significant effects to plot")
    return(invisible(NULL))
  }

  p <- ggplot(plot_data, aes(x = reorder(student_group, effect), y = effect, fill = variable_label)) +
    geom_hline(yintercept = 0, linetype = "solid", color = "gray40") +
    geom_col(position = position_dodge(width = 0.8), width = 0.7) +
    geom_text(aes(label = sprintf("%.3f", effect), hjust = ifelse(effect < 0, 1.1, -0.1)),
              position = position_dodge(width = 0.8),
              size = 3) +
    scale_fill_manual(values = c("Teacher Diversity" = "#2E86AB",
                                "Administrator Diversity" = "#A23B72")) +
    coord_flip() +
    labs(
      title = "Practical Effects of Staff Racial Diversity on Suspension Rates",
      subtitle = "Change in suspension rate for 10 percentage point increase in staff diversity (e.g., 40% → 50% non-white)",
      x = "Student Racial/Ethnic Group",
      y = "Change in Suspension Rate (percentage points)",
      fill = "Staff Type",
      caption = "Note: Only statistically significant effects shown (p < 0.05).\nNegative values = lower suspension rates with more diverse staff."
    ) +
    theme_minimal(base_size = 11) +
    theme(
      legend.position = "bottom",
      plot.title = element_text(face = "bold", size = 13),
      plot.caption = element_text(hjust = 0, size = 8, color = "gray40")
    )

  ggsave(
    file.path(output_dir, "teacher_diversity_practical_effects.png"),
    p, width = 10, height = 6, dpi = 300
  )

  message("✓ Saved practical effects plot: teacher_diversity_practical_effects.png")

  invisible(p)
}

# =============================================================================
# REGRESSION EXECUTION
# =============================================================================

run_regression <- function(model_info) {
  model_df <- model_info$data
  
  predictors <- c("teacher_non_white_share", "admin_non_white_share")
  optional <- intersect(c("sed_rate", "is_charter"), names(model_df))
  predictors <- c(predictors, optional)
  
  if ("grade_level" %in% names(model_df)) {
    predictors <- c(predictors, "grade_level")
  }
  
  formula <- stats::as.formula(paste("suspension_rate ~", paste(predictors, collapse = " + ")))
  fit <- stats::lm(formula, data = model_df, weights = model_df$weights)
  
  group_label <- model_info$student_group %||% "All Students"
  
  message("\n════════════════════════════════════════════════════════════════")
  message("📈 REGRESSION RESULTS: ", group_label)
  message("════════════════════════════════════════════════════════════════\n")
  
  print(summary(fit))
  
  # Extract coefficients
  coef_mat <- summary(fit)$coefficients
  conf <- suppressWarnings(confint(fit))
  
  message("\n────────────────────────────────────────────────────────────────")
  message("🔍 KEY COEFFICIENTS (with 95% CI)")
  message("────────────────────────────────────────────────────────────────\n")
  
  for (var in c("teacher_non_white_share", "admin_non_white_share", "sed_rate")) {
    if (var %in% rownames(coef_mat)) {
      coef <- coef_mat[var, "Estimate"]
      se <- coef_mat[var, "Std. Error"]
      pval <- coef_mat[var, "Pr(>|t|)"]
      ci_low <- conf[var, 1]
      ci_high <- conf[var, 2]
      
      sig <- if (pval < 0.001) "***" else if (pval < 0.01) "**" else if (pval < 0.05) "*" else ""
      
      message(sprintf("%-25s: %8.6f  [%8.6f, %8.6f]  p=%6.4f %s",
                      var, coef, ci_low, ci_high, pval, sig))
    }
  }
  
  s <- summary(fit)
  message("\n────────────────────────────────────────────────────────────────")
  message(sprintf("R² = %.4f  |  Adj. R² = %.4f  |  N = %s",
                  s$r.squared, s$adj.r.squared, format_number(stats::nobs(fit))))
  message("────────────────────────────────────────────────────────────────\n")
  
  invisible(fit)
}

# =============================================================================
# MAIN EXECUTION
# =============================================================================

main <- function() {
  message("\n")
  message("╔════════════════════════════════════════════════════════════════╗")
  message("║                                                                ║")
  message("║     TEACHER/ADMINISTRATOR RACIAL DIVERSITY ANALYSIS           ║")
  message("║     Association with Student Suspension Rates                 ║")
  message("║                                                                ║")
  message("╚════════════════════════════════════════════════════════════════╝")

  result <- load_features()
  df <- result$data

  # CRITICAL: Aggregate to school-year-race level to avoid clustering issues
  # This reduces observations from school-year-race-reason to school-year-race
  df <- aggregate_to_school_year_race(df)

  # Prepare regressions for each student group
  groups <- if ("student_group" %in% names(df)) {
    unique(df$student_group[!is.na(df$student_group)])
  } else {
    NULL
  }

  # Storage for results
  regression_fits <- list()
  regression_summaries <- list()
  model_infos <- list()

  if (!is.null(groups)) {
    # Prioritize important groups
    ordered <- intersect(ALLOWED_RACE_GROUPS, groups)

    for (group in ordered) {
      model_info <- prepare_regression_frame(df, student_group = group)
      if (!is.null(model_info)) {
        fit <- run_regression(model_info)
        regression_fits[[group]] <- fit
        model_infos[[group]] <- model_info

        # Extract results for this group
        regression_summaries[[group]] <- extract_regression_results(fit, model_info)
      }
    }

  } else {
    # Aggregate analysis only
    model_info <- prepare_regression_frame(df, student_group = NULL)
    if (!is.null(model_info)) {
      fit <- run_regression(model_info)
      regression_fits[["All Students"]] <- fit
      model_infos[["All Students"]] <- model_info
      regression_summaries[["All Students"]] <- extract_regression_results(fit, model_info)
    }
  }

  # Compile results into data frames
  if (length(regression_summaries) > 0) {
    message("\n╔════════════════════════════════════════════════════════════════╗")
    message("║           GENERATING TABLES AND VISUALIZATIONS                ║")
    message("╚════════════════════════════════════════════════════════════════╝\n")

    # Combine all results
    combined_results <- bind_rows(regression_summaries)

    # Calculate practical effects
    practical_results <- calculate_practical_effects(combined_results)

    # Generate interpretations
    interpretations <- generate_interpretation_text(practical_results)

    # Create output directory
    output_dir <- here("outputs", "teacher_diversity_analysis")
    if (!dir.exists(output_dir)) {
      dir.create(output_dir, recursive = TRUE)
    }

    # Save summary table (Excel)
    summary_table <- practical_results %>%
      select(
        student_group,
        n_observations,
        r_squared,
        adj_r_squared,
        teacher_coefficient,
        teacher_ci_lower,
        teacher_ci_upper,
        teacher_p_value,
        teacher_sig,
        teacher_direction,
        teacher_effect_10pp,
        admin_coefficient,
        admin_ci_lower,
        admin_ci_upper,
        admin_p_value,
        admin_sig,
        admin_direction,
        admin_effect_10pp
      )

    write_xlsx(
      list(
        "Summary" = summary_table,
        "Interpretations" = interpretations,
        "Technical_Details" = combined_results
      ),
      path = file.path(output_dir, "teacher_diversity_regression_results.xlsx")
    )
    message("✓ Saved Excel summary: teacher_diversity_regression_results.xlsx")

    # Save CSV versions
    write.csv(
      summary_table,
      file.path(output_dir, "teacher_diversity_summary.csv"),
      row.names = FALSE
    )
    message("✓ Saved CSV summary: teacher_diversity_summary.csv")

    write.csv(
      interpretations,
      file.path(output_dir, "teacher_diversity_interpretations.csv"),
      row.names = FALSE
    )
    message("✓ Saved interpretations: teacher_diversity_interpretations.csv")

    # Create visualizations
    create_coefficient_plot(practical_results, output_dir)
    create_practical_effects_plot(practical_results, output_dir)

    # Print summary to console
    message("\n╔════════════════════════════════════════════════════════════════╗")
    message("║                    SUMMARY OF KEY FINDINGS                     ║")
    message("╚════════════════════════════════════════════════════════════════╝\n")

    for (i in 1:nrow(interpretations)) {
      row <- interpretations[i, ]
      message("\n", strrep("─", 64))
      message("📊 ", row$student_group)
      message(strrep("─", 64))
      message("\nTEACHER DIVERSITY:")
      message(strwrap(row$teacher_interpretation, width = 64, prefix = "  "), sep = "\n")
      message("\nADMINISTRATOR DIVERSITY:")
      message(strwrap(row$admin_interpretation, width = 64, prefix = "  "), sep = "\n")
      if (nchar(row$practical_example) > 0) {
        message(strwrap(row$practical_example, width = 64, prefix = "  "), sep = "\n")
      }
    }
  }

  message("\n╔════════════════════════════════════════════════════════════════╗")
  message("║                       ANALYSIS COMPLETE                        ║")
  message("╚════════════════════════════════════════════════════════════════╝\n")

  message("📁 Output files saved to: ", output_dir)
  message("\n⚠️  IMPORTANT REMINDERS:")
  message("  • These are ASSOCIATIONS, not causal effects")
  message("  • Results describe correlations in observational data")
  message("  • Do not interpret coefficients as causal impacts")
  message("  • Multiple comparisons: consider adjusting significance thresholds")
  message("\n📊 METHODOLOGICAL NOTE:")
  message("  • Data aggregated to school-year-race level before regression")
  message("  • This properly handles clustering (multiple reasons per school)")
  message("  • N = unique school-year-race combinations")
  message("  • Standard errors are now appropriate for the unit of analysis\n")

  invisible(list(
    fits = regression_fits,
    results = practical_results,
    interpretations = interpretations
  ))
}

# =============================================================================
# RUN ANALYSIS
# =============================================================================

# Execute the analysis when script is sourced
# To load functions without running, use: options(teacher_regression_skip_main = TRUE)
if (!isTRUE(getOption("teacher_regression_skip_main"))) {
  main()
}