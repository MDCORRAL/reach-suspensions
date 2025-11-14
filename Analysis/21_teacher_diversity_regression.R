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
  "pacific_islander",
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
  
  message("Final sample size: ", format_number(nrow(model_df)), " schools")
  
  list(
    data = model_df,
    diversity_meta = list(teacher = teacher_race$meta, administrator = admin_race$meta),
    student_group = student_group
  )
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
  
  # Prepare regressions for each student group
  groups <- if ("student_group" %in% names(df)) {
    unique(df$student_group[!is.na(df$student_group)])
  } else {
    NULL
  }
  
  if (!is.null(groups)) {
    # Prioritize important groups
    ordered <- intersect(ALLOWED_RACE_GROUPS, groups)
    results <- list()
    
    for (group in ordered) {
      model_info <- prepare_regression_frame(df, student_group = group)
      if (!is.null(model_info)) {
        fit <- run_regression(model_info)
        results[[group]] <- fit
      }
    }
    
  } else {
    # Aggregate analysis only
    model_info <- prepare_regression_frame(df, student_group = NULL)
    if (!is.null(model_info)) {
      results <- list(run_regression(model_info))
    } else {
      results <- list()
    }
  }
  
  message("\n╔════════════════════════════════════════════════════════════════╗")
  message("║                       ANALYSIS COMPLETE                        ║")
  message("╚════════════════════════════════════════════════════════════════╝\n")
  
  message("⚠️  IMPORTANT REMINDERS:")
  message("  • These are ASSOCIATIONS, not causal effects")
  message("  • Results describe correlations in observational data")
  message("  • Do not interpret coefficients as causal impacts")
  message("  • Multiple comparisons: consider adjusting significance thresholds\n")
  
  invisible(results)
}

# Run if called directly
if (identical(environment(), globalenv()) &&
    !isTRUE(getOption("teacher_regression_skip_main"))) {
  main()
}