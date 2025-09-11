# 17_tail_by_grade-school_concentration_analysis.R
# Enhanced tail concentration analysis with grade level and school type stratification
# Computes Pareto shares, Lorenz/Gini, rate outliers by grade × setting combinations
# Designed for REACH Network suspension data analysis


suppressPackageStartupMessages({
  library(dplyr); library(stringr); library(forcats); library(janitor)
  library(arrow); library(here); library(ggplot2); library(openxlsx)
  library(scales); library(tidyr); library(purrr); library(readr)
  library(ineq)    # for Lorenz and Gini calculations
})

## -------------------------------------------------------------------------
## Configuration
## -------------------------------------------------------------------------

# Data paths - updated for your repository structure
DATA_STAGE <- here("data-stage")
INPUT_PATH <- file.path(DATA_STAGE, "susp_v5.parquet")  # Main suspension data
V6F_PARQ   <- file.path(DATA_STAGE, "susp_v6_features.parquet")  # School features

# Output directory
RUN_TAG <- format(Sys.time(), "%Y%m%d_%H%M")
OUT_DIR <- here("outputs", paste0("tail_by_grade_school_", RUN_TAG))
if (!dir.exists(OUT_DIR)) dir.create(OUT_DIR, recursive = TRUE)

# Updated column mappings for your data structure
cols <- list(
  school_id   = "school_code",
  school_name = "school_name", 
  year        = "academic_year",
  setting     = "school_type",                    # Traditional vs Other
  level       = "school_level_final",             # Grade level classification
  enrollment  = "cumulative_enrollment",
  total_susp  = "total_suspensions",
  undup_susp  = "unduplicated_count_of_students_suspended_total"
)

# Choose measure for concentration analysis
MEASURE <- "total_susp"

# Analysis parameters
TOP_PCT <- c(0.05, 0.10, 0.20)
RATE_PCTS <- c(0.90, 0.95)
RATE_PER <- 100

## -------------------------------------------------------------------------
## Helper Functions
## -------------------------------------------------------------------------

safe_div <- function(n, d) ifelse(is.na(d) | d == 0, NA_real_, n / d)

# Enhanced school type mapping
map_setting <- function(x, is_traditional = NULL) {
  if (!is.null(is_traditional)) {
    return(ifelse(is_traditional %in% c(TRUE, 1, "TRUE", "true"), 
                  "Traditional", "Non-traditional"))
  }
  
  x_clean <- str_to_lower(str_trim(as.character(x)))
  case_when(
    is.na(x) ~ "Unknown",
    str_detect(x_clean, "traditional") ~ "Traditional",
    str_detect(x_clean, "alternative|charter|continuation|community") ~ "Non-traditional",
    TRUE ~ "Other"
  )
}

# Enhanced grade level mapping
map_grade_level <- function(x) {
  x_clean <- str_to_lower(str_trim(as.character(x)))
  case_when(
    is.na(x) ~ "Unknown",
    str_detect(x_clean, "elementary|elem|primary|k.*5|k.*6") ~ "Elementary",
    str_detect(x_clean, "middle|junior|intermediate|6.*8|7.*8") ~ "Middle", 
    str_detect(x_clean, "high|secondary|9.*12|senior") ~ "High",
    str_detect(x_clean, "k.*12|ungraded|mixed|span") ~ "K-12/Mixed",
    str_detect(x_clean, "adult|continuation") ~ "Adult/Alternative",
    !is.na(x_clean) ~ "Other",
    TRUE ~ "Unknown"
  )
}

# Extract numeric year from academic year string
extract_year <- function(x) {
  year_match <- str_extract(x, "^\\d{4}")
  as.integer(year_match)
}

# Pareto shares calculation
pareto_shares <- function(df, top_ps = c(0.05, 0.10, 0.20)) {
  n_schools <- n_distinct(df$school_id)
  if (n_schools == 0) return(tibble())
  
  df_sorted <- df %>%
    group_by(school_id, school_name, enrollment) %>%
    summarise(measure = sum(measure, na.rm = TRUE), .groups = "drop") %>%
    arrange(desc(measure)) %>%
    mutate(
      rank          = row_number(),
      cum_measure   = cumsum(measure),
      total_measure = sum(measure, na.rm = TRUE),
      share_cum     = safe_div(cum_measure, total_measure)
    )
  
  map_dfr(top_ps, function(p) {
    cutoff_n <- max(1, floor(p * n_schools))
    top_share <- df_sorted %>%
      slice(1:cutoff_n) %>%
      summarise(
        top_schools   = cutoff_n,
        total_schools = n_schools,
        top_share     = safe_div(sum(measure, na.rm = TRUE), 
                                 df_sorted$total_measure[1])
      ) %>% 
      mutate(top_pct = p)
    top_share
  })
}

# Lorenz curve and Gini coefficient
lorenz_gini <- function(df) {
  x <- df %>%
    group_by(school_id) %>%
    summarise(measure = sum(measure, na.rm = TRUE), .groups = "drop") %>%
    pull(measure)
  
  x[is.na(x)] <- 0
  if (length(x) == 0 || sum(x) == 0) {
    return(list(curve = tibble(p = 0, L = 0), gini = NA_real_))
  }
  
  L <- ineq::Lc(x)
  g <- ineq::Gini(x)
  list(curve = tibble(p = L$p, L = L$L), gini = g)
}

# Grade/level-specific concentration analysis
analyze_by_grade_setting <- function(df, output_dir) {
  
  message("Running comprehensive grade × setting analysis...")
  
  # Create all combinations for comprehensive analysis
  grade_setting_analysis <- df %>%
    filter(!is.na(level), !is.na(setting), 
           level != "Unknown", setting != "Unknown") %>%
    group_by(year_num, level, setting) %>%
    group_modify(~{
      if(nrow(.x) < 5) {
        message("Skipping ", unique(.y$level), " × ", unique(.y$setting), 
                " in ", unique(.y$year_num), " (n=", nrow(.x), ")")
        return(tibble())
      }
      pareto_shares(.x, TOP_PCT)
    }) %>%
    ungroup() %>%
    mutate(measure_type = MEASURE)
  
  # Slide-ready output
  grade_setting_lines <- grade_setting_analysis %>%
    filter(!is.na(top_share)) %>%
    mutate(
      top_label = paste0("Top ", scales::percent(top_pct)),
      share_pct = scales::percent(top_share, accuracy = 0.1),
      slide_text = paste0(
        "In ", year_num, " (", level, " ", setting, " schools), ",
        top_label, " accounted for ", share_pct, " of suspension events."
      )
    )
  
  # Rate outliers by grade × setting
  grade_setting_outliers <- df %>%
    filter(!is.na(level), !is.na(setting), 
           level != "Unknown", setting != "Unknown") %>%
    group_by(year_num, level, setting) %>%
    filter(n() >= 5) %>% # Minimum group size
    mutate(
      rate_per100 = safe_div(measure, enrollment) * RATE_PER,
      p90 = quantile(rate_per100, 0.90, na.rm = TRUE),
      p95 = quantile(rate_per100, 0.95, na.rm = TRUE),
      flag_p90 = rate_per100 >= p90,
      flag_p95 = rate_per100 >= p95
    ) %>%
    arrange(year_num, level, setting, desc(rate_per100)) %>%
    ungroup()
  
  # Lorenz curves by grade × setting
  grade_setting_lorenz <- df %>%
    filter(!is.na(level), !is.na(setting), 
           level != "Unknown", setting != "Unknown") %>%
    group_by(year_num, level, setting) %>%
    group_modify(~{
      if(nrow(.x) < 5) return(tibble())
      lg_result <- lorenz_gini(.x)
      lg_result$curve %>% mutate(gini = lg_result$gini)
    }) %>%
    ungroup()
  
  # Save outputs
  write_csv(grade_setting_analysis, 
            file.path(output_dir, "pareto_by_grade_setting.csv"))
  write_csv(grade_setting_lines, 
            file.path(output_dir, "pareto_grade_setting_slide_ready.csv"))
  write_csv(grade_setting_outliers, 
            file.path(output_dir, "outliers_by_grade_setting.csv"))
  write_csv(grade_setting_lorenz, 
            file.path(output_dir, "lorenz_by_grade_setting.csv"))
  
  return(list(
    pareto = grade_setting_analysis,
    slides = grade_setting_lines,
    outliers = grade_setting_outliers,
    lorenz = grade_setting_lorenz
  ))
}

# Generate comparison tables
generate_comparison_tables <- function(df, output_dir) {
  
  # Compare traditional vs non-traditional by grade level
  comparison_by_grade <- df %>%
    filter(!is.na(level), !is.na(setting), 
           level %in% c("Elementary", "Middle", "High"),
           setting %in% c("Traditional", "Non-traditional")) %>%
    group_by(year_num, level, setting) %>%
    summarise(
      n_schools = n_distinct(school_id),
      total_enrollment = sum(enrollment, na.rm = TRUE),
      total_suspensions = sum(measure, na.rm = TRUE),
      mean_rate = mean(safe_div(measure, enrollment), na.rm = TRUE),
      median_rate = median(safe_div(measure, enrollment), na.rm = TRUE),
      q75_rate = quantile(safe_div(measure, enrollment), 0.75, na.rm = TRUE),
      q90_rate = quantile(safe_div(measure, enrollment), 0.90, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    mutate(
      overall_rate = safe_div(total_suspensions, total_enrollment),
      mean_rate_pct = scales::percent(mean_rate, accuracy = 0.1),
      median_rate_pct = scales::percent(median_rate, accuracy = 0.1),
      overall_rate_pct = scales::percent(overall_rate, accuracy = 0.1)
    )
  
  # Wide format for easy comparison
  comparison_wide <- comparison_by_grade %>%
    select(year_num, level, setting, n_schools, overall_rate_pct, 
           mean_rate_pct, median_rate_pct) %>%
    pivot_wider(
      names_from = setting,
      values_from = c(n_schools, overall_rate_pct, mean_rate_pct, median_rate_pct),
      names_sep = "_"
    )
  
  write_csv(comparison_by_grade, file.path(output_dir, "rates_by_grade_setting.csv"))
  write_csv(comparison_wide, file.path(output_dir, "rates_comparison_wide.csv"))
  
  return(list(
    detailed = comparison_by_grade,
    wide = comparison_wide
  ))
}

## -------------------------------------------------------------------------
## Data Loading and Preparation
## -------------------------------------------------------------------------

# Check required files exist
required_files <- c(INPUT_PATH)
missing_files <- required_files[!file.exists(required_files)]
if (length(missing_files) > 0) {
  stop("Missing required files: ", paste(missing_files, collapse = ", "))
}

# Load main suspension data
message("Loading suspension data from: ", INPUT_PATH)
dat0 <- read_parquet(INPUT_PATH) %>% clean_names()

# Check for required columns
req_cols <- unlist(cols[c("school_id", "year", "enrollment", "total_susp", "undup_susp")])
missing_cols <- setdiff(req_cols, names(dat0))
if (length(missing_cols) > 0) {
  stop("Missing required columns: ", paste(missing_cols, collapse = ", "))
}

# Process and clean data
dat <- dat0 %>%
  # Filter to Total/All Students records only
  filter(str_to_lower(reporting_category) %in% c("total", "all students", "ta")) %>%
  rename(
    school_id   = !!sym(cols$school_id),
    year        = !!sym(cols$year),
    enrollment  = !!sym(cols$enrollment),
    total_susp  = !!sym(cols$total_susp),
    undup_susp  = !!sym(cols$undup_susp)
  ) %>%
  mutate(
    year_num    = extract_year(year),
    enrollment  = as.numeric(enrollment),
    total_susp  = as.numeric(total_susp),
    undup_susp  = as.numeric(undup_susp),
    measure     = if (MEASURE == "undup_susp") undup_susp else total_susp,
    school_name = if ("school_name" %in% names(dat0)) school_name else school_id,
    level       = level_strict3  # Add this line
  ) %>%
  # Filter out invalid records
  filter(
    !is.na(year_num), 
    !is.na(enrollment), 
    !is.na(measure),
    enrollment > 0,
    measure >= 0
  )

# Add enhanced school features for traditional vs non-traditional classification
if (file.exists(V6F_PARQ)) {
  message("Loading and processing school features for traditional/non-traditional classification...")
  v6_features <- read_parquet(V6F_PARQ) %>% clean_names()
  
  # Check available columns
  available_cols <- intersect(c("school_code", "year", "is_traditional"), names(v6_features))
  message("Using feature columns: ", paste(available_cols, collapse = ", "))
  
  if ("is_traditional" %in% available_cols) {
    v6_features <- v6_features %>%
      select(all_of(available_cols)) %>%
      mutate(
        school_code = as.character(school_code),
        year_char = as.character(year)
      )
    
    # Join features and apply simple setting classification
    dat <- dat %>%
      left_join(
        v6_features, 
        by = c("school_id" = "school_code", "year" = "year_char")
      ) %>%
      mutate(
        setting = case_when(
          !is.na(is_traditional) & is_traditional == TRUE ~ "Traditional",
          !is.na(is_traditional) & is_traditional == FALSE ~ "Non-traditional",
          TRUE ~ "Unknown"
        )
      )
  } else {
    message("is_traditional column not found in features. Setting all to Unknown.")
    dat <- dat %>% mutate(setting = "Unknown")
  }
} else {
  message("School features file not found. Setting traditional/non-traditional to Unknown.")
  dat <- dat %>% mutate(setting = "Unknown")
}

message("Final dataset: ", nrow(dat), " school-year records")
message("Years covered: ", paste(sort(unique(dat$year_num)), collapse = ", "))

# Data quality summary
data_summary <- dat %>%
  count(year_num, level, setting, name = "n_schools") %>%
  arrange(year_num, level, setting)

message("\nData structure by year × level × setting:")
print(data_summary)

write_csv(data_summary, file.path(OUT_DIR, "data_structure_summary.csv"))

## -------------------------------------------------------------------------
## Analysis 1: Overall Pareto Shares by Year
## -------------------------------------------------------------------------

message("\nRunning overall Pareto analysis...")

ps_year <- dat %>%
  group_by(year_num) %>%
  summarise(
    pareto_results = list(pareto_shares(cur_data(), TOP_PCT)),
    .groups = "drop"
  ) %>%
  unnest(pareto_results) %>%
  mutate(measure_type = MEASURE)

# Create slide-ready output
ps_year_lines <- ps_year %>%
  mutate(
    top_label = paste0("Top ", scales::percent(top_pct)),
    share_pct = scales::percent(top_share, accuracy = 0.1)
  ) %>%
  select(year_num, top_label, share_pct, top_schools, total_schools, measure_type)

# Save results
write_csv(ps_year, file.path(OUT_DIR, "pareto_shares_by_year_raw.csv"))
write_csv(ps_year_lines, file.path(OUT_DIR, "pareto_shares_by_year_slide_ready.csv"))

## -------------------------------------------------------------------------
## Analysis 2: Enhanced Grade × Setting Stratified Analysis
## -------------------------------------------------------------------------

if (any(dat$level != "Unknown") && any(dat$setting != "Unknown")) {
  message("\nRunning enhanced grade × setting stratified analysis...")
  
  # Run comprehensive grade/setting analysis
  grade_results <- analyze_by_grade_setting(dat, OUT_DIR)
  
  # Generate comparison tables
  comparison_results <- generate_comparison_tables(dat, OUT_DIR)
  
  # Summary statistics by grade × setting
  grade_setting_summary <- dat %>%
    filter(!is.na(level), !is.na(setting), 
           level != "Unknown", setting != "Unknown") %>%
    group_by(year_num, level, setting) %>%
    summarise(
      n_schools = n_distinct(school_id),
      total_enrollment = sum(enrollment, na.rm = TRUE),
      total_suspensions = sum(measure, na.rm = TRUE),
      mean_rate = mean(safe_div(measure, enrollment), na.rm = TRUE),
      median_rate = median(safe_div(measure, enrollment), na.rm = TRUE),
      q75_rate = quantile(safe_div(measure, enrollment), 0.75, na.rm = TRUE),
      q90_rate = quantile(safe_div(measure, enrollment), 0.90, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    mutate(
      overall_rate = safe_div(total_suspensions, total_enrollment),
      mean_rate_pct = scales::percent(mean_rate, accuracy = 0.1),
      median_rate_pct = scales::percent(median_rate, accuracy = 0.1),
      overall_rate_pct = scales::percent(overall_rate, accuracy = 0.1)
    )
  
  write_csv(grade_setting_summary, 
            file.path(OUT_DIR, "summary_by_grade_setting.csv"))
  
  # Cross-tabulation for quick reference
  crosstab <- dat %>%
    filter(!is.na(level), !is.na(setting)) %>%
    count(level, setting, name = "n_school_years") %>%
    pivot_wider(names_from = setting, values_from = n_school_years, 
                values_fill = 0)
  
  write_csv(crosstab, file.path(OUT_DIR, "school_counts_by_grade_setting.csv"))
  
} else {
  message("Insufficient grade/setting data for stratified analysis.")
}

## -------------------------------------------------------------------------
## Analysis 3: Lorenz Curves and Gini Coefficients
## -------------------------------------------------------------------------

message("\nGenerating Lorenz curves and Gini coefficients...")

lg_year <- dat %>%
  group_by(year_num) %>%
  group_modify(~{
    lg_result <- lorenz_gini(.x)
    lg_result$curve %>% mutate(gini = lg_result$gini)
  }) %>%
  ungroup()

write_csv(lg_year, file.path(OUT_DIR, "lorenz_points_by_year_with_gini.csv"))

# Create Lorenz curve plot for most recent year
if (nrow(lg_year) > 0) {
  recent_year <- max(lg_year$year_num)
  
  p_lorenz <- lg_year %>%
    filter(year_num == recent_year) %>%
    ggplot(aes(x = p, y = L)) +
    geom_line(color = "blue", size = 1.2) +
    geom_abline(slope = 1, intercept = 0, linetype = "dashed", color = "red") +
    labs(
      title = paste("Lorenz Curve for", recent_year),
      subtitle = paste("Gini coefficient:", round(unique(lg_year$gini[lg_year$year_num == recent_year]), 3)),
      x = "Cumulative proportion of schools",
      y = "Cumulative proportion of suspensions"
    ) +
    theme_minimal() +
    coord_fixed()
  
  ggsave(file.path(OUT_DIR, paste0("lorenz_", recent_year, ".png")), 
         p_lorenz, width = 8, height = 6, dpi = 300)
}

## -------------------------------------------------------------------------
## Analysis 4: Overall Rate Outliers by Grade × Setting
## -------------------------------------------------------------------------

if (any(dat$level != "Unknown") && any(dat$setting != "Unknown")) {
  message("\nIdentifying rate outliers by grade × setting...")
  
  rates_overall <- dat %>%
    filter(!is.na(level), !is.na(setting), 
           level != "Unknown", setting != "Unknown") %>%
    group_by(year_num, school_id, school_name, level, setting) %>%
    summarise(
      enrollment = sum(enrollment, na.rm = TRUE),
      measure    = sum(measure, na.rm = TRUE),
      .groups    = "drop"
    ) %>%
    mutate(rate_per100 = safe_div(measure, enrollment) * RATE_PER)
  
  # Calculate thresholds by percentile within grade × setting
  thresh_overall <- rates_overall %>%
    group_by(year_num, level, setting) %>%
    summarise(
      p90 = quantile(rate_per100, probs = 0.90, na.rm = TRUE),
      p95 = quantile(rate_per100, probs = 0.95, na.rm = TRUE),
      n_schools = n(),
      .groups = "drop"
    )
  
  outliers_overall <- rates_overall %>%
    inner_join(thresh_overall, by = c("year_num", "level", "setting")) %>%
    mutate(
      flag_p90 = rate_per100 >= p90,
      flag_p95 = rate_per100 >= p95
    ) %>%
    arrange(year_num, level, setting, desc(rate_per100))
  
  write_csv(outliers_overall, file.path(OUT_DIR, "rate_outliers_by_year_level_setting.csv"))
  write_csv(thresh_overall, file.path(OUT_DIR, "rate_thresholds_by_grade_setting.csv"))
  
  # Summary counts
  outlier_summary <- outliers_overall %>%
    summarise(
      n_schools = n_distinct(school_id),
      n_p90     = sum(flag_p90, na.rm = TRUE),
      n_p95     = sum(flag_p95, na.rm = TRUE),
      .by       = c(year_num, level, setting)
    )
  write_csv(outlier_summary, file.path(OUT_DIR, "rate_outlier_summary_counts.csv"))
}

## -------------------------------------------------------------------------
## Analysis 5: Slide-Ready Text Output
## -------------------------------------------------------------------------

message("\nGenerating slide-ready outputs...")

slide_lines <- ps_year_lines %>%
  mutate(
    slide_text = paste0(
      "In ", year_num, ", ", top_label, " of schools accounted for ", share_pct,
      " of ", ifelse(MEASURE == "undup_susp", "unduplicated suspensions", "suspension events"), "."
    )
  ) %>%
  select(year_num, top_label, share_pct, slide_text)

write_csv(slide_lines, file.path(OUT_DIR, "pareto_slide_lines.csv"))

## -------------------------------------------------------------------------
## Summary Statistics and Completion
## -------------------------------------------------------------------------

# Generate overall summary report
summary_stats <- dat %>%
  group_by(year_num) %>%
  summarise(
    n_schools = n_distinct(school_id),
    total_enrollment = sum(enrollment, na.rm = TRUE),
    total_suspensions = sum(measure, na.rm = TRUE),
    mean_rate = mean(safe_div(measure, enrollment), na.rm = TRUE),
    median_rate = median(safe_div(measure, enrollment), na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    overall_rate = safe_div(total_suspensions, total_enrollment),
    mean_rate_pct = scales::percent(mean_rate, accuracy = 0.1),
    median_rate_pct = scales::percent(median_rate, accuracy = 0.1),
    overall_rate_pct = scales::percent(overall_rate, accuracy = 0.1)
  )

write_csv(summary_stats, file.path(OUT_DIR, "summary_statistics_by_year.csv"))

## Completion message
message("\n", paste(rep("=", 70), collapse = ""))
message("TAIL CONCENTRATION BY GRADE × SCHOOL TYPE ANALYSIS COMPLETE")
message(paste(rep("=", 70), collapse = ""))
message("Output directory: ", OUT_DIR)
message("\nKey files created:")
message("• data_structure_summary.csv")
message("• pareto_shares_by_year_slide_ready.csv")
message("• lorenz_points_by_year_with_gini.csv") 
message("• pareto_slide_lines.csv")
message("• summary_statistics_by_year.csv")

if (file.exists(file.path(OUT_DIR, "pareto_by_grade_setting.csv"))) {
  message("• pareto_by_grade_setting.csv")
  message("• pareto_grade_setting_slide_ready.csv") 
  message("• outliers_by_grade_setting.csv")
  message("• lorenz_by_grade_setting.csv")
  message("• summary_by_grade_setting.csv")
  message("• school_counts_by_grade_setting.csv")
  message("• rates_by_grade_setting.csv")
  message("• rates_comparison_wide.csv")
}

if (file.exists(file.path(OUT_DIR, "rate_outliers_by_year_level_setting.csv"))) {
  message("• rate_outliers_by_year_level_setting.csv")
  message("• rate_thresholds_by_grade_setting.csv")
  message("• rate_outlier_summary_counts.csv")
}

if (file.exists(file.path(OUT_DIR, paste0("lorenz_", max(lg_year$year_num), ".png")))) {
  message("• Lorenz curve plot for ", max(lg_year$year_num))
}

message("\nAnalysis complete! Check outputs for grade × setting stratified results.")
message("Files enable comparison of concentration patterns between:")
message("  - Traditional vs Non-traditional schools")
message("  - Elementary vs Middle vs High school levels")
message("  - All combinations of grade level × school setting")