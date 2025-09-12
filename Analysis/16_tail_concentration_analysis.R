# 16_tail_concentration_analysis.R
# Computes Pareto shares (top 5/10/20%), Lorenz/Gini, rate outliers, and tail reason-mix.
# Save into Analysis/ (or scripts/) and run from the project root.
## Tail Concentration Analysis Script
##
## This R script computes Pareto shares (top 5%, 10%, and 20% of schools),
## Lorenz curves and Gini coefficients, identifies rate outliers within
## level × setting bands, and compares the composition of suspension reason
## codes in the top 10% of schools against statewide totals. It is designed
## for use in the reach-suspensions project and assumes a tidy data table
## with school-level suspension counts and contextual fields.
# 16_tail_concentration_analysis.R
# Updated for REACH repository structure and data format
# Computes Pareto shares, Lorenz/Gini, rate outliers, and tail reason-mix.

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
OUT_DIR <- here("outputs", paste0("tail_concentration_", RUN_TAG))
if (!dir.exists(OUT_DIR)) dir.create(OUT_DIR, recursive = TRUE)

# Updated column mappings for your data structure
cols <- list(
  school_id   = "school_code",
  school_name = "school_name", 
  year        = "academic_year",
  setting     = "school_type",                    # Traditional vs Other
  level       = "school_level",
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

# Clean school type mapping
map_setting <- function(x) {
  x_clean <- str_to_lower(str_trim(as.character(x)))
  case_when(
    is.na(x) ~ NA_character_,
    str_detect(x_clean, "traditional") ~ "Traditional",
    TRUE ~ "Non-traditional"
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

# Group apply function
group_apply <- function(df, group_vars, fn) {
  df %>%
    group_by(across(all_of(group_vars))) %>%
    group_modify(function(d, key) {
      result <- fn(d)
      if (is.data.frame(result)) {
        result %>% mutate(!!!key)
      } else {
        tibble() %>% mutate(!!!key)
      }
    }) %>%
    ungroup()
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
    measure     = if (MEASURE == "undup_susp") undup_susp else total_susp
  ) %>%
  # Add school name if available
  mutate(school_name = if ("school_name" %in% names(dat0)) school_name else school_id) %>%
  # Filter out invalid records
  filter(
    !is.na(year_num), 
    !is.na(enrollment), 
    !is.na(measure),
    enrollment > 0,
    measure >= 0
  )

# Add school features if available (traditional vs non-traditional)
if (file.exists(V6F_PARQ)) {
  message("Loading school features...")
  v6_features <- read_parquet(V6F_PARQ) %>% clean_names()
  
  # Check what columns actually exist
  message("Available columns in v6_features: ", paste(names(v6_features), collapse = ", "))
  
  # Select only columns that exist
  available_cols <- intersect(c("school_code", "year", "is_traditional", "school_type"), names(v6_features))
  
  v6_features <- v6_features %>%
    select(all_of(available_cols)) %>%
    mutate(
      school_code = as.character(school_code),
      year_char = as.character(year)
    )
  
  # Join features
  dat <- dat %>%
    left_join(
      v6_features, 
      by = c("school_id" = "school_code", "year" = "year_char")
    ) %>%
    mutate(
      setting = ifelse(is.na(is_traditional) | !is_traditional, 
                       "Non-traditional", "Traditional"),
      level = if("school_type" %in% names(.)) coalesce(school_type, "Other") else "Other"
    )
} else {
  # Default values if no features available
  dat <- dat %>%
    mutate(
      setting = "Unknown",
      level = "Unknown"
    )
}

message("Final dataset: ", nrow(dat), " school-year records")
message("Years covered: ", paste(sort(unique(dat$year_num)), collapse = ", "))

## -------------------------------------------------------------------------
## Analysis 1: Pareto Shares by Year
## -------------------------------------------------------------------------

# Replace this problematic line:
# ps_year <- group_apply(dat, "year_num", ~pareto_shares(.x, TOP_PCT)) %>%
#     mutate(measure_type = MEASURE)

# With this working version:
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
## Analysis 2: Pareto Shares by Year × Level × Setting
## -------------------------------------------------------------------------

if (all(c("level", "setting") %in% names(dat))) {
  ps_y_ls <- dat %>%
    filter(!is.na(level), !is.na(setting), level != "Unknown", setting != "Unknown") %>%
    group_by(year_num, level, setting) %>%
    group_modify(~pareto_shares(.x, TOP_PCT)) %>%
    ungroup() %>%
    mutate(measure_type = MEASURE)
  
  ps_y_ls_lines <- ps_y_ls %>%
    mutate(
      top_label = paste0("Top ", scales::percent(top_pct)),
      share_pct = scales::percent(top_share, accuracy = 0.1)
    ) %>%
    select(year_num, level, setting, top_label, share_pct, top_schools, total_schools, measure_type)
  
  write_csv(ps_y_ls, file.path(OUT_DIR, "pareto_shares_by_year_level_setting_raw.csv"))
  write_csv(ps_y_ls_lines, file.path(OUT_DIR, "pareto_shares_by_year_level_setting_slide_ready.csv"))
}

## -------------------------------------------------------------------------
## Analysis 3: Lorenz Curve and Gini Coefficient
## -------------------------------------------------------------------------

lg_year <- dat %>%
  group_by(year_num) %>%
  group_modify(~{
    out <- lorenz_gini(.x)
    out$curve %>% mutate(gini = out$gini)
  }) %>%
  ungroup()

write_csv(lg_year, file.path(OUT_DIR, "lorenz_points_by_year_with_gini.csv"))

# Plot Lorenz curve for most recent year
if (nrow(lg_year) > 0) {
  latest_year <- max(lg_year$year_num, na.rm = TRUE)
  p_lorenz <- lg_year %>%
    filter(year_num == latest_year) %>%
    ggplot(aes(x = p, y = L)) +
    geom_line(size = 1, color = "steelblue") +
    geom_abline(slope = 1, intercept = 0, linetype = "dashed", color = "gray50") +
    scale_x_continuous(labels = scales::percent_format()) +
    scale_y_continuous(labels = scales::percent_format()) +
    labs(
      title = paste0("Lorenz Curve of Suspensions — ", latest_year),
      subtitle = paste0("Gini coefficient: ", 
                        round(lg_year$gini[lg_year$year_num == latest_year][1], 3)),
      x = "Cumulative share of schools",
      y = "Cumulative share of suspensions"
    ) +
    theme_minimal(base_size = 12) +
    theme(
      panel.grid.minor = element_blank(),
      plot.title = element_text(face = "bold")
    )
  
  ggsave(file.path(OUT_DIR, paste0("lorenz_", latest_year, ".png")),
         p_lorenz, width = 8, height = 6, dpi = 300)
}

## -------------------------------------------------------------------------
## Analysis 4: Rate Outliers by Level × Setting
## -------------------------------------------------------------------------

if (all(c("level", "setting") %in% names(dat))) {
  rates <- dat %>%
    filter(!is.na(level), !is.na(setting)) %>%
    group_by(year_num, school_id, school_name, level, setting) %>%
    summarise(
      enrollment = sum(enrollment, na.rm = TRUE),
      measure    = sum(measure, na.rm = TRUE),
      .groups    = "drop"
    ) %>%
    mutate(rate_per100 = safe_div(measure, enrollment) * RATE_PER)
  
  # Calculate thresholds by percentile
  thresh <- rates %>%
    group_by(year_num, level, setting) %>%
    summarise(
      p90 = quantile(rate_per100, probs = 0.90, na.rm = TRUE),
      p95 = quantile(rate_per100, probs = 0.95, na.rm = TRUE),
      .groups = "drop"
    )
  
  outliers <- rates %>%
    inner_join(thresh, by = c("year_num", "level", "setting")) %>%
    mutate(
      flag_p90 = rate_per100 >= p90,
      flag_p95 = rate_per100 >= p95
    ) %>%
    arrange(desc(rate_per100))
  
  write_csv(outliers, file.path(OUT_DIR, "rate_outliers_by_year_level_setting.csv"))
  
  # Summary counts
  outlier_summary <- outliers %>%
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

slide_lines <- ps_year_lines %>%
  mutate(
    slide_text = paste0(
      "In ", year_num, ", ", top_label, " of schools accounted for ", share_pct,
      " of ", ifelse(MEASURE == "undup_susp", "unduplicated suspensions", "suspension events"), "."
    )
  ) %>%
  select(year_num, top_label, share_pct, slide_text)

write_csv(slide_lines, file.path(OUT_DIR, "pareto_slide_lines.csv"))

if (exists("ps_y_ls_lines")) {
  slide_lines_ls <- ps_y_ls_lines %>%
    mutate(
      slide_text = paste0(
        "In ", year_num, " (", level, ", ", setting, "), ",
        top_label, " of schools accounted for ", share_pct, " of ",
        ifelse(MEASURE == "undup_susp", "unduplicated suspensions", "suspension events"), "."
      )
    ) %>%
    select(year_num, level, setting, top_label, share_pct, slide_text)
  write_csv(slide_lines_ls, file.path(OUT_DIR, "pareto_slide_lines_by_level_setting.csv"))
}

## -------------------------------------------------------------------------
## Summary Statistics and Completion
## -------------------------------------------------------------------------

# Generate summary report
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
    mean_rate_pct = scales::percent(mean_rate, accuracy = 0.1),
    median_rate_pct = scales::percent(median_rate, accuracy = 0.1)
  )

write_csv(summary_stats, file.path(OUT_DIR, "summary_statistics_by_year.csv"))

## Completion message
message("\n", paste(rep("=", 60), collapse = ""))
message("TAIL CONCENTRATION ANALYSIS COMPLETE")
message(paste(rep("=", 60), collapse = ""))
message("Output directory: ", OUT_DIR)
message("\nKey files created:")
message("• pareto_shares_by_year_slide_ready.csv")
message("• lorenz_points_by_year_with_gini.csv") 
message("• pareto_slide_lines.csv")
message("• summary_statistics_by_year.csv")

if (file.exists(file.path(OUT_DIR, "rate_outliers_by_year_level_setting.csv"))) {
  message("• rate_outliers_by_year_level_setting.csv")
}

if (file.exists(file.path(OUT_DIR, paste0("lorenz_", max(lg_year$year_num), ".png")))) {
  message("• Lorenz curve plot for ", max(lg_year$year_num))
}

message("\nAnalysis complete!")