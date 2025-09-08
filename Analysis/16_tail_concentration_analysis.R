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

## -------------------------------------------------------------------------
## Packages
## -------------------------------------------------------------------------
suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(janitor)
  library(tidyr)
  library(stringr)
  library(arrow)
  library(here)
  library(ggplot2)
  library(scales)
  library(ineq)    # for Lorenz and Gini calculations
  library(purrr)
})

## -------------------------------------------------------------------------
## Configuration
## -------------------------------------------------------------------------
# Edit the following constants to match your project structure and data.

# Path to input data (Parquet or CSV)
# Update this to point to your long-form suspension dataset.  In this project,
# the detailed long dataset with reason breakdowns is stored under
# data-stage/susp_v5_long_strict.parquet.  Change this path if you prefer
# to use another version (e.g., susp_v5_long.parquet).
INPUT_PATH <- here("data-stage", "susp_v5_long_strict.parquet")
# Set to TRUE if INPUT_PATH is a Parquet file; FALSE if it is a CSV
INPUT_IS_PARQUET <- TRUE

# Output directory (will be created if it doesn’t exist)
RUN_TAG <- format(Sys.time(), "%Y%m%d_%H%M")
OUT_DIR <- here("outputs", paste0("tail_concentration_", RUN_TAG))
if (!dir.exists(OUT_DIR)) dir.create(OUT_DIR, recursive = TRUE)

# Column names in the input table.  Adjust these strings to match your dataset.
# For the v5_long_strict dataset, academic_year has values like "2017-18".
# We extract the first four digits for the numeric year.  Settings are mapped
# from ed_ops_name (Traditional vs Non-traditional), and level from
# school_level_final.  Race and swd columns are not used in this script.
cols <- list(
  school_id   = "school_code",                                       # unique school identifier
  school_name = "school_name",                                       # school name
  year        = "academic_year",                                     # academic year string (e.g., "2017-18")
  setting     = "ed_ops_name",                                       # education operations type
  level       = "school_level_final",                                # Elementary/Middle/High/Other
  enrollment  = "cumulative_enrollment",                              # enrollment denominator
  total_susp  = "total_suspensions",                                 # total suspension events
  undup_susp  = "unduplicated_count_of_students_suspended_total",     # unduplicated suspensions
  race        = NULL,                                                  # not used
  swd         = NULL                                                   # not used
)

# Choose the measure for concentration (“total_susp” or “undup_susp”)
MEASURE <- "total_susp"

# If TRUE, compute Pareto shares within each level × setting band as well
GROUP_BY_LEVEL_SETTING <- TRUE

# Top percentages for Pareto shares
TOP_PCT <- c(0.05, 0.10, 0.20)

# Percentiles for rate outlier flags
RATE_PCTS <- c(0.90, 0.95)
# Rate denominator (per 100 students)
RATE_PER <- 100

## -------------------------------------------------------------------------
## Data Loading
## -------------------------------------------------------------------------
read_data <- function(path, is_parquet = TRUE) {
  if (is_parquet) {
    open_dataset(path) %>% collect()
  } else {
    readr::read_csv(path, guess_max = 1e6, progress = FALSE)
  }
}

dat0 <- read_data(INPUT_PATH, INPUT_IS_PARQUET) %>%
  clean_names()

# Validate required columns exist
req_cols <- unlist(cols[c("school_id", "school_name", "year", "enrollment",
                          "total_susp", "undup_susp")])
missing_cols <- setdiff(req_cols, names(dat0))
if (length(missing_cols) > 0) {
  stop("Missing required columns in input data: ", paste(missing_cols, collapse = ", "))
}

# Rename columns to unified names and coerce types
dat <- dat0 %>%
  rename(
    school_id   = !!sym(cols$school_id),
    school_name = !!sym(cols$school_name),
    year        = !!sym(cols$year),
    setting     = !!sym(cols$setting),
    level       = !!sym(cols$level),
    enrollment  = !!sym(cols$enrollment),
    total_susp  = !!sym(cols$total_susp),
    undup_susp  = !!sym(cols$undup_susp)
  ) %>%
  # Convert and map fields.  Extract the numeric year from academic_year (e.g. "2017-18" -> 2017).
  mutate(
    year       = as.integer(stringr::str_sub(year, 1, 4)),
    enrollment = as.numeric(enrollment),
    total_susp = as.numeric(total_susp),
    undup_susp = as.numeric(undup_susp),
    # Map setting: "Traditional" stays Traditional; all other values become "Non-traditional".
    setting    = dplyr::case_when(
      is.na(setting) ~ NA_character_,
      setting == "Traditional" ~ "Traditional",
      TRUE ~ "Non-traditional"
    ),
    # Retain level as is
    level      = level
  )

# Add “measure” column based on MEASURE option
dat <- dat %>%
  mutate(measure = if (MEASURE == "undup_susp") undup_susp else total_susp)

## -------------------------------------------------------------------------
## Helper Functions: Pareto, Lorenz/Gini, group apply
## -------------------------------------------------------------------------
# Compute Pareto shares for given top percentages
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
      share_cum     = ifelse(total_measure > 0, cum_measure / total_measure, NA_real_)
    )
  
  out <- map_dfr(top_ps, function(p) {
    cutoff_n <- max(1, floor(p * n_schools))
    top_share <- df_sorted %>%
      slice(1:cutoff_n) %>%
      summarise(
        top_schools   = cutoff_n,
        total_schools = n_schools,
        top_share     = ifelse(sum(total_measure, na.rm = TRUE)[1] > 0,
                               sum(measure, na.rm = TRUE) / df_sorted$total_measure[1],
                               NA_real_)
      ) %>% mutate(top_pct = p)
    top_share
  })
  out
}

# Compute Lorenz curve points and Gini coefficient
lorenz_gini <- function(df) {
  x <- df %>%
    group_by(school_id) %>%
    summarise(measure = sum(measure, na.rm = TRUE), .groups = "drop") %>%
    pull(measure)
  
  x[is.na(x)] <- 0
  if (length(x) == 0 || sum(x) == 0) return(list(curve = tibble(p = 0, L = 0), gini = NA_real_))
  L <- ineq::Lc(x)
  g <- ineq::Gini(x)
  list(curve = tibble(p = L$p, L = L$L), gini = g)
}

# Apply a function across groups of a dataframe
group_apply <- function(df, group_vars, fn) {
  df %>%
    group_by(across(all_of(group_vars))) %>%
    group_modify(function(d, key) {
      fn(d) %>% mutate(!!!key)
    }) %>%
    ungroup()
}

## -------------------------------------------------------------------------
## Pareto Shares by Year
## -------------------------------------------------------------------------
# Statewide by year
ps_year <- group_apply(dat, "year", ~pareto_shares(.x, TOP_PCT)) %>%
  mutate(measure_type = MEASURE)

# Generate slide-ready lines
ps_year_lines <- ps_year %>%
  mutate(
    top_label = paste0("Top ", percent(top_pct)),
    share_pct = percent(top_share, accuracy = 0.1)
  ) %>%
  select(year, top_label, share_pct, top_schools, total_schools, measure_type)

# Save results
write_csv(ps_year, file.path(OUT_DIR, "pareto_shares_by_year_raw.csv"))
write_csv(ps_year_lines, file.path(OUT_DIR, "pareto_shares_by_year_slide_ready.csv"))

## -------------------------------------------------------------------------
## Pareto Shares by Year × Level × Setting (optional)
## -------------------------------------------------------------------------
if (GROUP_BY_LEVEL_SETTING && all(c("level", "setting") %in% names(dat))) {
  ps_y_ls <- dat %>%
    filter(!is.na(level), !is.na(setting)) %>%
    group_by(year, level, setting) %>%
    group_modify(~pareto_shares(.x, TOP_PCT)) %>%
    ungroup() %>%
    mutate(measure_type = MEASURE)
  
  ps_y_ls_lines <- ps_y_ls %>%
    mutate(
      top_label = paste0("Top ", percent(top_pct)),
      share_pct = percent(top_share, accuracy = 0.1)
    ) %>%
    select(year, level, setting, top_label, share_pct, top_schools, total_schools, measure_type)
  
  write_csv(ps_y_ls, file.path(OUT_DIR, "pareto_shares_by_year_level_setting_raw.csv"))
  write_csv(ps_y_ls_lines, file.path(OUT_DIR, "pareto_shares_by_year_level_setting_slide_ready.csv"))
}

## -------------------------------------------------------------------------
## Lorenz Curve and Gini Coefficient
## -------------------------------------------------------------------------
lg_year <- dat %>%
  group_by(year) %>%
  group_modify(~{
    out <- lorenz_gini(.x)
    out$curve %>% mutate(gini = out$gini)
  }) %>%
  ungroup()

write_csv(lg_year, file.path(OUT_DIR, "lorenz_points_by_year_with_gini.csv"))

## Optional: Plot Lorenz curve for the most recent year
if (nrow(lg_year) > 0) {
  latest_year <- max(lg_year$year, na.rm = TRUE)
  p_lorenz <- lg_year %>%
    filter(year == latest_year) %>%
    ggplot(aes(x = p, y = L)) +
    geom_line(size = 1) +
    geom_abline(slope = 1, intercept = 0, linetype = "dashed") +
    scale_x_continuous(labels = percent) +
    scale_y_continuous(labels = percent) +
    labs(
      title = paste0("Lorenz Curve of Suspensions — ", latest_year),
      x = "Cumulative share of schools",
      y = "Cumulative share of suspensions"
    ) +
    theme_minimal(base_size = 12)
  ggsave(file.path(OUT_DIR, paste0("lorenz_", latest_year, ".png")),
         p_lorenz, width = 6.5, height = 5, dpi = 300)
}

## -------------------------------------------------------------------------
## Rate Outliers by Level × Setting
## -------------------------------------------------------------------------
if (all(c("level", "setting") %in% names(dat))) {
  rates <- dat %>%
    group_by(year, school_id, school_name, level, setting) %>%
    summarise(
      enrollment = sum(enrollment, na.rm = TRUE),
      measure    = sum(measure, na.rm = TRUE),
      .groups    = "drop"
    ) %>%
    mutate(rate_per100 = ifelse(enrollment > 0, (measure / enrollment) * RATE_PER, NA_real_))
  
  # Thresholds by percentile
  thresh <- rates %>%
    group_by(year, level, setting) %>%
    summarise(
      p90 = quantile(rate_per100, probs = 0.90, na.rm = TRUE),
      p95 = quantile(rate_per100, probs = 0.95, na.rm = TRUE),
      .groups = "drop"
    )
  
  outliers <- rates %>%
    inner_join(thresh, by = c("year", "level", "setting")) %>%
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
      .by       = c(year, level, setting)
    )
  write_csv(outlier_summary, file.path(OUT_DIR, "rate_outlier_summary_counts.csv"))
}

## -------------------------------------------------------------------------
## Reason Mix of the Tail (Top 10% of Schools)
## -------------------------------------------------------------------------
# If the dataset contains reason-specific count columns (r_vi, r_vn, r_wp, r_id, r_def, r_oth),
# compute the reason mix among the top 10% of schools versus the statewide mix.
reason_cols <- c("r_vi", "r_vn", "r_wp", "r_id", "r_def", "r_oth")

if (all(reason_cols %in% names(dat))) {
  # Rank schools by total measure (either total_susp or undup_susp) within each year
  by_school_year <- dat %>%
    group_by(year, school_id, school_name) %>%
    summarise(measure = sum(measure, na.rm = TRUE), .groups = "drop")
  
  tail_labels <- by_school_year %>%
    group_by(year) %>%
    arrange(desc(measure), .by_group = TRUE) %>%
    mutate(
      rank           = row_number(),
      n_schools      = n(),
      top_decile_cut = ceiling(0.10 * n_schools),
      in_tail10      = rank <= top_decile_cut
    ) %>%
    ungroup() %>%
    select(year, school_id, in_tail10)
  
  # Pivot reason counts into long format with descriptive category names
  reason_df <- dat %>%
    select(year, school_id, all_of(reason_cols)) %>%
    pivot_longer(cols = all_of(reason_cols), names_to = "reason_code", values_to = "reason_count") %>%
    mutate(
      reason_code = recode(reason_code,
                           r_vi  = "Violent incident (injury)",
                           r_vn  = "Violent incident (no injury)",
                           r_wp  = "Weapons possession",
                           r_id  = "Illicit drug",
                           r_def = "Defiance only",
                           r_oth = "Other reasons"
      )
    )
  
  # Statewide reason composition (sum across all schools)
  reason_mix_state <- reason_df %>%
    group_by(year, reason_code) %>%
    summarise(reason_state = sum(reason_count, na.rm = TRUE), .groups = "drop")
  
  # Reason composition within the top decile of schools
  tail_reason_mix <- reason_df %>%
    inner_join(tail_labels, by = c("year", "school_id")) %>%
    filter(in_tail10) %>%
    group_by(year, reason_code) %>%
    summarise(reason_tail = sum(reason_count, na.rm = TRUE), .groups = "drop")
  
  # Combine and compute shares
  reason_compare <- reason_mix_state %>%
    full_join(tail_reason_mix, by = c("year", "reason_code")) %>%
    group_by(year) %>%
    mutate(
      total_state = sum(reason_state, na.rm = TRUE),
      total_tail  = sum(reason_tail, na.rm = TRUE),
      share_state = ifelse(total_state > 0, reason_state / total_state, NA_real_),
      share_tail  = ifelse(total_tail  > 0, reason_tail  / total_tail,  NA_real_)
    ) %>%
    ungroup() %>%
    arrange(year, desc(share_tail))
  
  write_csv(reason_compare, file.path(OUT_DIR, "reason_mix_tail_vs_statewide.csv"))
  
  # Plot reason mix comparison for the latest available year
  latest_year_rc <- max(reason_compare$year, na.rm = TRUE)
  rc_plot <- reason_compare %>%
    filter(year == latest_year_rc) %>%
    pivot_longer(c(share_state, share_tail), names_to = "where", values_to = "share") %>%
    mutate(where = recode(where, share_state = "Statewide", share_tail = "Top 10% schools")) %>%
    ggplot(aes(x = reorder(reason_code, share), y = share, fill = where)) +
    geom_col(position = position_dodge(width = 0.75)) +
    coord_flip() +
    scale_y_continuous(labels = percent_format(accuracy = 1)) +
    labs(
      title = paste0("Reason Mix — Tail vs Statewide (", latest_year_rc, ")"),
      x = "Reason category", y = "Share of suspensions", fill = NULL
    ) +
    theme_minimal(base_size = 12)
  ggsave(file.path(OUT_DIR, paste0("reason_mix_tail_vs_statewide_", latest_year_rc, ".png")),
         rc_plot, width = 7.5, height = 6, dpi = 300)
}

## -------------------------------------------------------------------------
## Slide-Ready Text Output
## -------------------------------------------------------------------------
slide_lines <- ps_year_lines %>%
  mutate(
    slide_text = paste0(
      "In ", year, ", ", top_label, " of schools accounted for ", share_pct,
      " of ",
      ifelse(MEASURE == "undup_susp", "unduplicated suspensions", "suspension events"), "."
    )
  ) %>%
  select(year, top_label, share_pct, slide_text)

write_csv(slide_lines, file.path(OUT_DIR, "pareto_slide_lines.csv"))

if (GROUP_BY_LEVEL_SETTING && exists("ps_y_ls_lines")) {
  slide_lines_ls <- ps_y_ls_lines %>%
    mutate(
      slide_text = paste0(
        "In ", year, " (", level, ", ", setting, "), ",
        top_label, " of schools accounted for ", share_pct, " of ",
        ifelse(MEASURE == "undup_susp", "unduplicated suspensions", "suspension events"), "."
      )
    ) %>%
    select(year, level, setting, top_label, share_pct, slide_text)
  write_csv(slide_lines_ls, file.path(OUT_DIR, "pareto_slide_lines_by_level_setting.csv"))
}

## -------------------------------------------------------------------------
## Completion Message
## -------------------------------------------------------------------------
message("All outputs written to: ", OUT_DIR)
message("Key files include:")
message(" - pareto_shares_by_year_slide_ready.csv (Pareto summary)")
message(" - lorenz_points_by_year_with_gini.csv (Lorenz points and Gini)")
message(" - rate_outliers_by_year_level_setting.csv (Rate outliers)")
message(" - reason_mix_tail_vs_statewide.csv (Reason mix for tail vs statewide)")
