# analysis/10_eda_hotspots_and_trends.R
# Exploratory scan for: (1) where disparities are largest, (2) where rates changed the most,
# with (3) diagnostics for thin buckets and (4) optional reason-specific rate changes.

# --- Libraries ---------------------------------------------------------------
suppressPackageStartupMessages({
  library(dplyr); library(tidyr); library(arrow); library(here); library(readr); library(scales)
})

source(here::here("R", "utils_keys_filters.R"))

# --- Configuration -----------------------------------------------------------
MIN_ENROLLMENT_THRESHOLD <- 10   # guard against tiny denominators in rate calc
MIN_RACES_FOR_SPREAD     <- 2    # need at least this many races to compute spread
DROP_UNKNOWN_LOCALE      <- FALSE
INCLUDE_REASON_ANALYSIS  <- TRUE  # set FALSE to skip reason tables

# NEW toggles and constants
ENABLE_REASON_VALIDATION <- TRUE
EPSILON_START_RATE       <- 1e-3   # for pct-change stability
ENABLE_TREND_TEST        <- FALSE  # set TRUE to add Spearman trend labels

# --- Helpers -----------------------------------------------------------------
safe_rate <- function(suspensions, enrollment, min_enroll = 0) {
  dplyr::if_else(enrollment > min_enroll, suspensions / enrollment, NA_real_)
}

safe_ratio <- function(numerator, denominator, min_denom = 0) {
  dplyr::if_else(!is.na(denominator) & denominator > min_denom, numerator / denominator, NA_real_)
}

trend_dir_spearman <- function(v) {
  v <- v[is.finite(v)]
  if (length(v) < 3) return(NA_character_)
  ct <- suppressWarnings(cor.test(seq_along(v), v, method = "spearman"))
  if (is.na(ct$p.value)) return(NA_character_)
  if (ct$p.value < 0.05) if (ct$estimate > 0) "increasing" else "decreasing" else "stable"
}

# --- Load & guards -----------------------------------------------------------
v6_path <- here("data-stage", "susp_v6_long.parquet")
if (!file.exists(v6_path)) stop("Data file not found: ", v6_path)
v6 <- read_parquet(v6_path)

required_cols <- c("subgroup","academic_year","total_suspensions",
                   "cumulative_enrollment","school_level","locale_simple")
missing_cols <- setdiff(required_cols, names(v6))
if (length(missing_cols)) stop("Missing required columns: ", paste(missing_cols, collapse = ", "))

# Order academic years (driven by TA)
year_levels <- v6 %>%
  filter(category_type == "Race/Ethnicity", subgroup == "All Students") %>%
  distinct(academic_year) %>% arrange(academic_year) %>% pull()
if (!length(year_levels)) stop("No TA rows found to anchor academic_year order.")

# Optional: drop Unknown locale up front
if (isTRUE(DROP_UNKNOWN_LOCALE)) {
  v6 <- v6 %>% filter(locale_simple != "Unknown")
}

# --- Base with labels & factors ---------------------------------------------
base <- v6 %>%
  mutate(
    race     = canon_race_label(subgroup),
    year_fct = factor(academic_year, levels = year_levels, ordered = TRUE)
  ) %>%
  filter(!is.na(race)) %>%
  mutate(
    has_valid_enrollment  = cumulative_enrollment > 0,
    has_valid_suspensions = !is.na(total_suspensions) & total_suspensions >= 0
  )

# Small memory cleanup
rm(v6); invisible(gc())

# --- Outputs: directory + timestamp (used throughout) ------------------------
outdir <- here("outputs"); dir.create(outdir, showWarnings = FALSE, recursive = TRUE)
ts <- format(Sys.time(), "%Y%m%d_%H%M%S")
ts_date <- format(Sys.Date(), "%Y%m%d")

# --- TA (All Students) rates by setting-year --------------------------------
ta_rates <- base %>%
  filter(race == "All Students") %>%
  group_by(school_level, locale_simple, academic_year, year_fct) %>%
  summarise(
    total_susp_TA = sum(total_suspensions, na.rm = TRUE),
    enroll_TA     = sum(cumulative_enrollment, na.rm = TRUE),
    rate_TA       = safe_rate(total_susp_TA, enroll_TA, MIN_ENROLLMENT_THRESHOLD),
    n_schools     = n(),
    .groups = "drop"
  )

# --- Race-specific pooled rates & disparities --------------------------------
race_rates <- base %>%
  group_by(school_level, locale_simple, academic_year, year_fct, race) %>%
  summarise(
    susp      = sum(total_suspensions, na.rm = TRUE),
    enroll    = sum(cumulative_enrollment, na.rm = TRUE),
    n_schools = n(),
    .groups   = "drop"
  ) %>%
  mutate(
    rate               = safe_rate(susp, enroll, MIN_ENROLLMENT_THRESHOLD),
    sufficient_sample  = enroll >= MIN_ENROLLMENT_THRESHOLD
  ) %>%
  left_join(ta_rates %>% select(-n_schools),
            by = c("school_level","locale_simple","academic_year","year_fct")) %>%
  mutate(
    disparity_ratio = safe_ratio(rate, rate_TA),
    disparity_diff  = if_else(!is.na(rate) & !is.na(rate_TA), rate - rate_TA, NA_real_)
  )

# --- Thin-bucket diagnostic (why some spreads end up empty) -------------------
thin_buckets <- race_rates %>%
  dplyr::filter(race != "All Students") %>%
  dplyr::group_by(school_level, locale_simple, academic_year, year_fct) %>%
  dplyr::summarise(
    n_races_total         = dplyr::n(),
    n_ok_races            = sum(!is.na(rate) & sufficient_sample),
    n_na_rate             = sum(is.na(rate)),
    n_insufficient_sample = sum(!sufficient_sample, na.rm = TRUE),
    any_ta                = any(!is.na(rate_TA)),
    min_enrollment = if (all(is.na(enroll))) NA_real_ else min(enroll, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  dplyr::filter(n_ok_races < MIN_RACES_FOR_SPREAD) %>%
  dplyr::mutate(
    issue_type = dplyr::case_when(
      n_races_total < MIN_RACES_FOR_SPREAD ~ "insufficient_races_reported",
      n_insufficient_sample > 0            ~ "small_enrollment",
      n_na_rate > 0                         ~ "missing_rate_data",
      TRUE                                  ~ "other"
    )
  )

# --- Spread / inequality within setting-year ---------------------------------
spread_by_year <- race_rates %>%
  filter(race != "All Students", !is.na(rate), sufficient_sample) %>%
  group_by(school_level, locale_simple, academic_year, year_fct) %>%
  filter(n() >= MIN_RACES_FOR_SPREAD) %>%
  summarise(
    n_races      = n(),
    max_rate     = max(rate, na.rm = TRUE),
    min_rate     = min(rate, na.rm = TRUE),
    spread_abs   = max_rate - min_rate,
    spread_ratio = safe_ratio(max_rate, min_rate),
    # robust alternatives
    p90_rate     = quantile(rate, 0.9, na.rm = TRUE),
    p10_rate     = quantile(rate, 0.1, na.rm = TRUE),
    spread_9010  = p90_rate - p10_rate,
    cv           = sd(rate, na.rm = TRUE) / mean(rate, na.rm = TRUE),
    .groups = "drop"
  )

# Optional: trend test on spread (Spearman)
trend_by_setting <- NULL
if (isTRUE(ENABLE_TREND_TEST)) {
  trend_by_setting <- spread_by_year %>%
    arrange(year_fct) %>%
    group_by(school_level, locale_simple) %>%
    summarise(trend_direction_spearman = trend_dir_spearman(spread_abs), .groups = "drop")
}

# Rank settings by average spread across years
spread_rank <- spread_by_year %>%
  group_by(school_level, locale_simple) %>%
  arrange(year_fct, .by_group = TRUE) %>%
  summarise(
    years_n         = n(),
    races_per_year  = mean(n_races, na.rm = TRUE),
    avg_spread      = mean(spread_abs, na.rm = TRUE),
    avg_ratio       = mean(spread_ratio, na.rm = TRUE),
    median_spread   = median(spread_abs, na.rm = TRUE),
    avg_spread_9010 = mean(spread_9010, na.rm = TRUE),
    avg_cv          = mean(cv, na.rm = TRUE),
    latest_year     = as.character(dplyr::last(academic_year)),
    latest_spread   = dplyr::last(spread_abs),
    latest_ratio    = dplyr::last(spread_ratio),
    first_spread    = dplyr::first(spread_abs),
    trend_direction = case_when(
      latest_spread > first_spread * 1.10 ~ "increasing",
      latest_spread < first_spread * 0.90 ~ "decreasing",
      TRUE ~ "stable"
    ),
    .groups = "drop"
  ) %>%
  { if (!is.null(trend_by_setting)) left_join(., trend_by_setting, by = c("school_level","locale_simple")) else . } %>%
  arrange(desc(avg_spread))

# --- Disparity vs All Students by race ---------------------------------------
disp_vs_all_rank <- race_rates %>%
  filter(race != "All Students", sufficient_sample) %>%
  group_by(school_level, locale_simple, race) %>%
  arrange(year_fct, .by_group = TRUE) %>%
  summarise(
    years_n             = sum(!is.na(disparity_ratio) & is.finite(disparity_ratio)),
    avg_ratio_vs_all    = mean(disparity_ratio[is.finite(disparity_ratio)], na.rm = TRUE),
    median_ratio_vs_all = median(disparity_ratio[is.finite(disparity_ratio)], na.rm = TRUE),
    latest_ratio_vs_all = dplyr::last(disparity_ratio[!is.na(disparity_ratio)]),
    avg_diff_vs_all     = mean(disparity_diff[is.finite(disparity_diff)], na.rm = TRUE),
    latest_diff_vs_all  = dplyr::last(disparity_diff[!is.na(disparity_diff)]),
    latest_year         = as.character(dplyr::last(academic_year)),
    avg_enrollment      = mean(enroll, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  tidyr::replace_na(list(years_n = 0)) %>%
  arrange(desc(avg_ratio_vs_all))

# --- Optional: reason-specific rate changes (per student) ---------------------
reason_cols <- names(base)[grepl("^prop_susp_", names(base))]

val_summary <- NULL
reason_changes <- NULL

if (INCLUDE_REASON_ANALYSIS && length(reason_cols)) {
  # First create reason_wide
  reason_wide <- base %>%
    dplyr::filter(race != "All Students") %>%
    dplyr::select(school_level, locale_simple, academic_year, year_fct,
                  total_suspensions, dplyr::all_of(reason_cols))
  
  # Then validate if enabled
  if (isTRUE(ENABLE_REASON_VALIDATION)) {
    reason_wide <- reason_wide %>%
      dplyr::mutate(
        prop_sum   = rowSums(dplyr::across(dplyr::all_of(reason_cols)), na.rm = TRUE),
        prop_valid = abs(prop_sum - 1) < 0.05
      )
    
    val_summary <- reason_wide %>%
      dplyr::summarise(
        n_rows        = dplyr::n(),
        n_valid       = sum(prop_valid, na.rm = TRUE),
        valid_share   = n_valid / n_rows,
        mean_prop_sum = mean(prop_sum, na.rm = TRUE),
        sd_prop_sum   = sd(prop_sum, na.rm = TRUE),
        min_prop_sum  = min(prop_sum, na.rm = TRUE),
        max_prop_sum  = max(prop_sum, na.rm = TRUE),
        .by = c(school_level, locale_simple, academic_year)
      ) %>%
      dplyr::mutate(
        issue_flag = dplyr::case_when(
          valid_share < 0.95 ~ "validation_issues",
          abs(mean_prop_sum - 1) > 0.02 ~ "systematic_bias",
          sd_prop_sum > 0.10 ~ "high_variability",
          TRUE ~ "ok"
        )
      )
  }
  
  reason_long <- reason_wide %>%
    tidyr::pivot_longer(dplyr::all_of(reason_cols),
                        names_to = "reason_key", values_to = "prop") %>%
    dplyr::mutate(
      reason_count = dplyr::case_when(
        is.na(prop) | is.na(total_suspensions) ~ 0,
        prop < 0 | prop > 1                    ~ NA_real_,   # flag bad props
        TRUE                                   ~ prop * total_suspensions
      )
    )
  
    reason_by_set_year <- reason_long %>%
      dplyr::group_by(school_level, locale_simple, academic_year, year_fct, reason_key) %>%
      dplyr::summarise(total_reason = sum(reason_count, na.rm = TRUE), .groups = "drop") %>%
      dplyr::left_join(ta_rates,
                       by = c("school_level","locale_simple","academic_year","year_fct")) %>%
      dplyr::mutate(
        reason_rate = safe_rate(total_reason, enroll_TA, MIN_ENROLLMENT_THRESHOLD),
        reason = sub("^prop_susp_", "", reason_key)
      ) %>%
      add_reason_label() %>%
      dplyr::mutate(reason = reason_lab) %>%
      dplyr::select(-reason_lab)
  
  reason_changes <- reason_by_set_year %>%
    group_by(school_level, locale_simple, reason) %>%
    arrange(year_fct, .by_group = TRUE) %>%
    summarise(
      start_year = as.character(dplyr::first(academic_year)),
      end_year   = as.character(dplyr::last(academic_year)),
      start_rate = dplyr::first(reason_rate),
      end_rate   = dplyr::last(reason_rate),
      abs_change = end_rate - start_rate,
      pct_change = dplyr::if_else(is.finite(start_rate) & start_rate > EPSILON_START_RATE,
                                  (end_rate - start_rate)/start_rate, NA_real_),
      log_change = dplyr::if_else(is.finite(start_rate) & is.finite(end_rate) &
                                    start_rate > 0 & end_rate > 0,
                                  log(end_rate/start_rate), NA_real_),
      .groups = "drop"
    ) %>%
    mutate(direction = case_when(
      is.na(abs_change) ~ NA_character_,
      abs_change > 0     ~ "Increase",
      abs_change < 0     ~ "Decrease",
      TRUE               ~ "No change"
    )) %>%
    arrange(desc(abs(abs_change)))
}

# --- TOTAL rate changes (stable %) -------------------------------------------
total_changes <- ta_rates %>%
  dplyr::group_by(school_level, locale_simple) %>%
  dplyr::arrange(year_fct, .by_group = TRUE) %>%
  dplyr::summarise(
    start_year = as.character(dplyr::first(academic_year)),
    end_year   = as.character(dplyr::last(academic_year)),
    start_rate = dplyr::first(rate_TA),
    end_rate   = dplyr::last(rate_TA),
    abs_change = end_rate - start_rate,
    pct_change = dplyr::if_else(is.finite(start_rate) & start_rate > EPSILON_START_RATE,
                                (end_rate - start_rate)/start_rate, NA_real_),
    log_change = dplyr::if_else(is.finite(start_rate) & is.finite(end_rate) &
                                  start_rate > 0 & end_rate > 0,
                                log(end_rate/start_rate), NA_real_),
    .groups="drop"
  ) %>%
  dplyr::mutate(
    direction = dplyr::case_when(
      is.na(abs_change) ~ NA_character_,
      abs_change > 0     ~ "Increase",
      abs_change < 0     ~ "Decrease",
      TRUE               ~ "No change"
    )
  ) %>%
  dplyr::arrange(dplyr::desc(abs(abs_change)))

# --- Data completeness snapshot ----------------------------------------------
data_completeness <- base %>%
  dplyr::group_by(school_level, locale_simple, race) %>%
  dplyr::summarise(
    total_records     = dplyr::n(),
    complete_records  = sum(has_valid_enrollment & has_valid_suspensions, na.rm = TRUE),
    completeness_rate = complete_records / total_records,
    .groups = "drop"
  )

# --- Outputs ------------------------------------------------------------------
# Diagnostics
readr::write_csv(thin_buckets,  file.path(outdir, paste0("EDA_why_empty_buckets_level_locale_year_", ts, ".csv")))
readr::write_csv(data_completeness, file.path(outdir, paste0("EDA_data_completeness_", ts, ".csv")))

# Main outputs
readr::write_csv(spread_rank,      file.path(outdir, paste0("EDA_spread_by_level_locale_rank_", ts, ".csv")))
readr::write_csv(disp_vs_all_rank, file.path(outdir, paste0("EDA_disparity_vs_AllStudents_by_race_rank_", ts, ".csv")))
readr::write_csv(spread_by_year,   file.path(outdir, paste0("EDA_spread_by_year_detail_", ts, ".csv")))
readr::write_csv(total_changes,    file.path(outdir, paste0("EDA_total_rate_changes_by_level_locale_", ts, ".csv")))

if (!is.null(reason_changes)) {
  readr::write_csv(reason_changes, file.path(outdir, paste0("EDA_reason_rate_changes_by_level_locale_", ts, ".csv")))
}
if (!is.null(val_summary)) {
  readr::write_csv(val_summary, file.path(outdir, paste0("EDA_reason_prop_validation_", ts, ".csv")))
}

# --- Console summary ----------------------------------------------------------
summary_stats <- list(
  n_settings                 = nrow(spread_rank),
  n_race_setting_combos      = nrow(disp_vs_all_rank),
  years_covered              = paste(range(year_levels), collapse = " to "),
  settings_high_disparity    = sum(spread_rank$avg_ratio > 3, na.rm = TRUE),
  settings_decreasing_trend  = sum(spread_rank$trend_direction == "decreasing", na.rm = TRUE),
  wrote_reason_changes_table = !is.null(reason_changes)
)

cat(
  "\n========== ANALYSIS SUMMARY ==========",
  "\nRows in spread_rank: ", summary_stats$n_settings,
  "\nRows in disp_vs_all_rank: ", summary_stats$n_race_setting_combos,
  "\nYears analyzed: ", summary_stats$years_covered,
  "\nSettings with high disparity (ratio > 3): ", summary_stats$settings_high_disparity,
  "\nSettings with decreasing inequality trend: ", summary_stats$settings_decreasing_trend,
  "\nReason-change table written: ", summary_stats$wrote_reason_changes_table,
  if (isTRUE(DROP_UNKNOWN_LOCALE)) "\n(Unknown locale dropped)" else "",
  "\nTimestamp: ", ts,
  "\n======================================\n"
)

# Optional interactive peeks
if (interactive()) {
  # Current quick views
  View(spread_rank %>% arrange(desc(avg_spread)) %>% head(20))
  View(disp_vs_all_rank %>% filter(race == "Black/African American") %>% head(20))
  
  cat("\n========== INTERACTIVE DIAGNOSTICS ==========\n")
  
  # Top disparities for Black students
  cat("\nTop 5 settings with highest Black/African American disparity:\n")
  print(
    disp_vs_all_rank %>% 
      filter(race == "Black/African American") %>% 
      arrange(desc(avg_ratio_vs_all)) %>% 
      select(school_level, locale_simple, avg_ratio_vs_all, latest_ratio_vs_all) %>%
      head(5),
    n = 5
  )
  
  # Settings with biggest spread change
  cat("\nSettings with most dramatic spread changes:\n")
  print(
    spread_rank %>%
      mutate(spread_change = abs(latest_spread - first_spread)) %>%
      arrange(desc(spread_change)) %>%
      select(school_level, locale_simple, first_spread, latest_spread, trend_direction) %>%
      head(5),
    n = 5
  )
  
  # Data completeness warning
  low_complete <- data_completeness %>% 
    filter(completeness_rate < 0.95) %>%
    nrow()
  if (low_complete > 0) {
    cat("\n⚠️  WARNING:", low_complete, "level-locale-race combinations have <95% data completeness\n")
  }
  
  # Reason validation warning
  if (!is.null(val_summary)) {
    problem_years <- val_summary %>% filter(valid_share < 0.95) %>% nrow()
    if (problem_years > 0) {
      cat("⚠️  WARNING:", problem_years, "setting-years have reason proportion validation issues\n")
    }
  }
  cat("===========================================\n")
}