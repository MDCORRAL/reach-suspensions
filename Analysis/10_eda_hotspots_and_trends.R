# analysis/10_eda_hotspots_and_trends.R
# Exploratory scan for: (1) where disparities are largest, (2) where rates changed the most,
# with (3) diagnostics for thin buckets and (4) optional reason-specific rate changes.

# --- Libraries ---------------------------------------------------------------
suppressPackageStartupMessages({
  library(dplyr); library(tidyr); library(arrow); library(here); library(readr); library(scales)
})

# --- Configuration -----------------------------------------------------------
RACE_CODES <- c(
  RB = "Black/African American",
  RW = "White",
  RH = "Hispanic/Latino",
  RL = "Hispanic/Latino",                     # alias
  RI = "American Indian/Alaska Native",
  RA = "Asian",
  RF = "Filipino",
  RP = "Pacific Islander",
  RT = "Two or More Races",
  TA = "All Students"
)

MIN_ENROLLMENT_THRESHOLD <- 10   # guard against tiny denominators in rate calc
MIN_RACES_FOR_SPREAD     <- 2    # need at least this many races to compute spread
DROP_UNKNOWN_LOCALE      <- FALSE
INCLUDE_REASON_ANALYSIS  <- TRUE  # set FALSE to skip reason tables

# --- Helpers -----------------------------------------------------------------
race_label <- function(code) dplyr::recode(code, !!!RACE_CODES, .default = NA_character_)

safe_rate <- function(suspensions, enrollment, min_enroll = 0) {
  dplyr::if_else(enrollment > min_enroll, suspensions / enrollment, NA_real_)
}

safe_ratio <- function(numerator, denominator, min_denom = 0) {
  dplyr::if_else(!is.na(denominator) & denominator > min_denom, numerator / denominator, NA_real_)
}

# --- Load & guards -----------------------------------------------------------
v5_path <- here("data-stage", "susp_v5.parquet")
if (!file.exists(v5_path)) stop("Data file not found: ", v5_path)
v5 <- read_parquet(v5_path)

required_cols <- c("reporting_category","academic_year","total_suspensions",
                   "cumulative_enrollment","level_strict3","locale_simple")
missing_cols <- setdiff(required_cols, names(v5))
if (length(missing_cols)) stop("Missing required columns: ", paste(missing_cols, collapse = ", "))

# Order academic years (driven by TA)
year_levels <- v5 %>%
  filter(reporting_category == "TA") %>%
  distinct(academic_year) %>% arrange(academic_year) %>% pull()
if (!length(year_levels)) stop("No TA rows found to anchor academic_year order.")

# Optional: drop Unknown locale up front
if (isTRUE(DROP_UNKNOWN_LOCALE)) {
  v5 <- v5 %>% filter(locale_simple != "Unknown")
}

# --- Base with labels & factors ---------------------------------------------
base <- v5 %>%
  mutate(
    race     = race_label(reporting_category),
    year_fct = factor(academic_year, levels = year_levels, ordered = TRUE)
  ) %>%
  filter(!is.na(race)) %>%
  mutate(
    has_valid_enrollment  = cumulative_enrollment > 0,
    has_valid_suspensions = !is.na(total_suspensions) & total_suspensions >= 0
  )

# --- TA (All Students) rates by setting-year --------------------------------
ta_rates <- base %>%
  filter(race == "All Students") %>%
  group_by(level_strict3, locale_simple, academic_year, year_fct) %>%
  summarise(
    total_susp_TA = sum(total_suspensions, na.rm = TRUE),
    enroll_TA     = sum(cumulative_enrollment, na.rm = TRUE),
    rate_TA       = safe_rate(total_susp_TA, enroll_TA, MIN_ENROLLMENT_THRESHOLD),
    n_schools     = n(),
    .groups = "drop"
  )

# --- Race-specific pooled rates & disparities --------------------------------
race_rates <- base %>%
  group_by(level_strict3, locale_simple, academic_year, year_fct, race) %>%
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
            by = c("level_strict3","locale_simple","academic_year","year_fct")) %>%
  mutate(
    disparity_ratio = safe_ratio(rate, rate_TA),
    disparity_diff  = if_else(!is.na(rate) & !is.na(rate_TA), rate - rate_TA, NA_real_)
  )

# --- Thin-bucket diagnostic (why some spreads end up empty) -------------------
thin_buckets <- race_rates %>%
  filter(race != "All Students") %>%
  group_by(level_strict3, locale_simple, academic_year, year_fct) %>%
  summarise(
    n_ok_races = sum(!is.na(rate) & sufficient_sample),
    any_ta     = any(!is.na(rate_TA)),
    .groups = "drop"
  ) %>%
  filter(n_ok_races < MIN_RACES_FOR_SPREAD)

# --- Spread / inequality within setting-year ---------------------------------
spread_by_year <- race_rates %>%
  filter(race != "All Students", !is.na(rate), sufficient_sample) %>%
  group_by(level_strict3, locale_simple, academic_year, year_fct) %>%
  filter(n() >= MIN_RACES_FOR_SPREAD) %>%
  summarise(
    n_races     = n(),
    max_rate    = max(rate, na.rm = TRUE),
    min_rate    = min(rate, na.rm = TRUE),
    spread_abs  = max_rate - min_rate,
    spread_ratio= safe_ratio(max_rate, min_rate),
    # robust alternatives
    p90_rate    = quantile(rate, 0.9, na.rm = TRUE),
    p10_rate    = quantile(rate, 0.1, na.rm = TRUE),
    spread_9010 = p90_rate - p10_rate,
    cv          = sd(rate, na.rm = TRUE) / mean(rate, na.rm = TRUE),
    .groups = "drop"
  )

# Rank settings by average spread across years
spread_rank <- spread_by_year %>%
  group_by(level_strict3, locale_simple) %>%
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
  arrange(desc(avg_spread))

# --- Disparity vs All Students by race ---------------------------------------
disp_vs_all_rank <- race_rates %>%
  filter(race != "All Students", sufficient_sample) %>%
  group_by(level_strict3, locale_simple, race) %>%
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
reason_cols <- names(v5)[grepl("^prop_susp_", names(v5))]
reason_map <- c(
  prop_susp_violent_injury     = "Violent (Injury)",
  prop_susp_violent_no_injury  = "Violent (No Injury)",
  prop_susp_weapons_possession = "Weapons",
  prop_susp_illicit_drug       = "Illicit Drug",
  prop_susp_defiance_only      = "Willful Defiance",
  prop_susp_other_reasons      = "Other"
)

reason_changes <- NULL
if (INCLUDE_REASON_ANALYSIS && length(reason_cols)) {
  reason_long <- base %>%
    filter(race != "All Students") %>%
    select(level_strict3, locale_simple, academic_year, year_fct,
           total_suspensions, dplyr::all_of(reason_cols)) %>%
    pivot_longer(dplyr::all_of(reason_cols),
                 names_to = "reason_key", values_to = "prop") %>%
    mutate(reason_count = if_else(!is.na(prop) & !is.na(total_suspensions),
                                  prop * total_suspensions, 0))
  
  reason_by_set_year <- reason_long %>%
    group_by(level_strict3, locale_simple, academic_year, year_fct, reason_key) %>%
    summarise(total_reason = sum(reason_count, na.rm = TRUE), .groups = "drop") %>%
    left_join(ta_rates, by = c("level_strict3","locale_simple","academic_year","year_fct")) %>%
    mutate(
      reason_rate = safe_rate(total_reason, enroll_TA, MIN_ENROLLMENT_THRESHOLD),
      reason      = dplyr::recode(reason_key, !!!reason_map)
    )
  
  reason_changes <- reason_by_set_year %>%
    group_by(level_strict3, locale_simple, reason) %>%
    arrange(year_fct, .by_group = TRUE) %>%
    summarise(
      start_year = dplyr::first(academic_year),
      end_year   = dplyr::last(academic_year),
      start_rate = dplyr::first(reason_rate),
      end_rate   = dplyr::last(reason_rate),
      abs_change = end_rate - start_rate,
      pct_change = if_else(is.finite(start_rate) & start_rate > 0,
                           (end_rate - start_rate)/start_rate, NA_real_),
      .groups = "drop"
    ) %>%
    mutate(direction = case_when(
      is.na(abs_change) ~ NA_character_,
      abs_change > 0 ~ "Increase",
      abs_change < 0 ~ "Decrease",
      TRUE ~ "No change"
    )) %>%
    arrange(desc(abs(abs_change)))
}

# --- Outputs ------------------------------------------------------------------
outdir <- here("outputs")
dir.create(outdir, showWarnings = FALSE, recursive = TRUE)

# Diagnostics
write_csv(thin_buckets, file.path(outdir, "EDA_why_empty_buckets_level_locale_year.csv"))

# Main outputs
write_csv(spread_rank,      file.path(outdir, "EDA_spread_by_level_locale_rank.csv"))
write_csv(disp_vs_all_rank, file.path(outdir, "EDA_disparity_vs_AllStudents_by_race_rank.csv"))
write_csv(spread_by_year,   file.path(outdir, "EDA_spread_by_year_detail.csv"))

if (!is.null(reason_changes)) {
  write_csv(reason_changes, file.path(outdir, "EDA_reason_rate_changes_by_level_locale.csv"))
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
  "\n======================================\n"
)

# Optional interactive peeks
if (interactive()) {
  View(spread_rank %>% arrange(desc(avg_spread)) %>% head(20))
  View(disp_vs_all_rank %>% filter(race == "Black/African American") %>% head(20))
}