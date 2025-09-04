# analysis/15_merge_demographic_categories.R
# Merge additional demographic categories with race data for intersectional EDA
suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(arrow)
  library(here)
  library(readr)
  library(janitor)
  library(stringr)
})

# -------- Config -------------------------------------------------------------
RACE_DATA_PATH <- here("data-stage", "susp_v5.parquet")
OTH_PARQUET    <- here("data-stage", "oth_long.parquet")   # produced by R/01b_ingest_demographics.R
MIN_ENROLLMENT_THRESHOLD <- 10

safe_rate <- function(susp, enroll, min_enroll=0) dplyr::if_else(enroll > min_enroll, susp / enroll, NA_real_)

# Keep leading zeros on CDS codes
norm_code <- function(x, width) {
  x_chr <- as.character(x)
  x_num <- stringr::str_replace_all(x_chr, "\\D", "")
  stringr::str_pad(x_num, width = width, side = "left", pad = "0")
}
pad_keys <- function(df) {
  df %>%
    mutate(
      academic_year = as.character(academic_year),
      across(any_of("county_code"),   ~ norm_code(.x, 2)),
      across(any_of("district_code"), ~ norm_code(.x, 5)),
      across(any_of("school_code"),   ~ norm_code(.x, 7))
    )
}

# -------- Load data ----------------------------------------------------------
cat("\n=== Loading race data ===\n")
race_data <- arrow::read_parquet(RACE_DATA_PATH) %>% janitor::clean_names()
if (!"academic_year" %in% names(race_data) && "year" %in% names(race_data)) {
  race_data <- race_data %>%
    mutate(academic_year = ifelse(!is.na(year), paste0(year - 1, "-", substr(year,3,4)), NA_character_))
}
race_data <- pad_keys(race_data)

cat("\n=== Loading OTH demographic parquet ===\n")
if (!file.exists(OTH_PARQUET)) stop("Missing file: ", OTH_PARQUET, "\nRun R/01b_ingest_demographics.R first.")
demo_data <- arrow::read_parquet(OTH_PARQUET) %>% janitor::clean_names()
demo_data <- pad_keys(demo_data)

# Year ordering (prefer TA in race_data if present; otherwise use OTH)
year_levels <- race_data %>%
  filter(if ("reporting_category" %in% names(.)) reporting_category == "TA" else TRUE) %>%
  distinct(academic_year) %>% arrange(academic_year) %>% pull()
if (!length(year_levels)) {
  year_levels <- demo_data %>% distinct(academic_year) %>% arrange(academic_year) %>% pull()
}

# -------- Bring level/locale onto demo_data ----------------------------------
race_keys <- race_data %>%
  select(any_of(c("academic_year","district_code","school_code","level_strict3","locale_simple"))) %>%
  distinct()

demo_data <- demo_data %>%
  left_join(race_keys,
            by = c("academic_year","district_code","school_code"),
            relationship = "many-to-one") %>%
  {
    missing_idx <- is.na(.$level_strict3) | is.na(.$locale_simple)
    if (any(missing_idx)) {
      fill_df <- .[missing_idx, ] %>%
        select(-level_strict3, -locale_simple) %>%
        left_join(
          race_keys %>% select(academic_year, district_code, level_strict3, locale_simple) %>% distinct(),
          by = c("academic_year","district_code"),
          relationship = "many-to-one"
        )
      .[missing_idx, c("level_strict3","locale_simple")] <- fill_df[, c("level_strict3","locale_simple")]
    }
    .
  }

demo_data %>% summarise(
  pct_missing_level  = mean(is.na(level_strict3))*100,
  pct_missing_locale = mean(is.na(locale_simple))*100
) %>% print()

# -------- Demographic rates vs TA (pooled) -----------------------------------
cat("\n=== Calculating rates and disparities (pooled counts) ===\n")

# Subgroup (non-TA)
demo_rates <- demo_data %>%
  filter(category_type != "Total") %>%
  mutate(year_fct = factor(academic_year, levels = year_levels, ordered = TRUE)) %>%
  group_by(level_strict3, locale_simple, academic_year, year_fct, category_type, subgroup, subgroup_code) %>%
  summarise(
    susp   = sum(total_suspensions, na.rm = TRUE),
    enroll = sum(cumulative_enrollment, na.rm = TRUE),
    n_schools = dplyr::n(),
    .groups = "drop"
  ) %>%
  mutate(
    rate = safe_rate(susp, enroll, MIN_ENROLLMENT_THRESHOLD),
    sufficient_sample = enroll >= MIN_ENROLLMENT_THRESHOLD
  )

# TA (All Students)
ta_rates <- demo_data %>%
  filter(category_type == "Total") %>%
  mutate(year_fct = factor(academic_year, levels = year_levels, ordered = TRUE)) %>%
  group_by(level_strict3, locale_simple, academic_year, year_fct) %>%
  summarise(
    total_susp_TA = sum(total_suspensions, na.rm = TRUE),
    enroll_TA     = sum(cumulative_enrollment, na.rm = TRUE),
    rate_TA       = safe_rate(total_susp_TA, enroll_TA, MIN_ENROLLMENT_THRESHOLD),
    .groups = "drop"
  )

# Disparities vs TA
demo_disparities <- demo_rates %>%
  left_join(ta_rates, by = c("level_strict3","locale_simple","academic_year","year_fct")) %>%
  mutate(
    disparity_ratio = if_else(!is.na(rate) & !is.na(rate_TA) & rate_TA > 0, rate / rate_TA, NA_real_),
    disparity_diff  = if_else(!is.na(rate) & !is.na(rate_TA), rate - rate_TA, NA_real_)
  )

# -------- Within-category spreads (e.g., Male vs Female) ---------------------
cat("\n=== Within-category spreads ===\n")
within_category_disparities <- demo_disparities %>%
  filter(sufficient_sample) %>%
  group_by(level_strict3, locale_simple, academic_year, category_type) %>%
  filter(dplyr::n() >= 2) %>%
  summarise(
    n_groups   = dplyr::n(),
    max_rate   = max(rate, na.rm = TRUE),
    min_rate   = min(rate, na.rm = TRUE),
    spread_abs = max_rate - min_rate,
    spread_ratio = if_else(min_rate > 0, max_rate / min_rate, NA_real_),
    highest_group = subgroup[which.max(rate)],
    lowest_group  = subgroup[which.min(rate)],
    .groups = "drop"
  )

# -------- Ranking by average disparity vs TA ---------------------------------
demo_disparity_rank <- demo_disparities %>%
  filter(sufficient_sample) %>%
  group_by(level_strict3, locale_simple, category_type, subgroup, subgroup_code) %>%
  summarise(
    years_n      = dplyr::n(),
    avg_ratio_vs_all = mean(disparity_ratio[is.finite(disparity_ratio)], na.rm = TRUE),
    latest_ratio = dplyr::last(disparity_ratio[!is.na(disparity_ratio)]),
    avg_rate     = mean(rate, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  arrange(category_type, desc(avg_ratio_vs_all))

# -------- Intersectional summary (pooled-count ratios) -----------------------
cat("\n=== Intersectional pooled ratios (Sex, SPED, Socioecon) ===\n")

counts <- demo_data %>%
  filter(category_type %in% c("Sex","Special Education","Socioeconomic")) %>%
  group_by(level_strict3, locale_simple, academic_year, category_type, subgroup_code) %>%
  summarise(
    susp   = sum(total_suspensions, na.rm = TRUE),
    enroll = sum(cumulative_enrollment, na.rm = TRUE),
    .groups = "drop"
  )

# Replace the old ratio_from_codes() with this join-based version
ratio_from_codes <- function(df_counts, category_name, num_code, den_code, out_col) {
  keys <- c("level_strict3", "locale_simple", "academic_year")
  
  num <- df_counts %>%
    filter(category_type == category_name, subgroup_code == num_code) %>%
    select(all_of(keys), susp_num = susp, enroll_num = enroll)
  
  den <- df_counts %>%
    filter(category_type == category_name, subgroup_code == den_code) %>%
    select(all_of(keys), susp_den = susp, enroll_den = enroll)
  
  full_join(num, den, by = keys) %>%
    mutate(
      !!out_col :=
        safe_rate(susp_num, enroll_num, MIN_ENROLLMENT_THRESHOLD) /
        safe_rate(susp_den, enroll_den, MIN_ENROLLMENT_THRESHOLD)
    ) %>%
    select(all_of(keys), !!out_col)
}

sex_ratios <- ratio_from_codes(counts, "Sex",              "SM", "SF", "male_female_ratio")
sped_ratio <- ratio_from_codes(counts, "Special Education","SE", "SN", "sped_ratio")
sed_ratio  <- ratio_from_codes(counts, "Socioeconomic",    "SD", "NS", "sed_ratio")

demo_summary_by_setting <- list(sex_ratios, sped_ratio, sed_ratio) %>%
  Reduce(function(x, y) dplyr::left_join(x, y,
                                         by = c("level_strict3","locale_simple","academic_year")), .)

# Optional: extend for EL / Foster / Homeless by uncommenting below:
# el_ratio     <- ratio_from_codes(counts, "English Learner", "EL", "EO", "el_ratio")
# foster_ratio <- ratio_from_codes(counts, "Foster",          "FY", "NF", "foster_ratio")
# homeless_ratio <- ratio_from_codes(counts, "Homeless",      "HL", "NH", "homeless_ratio")
# demo_summary_by_setting <- list(demo_summary_by_setting, el_ratio, foster_ratio, homeless_ratio) %>%
#   Reduce(function(x, y) dplyr::left_join(x, y, by = c("level_strict3","locale_simple","academic_year")), .)

# -------- Output -------------------------------------------------------------
outdir <- here("outputs"); dir.create(outdir, recursive = TRUE, showWarnings = FALSE)
ts <- format(Sys.time(), "%Y%m%d_%H%M%S")

readr::write_csv(demo_disparities,            file.path(outdir, paste0("EDA_demographic_disparities_", ts, ".csv")))
readr::write_csv(within_category_disparities, file.path(outdir, paste0("EDA_within_demographic_spreads_", ts, ".csv")))
readr::write_csv(demo_disparity_rank,         file.path(outdir, paste0("EDA_demographic_disparity_rankings_", ts, ".csv")))
readr::write_csv(demo_summary_by_setting,     file.path(outdir, paste0("EDA_demographic_summary_by_setting_", ts, ".csv")))

# Console summary
cat("\n========== DEMOGRAPHIC ANALYSIS SUMMARY ==========\n")
ctypes <- sort(unique(demo_disparities$category_type))
cat("Categories analyzed:", paste(ctypes, collapse=", "), "\n")
cat("Settings with data:", dplyr::n_distinct(demo_disparities$level_strict3, demo_disparities$locale_simple), "\n")

cat("\nHighest disparities by category:\n")
top_disparities <- demo_disparity_rank %>% group_by(category_type) %>%
  slice_max(avg_ratio_vs_all, n = 1, with_ties = FALSE) %>%
  select(category_type, subgroup, avg_ratio_vs_all)
print(top_disparities, n = Inf)

cat("\nLargest within-category gaps:\n")
max_gaps <- within_category_disparities %>% group_by(category_type) %>%
  slice_max(spread_ratio, n = 1, with_ties = FALSE) %>%
  select(category_type, level_strict3, locale_simple, spread_ratio, highest_group, lowest_group)
print(max_gaps, n = Inf)
cat("\n=================================================\n")

# Flags (thresholds are easy to tune)
merged_summary <- demo_summary_by_setting %>%
  mutate(
    data_source = "demographic",
    high_sped_disparity   = sped_ratio > 2,
    high_gender_disparity = abs(male_female_ratio - 1) > 0.5,
    high_sed_disparity    = sed_ratio > 1.5
  )
readr::write_csv(merged_summary, file.path(outdir, paste0("EDA_demographic_flags_for_merge_", ts, ".csv")))

cat("\nMerge-ready demographic summary saved. Join with race data on:\n- level_strict3\n- locale_simple\n- academic_year\n")
# EOF