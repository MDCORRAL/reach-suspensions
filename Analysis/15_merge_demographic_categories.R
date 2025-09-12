# analysis/15_merge_demographic_categories.R
# Merge additional demographic categories with race data for intersectional EDA
# analysis/15_merge_demographic_categories.R
# Purpose: Merge additional demographic categories with race data for intersectional EDA
# Inputs:  - data-stage/susp_v6_long.parquet (race/ethnicity suspension data)
#          - data-stage/oth_long.parquet (other demographic categories)
# Outputs: - 15_demographic_*.xlsx (disparity analysis)
#          - 15_demographic_flags.csv (merge-ready summary)
# Dependencies: R/utils_keys_filters.R, R/01b_ingest_demographics.R
# Canonical subgroup term: Students with Disabilities (SWD)

suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(arrow)
  library(here)
  library(readr)
  library(janitor)
  library(stringr)
  library(writexl)
  library(purrr)
})

try(here::i_am("Analysis/15_merge_demographic_categories.R"), silent = TRUE)

# Load repository utilities
source(here("R", "utils_keys_filters.R"))
source(here("R", "demographic_labels.R"))

# -------- Config -------------------------------------------------------------
RACE_DATA_PATH <- here("data-stage", "susp_v6_long.parquet")
OTH_PARQUET    <- here("data-stage", "oth_long.parquet")
MIN_ENROLLMENT_THRESHOLD <- 10

# Helper functions
safe_rate <- function(susp, enroll, min_enroll=0) {
  dplyr::if_else(enroll > min_enroll, susp / enroll, NA_real_)
}

# String standardization helper
norm <- function(x) {
  x %>%
    stringr::str_replace_all("\u2013|\u2014", "-") %>%  # en/em dash -> hyphen
    stringr::str_squish()
}

# -------- Load and validate data ---------------------------------------------
cat("\n=== Loading race data ===\n")
race_data <- arrow::read_parquet(RACE_DATA_PATH) %>% 
  janitor::clean_names()

# Create academic_year if needed
if (!"academic_year" %in% names(race_data) && "year" %in% names(race_data)) {
  race_data <- race_data %>%
    mutate(academic_year = ifelse(!is.na(year), paste0(year - 1, "-", substr(year,3,4)), NA_character_))
}

# Apply repository utilities for key standardization
race_data <- build_keys(race_data)

cat("\n=== Loading OTH demographic parquet ===\n")
if (!file.exists(OTH_PARQUET)) {
  stop("Missing file: ", OTH_PARQUET, "\nRun R/01b_ingest_demographics.R first.")
}

demo_data <- arrow::read_parquet(OTH_PARQUET) %>% 
  janitor::clean_names() %>%
  build_keys()

# Validate required categories
must_have <- c("Sex","Special Education","Socioeconomic",
               "English Learner","Foster","Migrant","Homeless","Total")
missing <- setdiff(must_have, unique(demo_data$category_type))
if (length(missing)) {
  stop("[15] Missing categories after read: ", paste(missing, collapse = ", "),
       "\nCheck OTH_PARQUET path or re-run 01b.")
}

cat("\n[15] Categories present:", paste(sort(unique(demo_data$category_type)), collapse=", "), "\n")

# -------- Demographic standardization ---------------------------------------
demo_data <- demo_data %>%
  mutate(
    subgroup_raw = subgroup,
    subgroup = norm(subgroup),
    category_type = norm(category_type)
  ) %>%
  canonicalize_demo(desc_col = "subgroup", code_col = "subgroup_code")

# -------- Data quality validation -------------------------------------------
# Check for duplicates
dup_check <- demo_data %>%
  count(academic_year, district_code, school_code, category_type, subgroup) %>%
  filter(n > 1)

if (nrow(dup_check) > 0) {
  warning("Duplicate unit-year-category-subgroup rows found: ", nrow(dup_check))
}

# Check for impossible rates
rate_anomalies <- demo_data %>%
  filter(total_suspensions > cumulative_enrollment, 
         !is.na(total_suspensions), !is.na(cumulative_enrollment))

if(nrow(rate_anomalies) > 0) {
  warning("Found ", nrow(rate_anomalies), " records with suspensions > enrollment")
}

# Add after data quality validation section
demo_data <- demo_data %>%
  mutate(
    # Flag and cap impossible rates
    rate_flag = total_suspensions > cumulative_enrollment,
    total_suspensions = pmin(total_suspensions, cumulative_enrollment, na.rm = TRUE)
  )

cat("Capped", sum(demo_data$rate_flag, na.rm = TRUE), "impossible suspension counts\n")

# -------- Create canonical school attributes --------------------------------
# Year ordering (prefer TA in race_data if present)
year_levels <- race_data %>%
  filter(if ("subgroup" %in% names(.)) category_type == "Race/Ethnicity", subgroup == "All Students" else TRUE) %>%
  distinct(academic_year) %>% arrange(academic_year) %>% pull()

if (!length(year_levels)) {
  year_levels <- demo_data %>% distinct(academic_year) %>% arrange(academic_year) %>% pull()
}

# Create canonical school attributes from race data
canon_keys <- race_data %>%
  filter_campus_only() %>%
  dplyr::select(dplyr::any_of(c(
    "academic_year","county_code","district_code","school_code","cds_school",
    "ed_ops_name","school_level","locale_simple"
  ))) %>%
  dplyr::distinct()

# Prefer canonical 14-digit key for joining
join_keys <- if("cds_school" %in% names(demo_data) && "cds_school" %in% names(canon_keys)) {
  c("academic_year", "cds_school")
} else {
  intersect(
    c("academic_year","county_code","district_code","school_code"),
    names(demo_data)
  )
}

# Perform main join to add school attributes
# Perform main join to add school attributes
demo_data <- demo_data %>%
  dplyr::left_join(
    canon_keys,
    by = join_keys,
    relationship = "many-to-one"
  ) %>%
  dplyr::mutate(
    setting = dplyr::case_when(
      ed_ops_name == "Traditional" ~ "Traditional",
      !is.na(ed_ops_name) ~ "Non-traditional",
      TRUE ~ NA_character_
    ),
    # Fix impossible suspension rates
    rate_flag = total_suspensions > cumulative_enrollment,
    total_suspensions = pmin(total_suspensions, cumulative_enrollment, na.rm = TRUE)
  ) %>%
  # Clean duplicate columns from join
  select(-any_of(c("county_code.x", "district_code.x", "school_code.x"))) %>%
  rename_with(~ str_remove(.x, "\\.y$"), ends_with(".y"))

# Report data cleaning
cat("Fixed", sum(demo_data$rate_flag, na.rm = TRUE), "impossible suspension rates\n")

# -------- District-level fallback for missing attributes --------------------
missing_count <- demo_data %>% 
  summarise(
    missing_level = sum(is.na(school_level)),
    missing_locale = sum(is.na(locale_simple))
  )

if(missing_count$missing_level > 0 || missing_count$missing_locale > 0) {
  cat("Applying district-level fallback for", missing_count$missing_level, "records\n")
  
  # Check what's available in both datasets
  demo_cols <- names(demo_data)
  canon_cols <- names(canon_keys)
  
  cat("Demo data columns:", paste(demo_cols[grepl("district|county|cds", demo_cols)], collapse = ", "), "\n")
  cat("Canon keys columns:", paste(canon_cols[grepl("district|county|cds", canon_cols)], collapse = ", "), "\n")
  
  # Try cds_district first (7-digit district code)
  if("cds_district" %in% demo_cols && "cds_district" %in% canon_cols) {
    cat("Using cds_district for fallback join\n")
    
    district_attrs <- canon_keys %>%
      filter(!is.na(school_level), !is.na(locale_simple)) %>%
      group_by(academic_year, cds_district) %>%
      summarise(
        level_mode = names(sort(table(school_level), decreasing = TRUE))[1],
        locale_mode = names(sort(table(locale_simple), decreasing = TRUE))[1],
        .groups = "drop"
      )
    
    demo_data <- demo_data %>%
      left_join(district_attrs, by = c("academic_year", "cds_district")) %>%
      mutate(
        school_level = coalesce(school_level, level_mode),
        locale_simple = coalesce(locale_simple, locale_mode)
      ) %>%
      select(-level_mode, -locale_mode)
    
    cat("✓ Applied district-level fallback using cds_district\n")
    
    # Fallback to county_code + district_code
  } else if(all(c("county_code", "district_code") %in% demo_cols) && 
            all(c("county_code", "district_code") %in% canon_cols)) {
    cat("Using county_code + district_code for fallback join\n")
    
    district_attrs <- canon_keys %>%
      filter(!is.na(school_level), !is.na(locale_simple)) %>%
      group_by(academic_year, county_code, district_code) %>%
      summarise(
        level_mode = names(sort(table(school_level), decreasing = TRUE))[1],
        locale_mode = names(sort(table(locale_simple), decreasing = TRUE))[1],
        .groups = "drop"
      )
    
    demo_data <- demo_data %>%
      left_join(district_attrs, by = c("academic_year", "county_code", "district_code")) %>%
      mutate(
        school_level = coalesce(school_level, level_mode),
        locale_simple = coalesce(locale_simple, locale_mode)
      ) %>%
      select(-level_mode, -locale_mode)
    
    cat("✓ Applied district-level fallback using county_code + district_code\n")
    
  } else {
    # Skip fallback and proceed with available data
    cat("Skipping district-level fallback - incompatible column structure\n")
    cat("Proceeding with", round((1 - missing_count$missing_level/nrow(demo_data))*100, 1), "% coverage\n")
  }
}
# -------- Calculate demographic rates and disparities -----------------------
cat("\n=== Calculating rates and disparities ===\n")

# Subgroup rates (non-Total)
demo_rates <- demo_data %>%
  filter(category_type != "Total") %>%
  mutate(year_fct = factor(academic_year, levels = year_levels, ordered = TRUE)) %>%
  group_by(school_level, locale_simple, academic_year, year_fct, category_type, subgroup, subgroup_code) %>%
  summarise(
    susp = sum(total_suspensions, na.rm = TRUE),
    enroll = sum(cumulative_enrollment, na.rm = TRUE),
    n_schools = dplyr::n(),
    .groups = "drop"
  ) %>%
  mutate(
    rate = safe_rate(susp, enroll, MIN_ENROLLMENT_THRESHOLD),
    sufficient_sample = enroll >= MIN_ENROLLMENT_THRESHOLD
  )

# Total All Students (TA) rates for comparison
ta_rates <- demo_data %>%
  filter(category_type == "Total") %>%
  mutate(year_fct = factor(academic_year, levels = year_levels, ordered = TRUE)) %>%
  group_by(school_level, locale_simple, academic_year, year_fct) %>%
  summarise(
    total_susp_TA = sum(total_suspensions, na.rm = TRUE),
    enroll_TA = sum(cumulative_enrollment, na.rm = TRUE),
    rate_TA = safe_rate(total_susp_TA, enroll_TA, MIN_ENROLLMENT_THRESHOLD),
    .groups = "drop"
  )

# Calculate disparities vs Total All Students
demo_disparities <- demo_rates %>%
  left_join(ta_rates, by = c("school_level","locale_simple","academic_year","year_fct")) %>%
  mutate(
    disparity_ratio = if_else(!is.na(rate) & !is.na(rate_TA) & rate_TA > 0, rate / rate_TA, NA_real_),
    disparity_diff = if_else(!is.na(rate) & !is.na(rate_TA), rate - rate_TA, NA_real_)
  )

# -------- Within-category spreads analysis ----------------------------------
cat("\n=== Analyzing within-category spreads ===\n")

within_category_disparities <- demo_disparities %>%
  filter(sufficient_sample) %>%
  group_by(school_level, locale_simple, academic_year, category_type) %>%
  filter(dplyr::n() >= 2) %>%
  summarise(
    n_groups = dplyr::n(),
    max_rate = max(rate, na.rm = TRUE),
    min_rate = min(rate, na.rm = TRUE),
    spread_abs = max_rate - min_rate,
    spread_ratio = if_else(min_rate > 0, max_rate / min_rate, NA_real_),
    highest_group = subgroup[which.max(rate)],
    lowest_group = subgroup[which.min(rate)],
    .groups = "drop"
  )

# -------- Ranking by average disparity --------------------------------------
demo_disparity_rank <- demo_disparities %>%
  filter(sufficient_sample) %>%
  group_by(school_level, locale_simple, category_type, subgroup, subgroup_code) %>%
  summarise(
    years_n = dplyr::n(),
    avg_ratio_vs_all = mean(disparity_ratio[is.finite(disparity_ratio)], na.rm = TRUE),
    latest_ratio = dplyr::last(disparity_ratio[!is.na(disparity_ratio)]),
    avg_rate = mean(rate, na.rm = TRUE),
    total_enrollment = sum(enroll, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  arrange(category_type, desc(avg_ratio_vs_all))

# -------- Robust intersectional ratio calculations -------------------------
# -------- Intersectional summary using TA-based disparities -----------------
cat("\n=== Creating intersectional summary using TA comparisons ===\n")

# Extract disparity ratios vs TA for each demographic category
# Extract disparity ratios vs TA for each demographic category
demo_summary_by_setting <- demo_disparities %>%
  filter(sufficient_sample, !is.na(disparity_ratio)) %>%
  select(school_level, locale_simple, academic_year, category_type, subgroup, disparity_ratio) %>%
  # Create meaningful column names for key demographics
  mutate(
    demographic_indicator = case_when(
      category_type == "Sex" & subgroup == "Male" ~ "male_vs_ta_ratio",
      category_type == "Sex" & subgroup == "Female" ~ "female_vs_ta_ratio", 
      category_type == "Special Education" & subgroup == "Students with Disabilities" ~ "swd_vs_ta_ratio",
      category_type == "Socioeconomic" & subgroup == "Socioeconomically Disadvantaged" ~ "sed_vs_ta_ratio",
      category_type == "English Learner" & subgroup == "English Learner" ~ "el_vs_ta_ratio",
      category_type == "Foster" & subgroup == "Foster Youth" ~ "foster_vs_ta_ratio",
      category_type == "Migrant" & subgroup == "Migrant" ~ "migrant_vs_ta_ratio",
      category_type == "Homeless" & subgroup == "Homeless" ~ "homeless_vs_ta_ratio",
      TRUE ~ NA_character_
    )
  ) %>%
  filter(!is.na(demographic_indicator)) %>%
  # Pivot wider to get columns for each demographic
  pivot_wider(
    names_from = demographic_indicator,
    values_from = disparity_ratio,
    values_fn = mean
  )

# Calculate male/female ratio separately to ensure it works
sex_ratios <- demo_disparities %>%
  filter(sufficient_sample, !is.na(disparity_ratio), category_type == "Sex") %>%
  select(school_level, locale_simple, academic_year, subgroup, disparity_ratio) %>%
  pivot_wider(names_from = subgroup, values_from = disparity_ratio, values_fn = mean) %>%
  mutate(
    male_female_ratio = if_else(
      !is.na(Male) & !is.na(Female) & Female > 0,
      Male / Female,
      NA_real_
    )
  ) %>%
  select(school_level, locale_simple, academic_year, male_female_ratio)

# Join back to main summary
demo_summary_by_setting <- demo_summary_by_setting %>%
  left_join(sex_ratios, by = c("school_level", "locale_simple", "academic_year")) %>%
  # Remove individual male/female ratios, keep the comparison
  select(-any_of(c("male_vs_ta_ratio", "female_vs_ta_ratio"))) %>%
  # Rename for consistency
  rename(
    swd_ratio = swd_vs_ta_ratio,
    sed_ratio = sed_vs_ta_ratio,
    el_ratio = el_vs_ta_ratio,
    foster_ratio = foster_vs_ta_ratio,
    migrant_ratio = migrant_vs_ta_ratio,
    homeless_ratio = homeless_vs_ta_ratio
  )

# Verify the output
cat("Intersectional summary dimensions:", nrow(demo_summary_by_setting), "rows x", ncol(demo_summary_by_setting), "columns\n")
cat("Available demographic ratios:\n")
ratio_cols <- names(demo_summary_by_setting)[grepl("_ratio$", names(demo_summary_by_setting))]
for(col in ratio_cols) {
  non_na_count <- sum(!is.na(demo_summary_by_setting[[col]]))
  cat(" -", col, ":", non_na_count, "non-NA values\n")
}
# -------- Enhanced extreme disparities (no join needed) ---------------------
extreme_disparities <- demo_disparities %>%
  filter(disparity_ratio > 5, !is.na(disparity_ratio), sufficient_sample) %>%
  # All the data we need is already in demo_disparities!
  mutate(
    # Check if we have complete data for confidence intervals
    has_complete_data = !is.na(susp) & !is.na(enroll) & !is.na(total_susp_TA) & !is.na(enroll_TA) &
      susp > 0 & enroll >= 10 & total_susp_TA > 0 & enroll_TA >= 10,
    
    # Standard error for log ratio (robust calculation)
    se_log_ratio = if_else(
      has_complete_data,
      sqrt(1/susp + 1/total_susp_TA - 1/enroll - 1/enroll_TA),
      NA_real_
    ),
    
    # Confidence intervals for the disparity ratio
    ci_lower = if_else(
      has_complete_data & !is.na(se_log_ratio) & se_log_ratio > 0,
      exp(log(disparity_ratio) - 1.96 * se_log_ratio),
      NA_real_
    ),
    ci_upper = if_else(
      has_complete_data & !is.na(se_log_ratio) & se_log_ratio > 0,
      exp(log(disparity_ratio) + 1.96 * se_log_ratio),
      NA_real_
    ),
    
    # Statistical significance (CI doesn't include 1)
    statistically_significant = !is.na(ci_lower) & ci_lower > 1
  ) %>%
  # Summarize by category and subgroup
  group_by(category_type, subgroup) %>%
  summarise(
    observations = n(),
    settings_n = n_distinct(school_level, locale_simple),
    years_n = n_distinct(academic_year),
    
    # Disparity statistics
    avg_ratio_vs_all = mean(disparity_ratio, na.rm = TRUE),
    median_ratio = median(disparity_ratio, na.rm = TRUE),
    min_ratio = min(disparity_ratio, na.rm = TRUE),
    max_ratio = max(disparity_ratio, na.rm = TRUE),
    
    # Sample size information
    total_enrollment = sum(enroll, na.rm = TRUE),
    total_suspensions = sum(susp, na.rm = TRUE),
    avg_subgroup_rate = mean(rate, na.rm = TRUE),
    avg_overall_rate = mean(rate_TA, na.rm = TRUE),
    
    # Statistical significance
    significant_observations = sum(statistically_significant, na.rm = TRUE),
    pct_significant = round(mean(statistically_significant, na.rm = TRUE) * 100, 1),
    
    # Confidence interval summaries (where available)
    ci_data_available = sum(has_complete_data, na.rm = TRUE),
    avg_ci_lower = mean(ci_lower, na.rm = TRUE),
    avg_ci_upper = mean(ci_upper, na.rm = TRUE),
    
    # Reliability assessment
    sample_reliability = case_when(
      total_enrollment >= 1000 & years_n >= 3 ~ "High",
      total_enrollment >= 100 & years_n >= 2 ~ "Medium", 
      TRUE ~ "Low"
    ),
    
    .groups = "drop"
  ) %>%
  arrange(desc(avg_ratio_vs_all))

# Summary report
cat("\n=== Extreme Disparities Summary ===\n")
cat("Found", nrow(extreme_disparities), "subgroups with >5x disparity rates\n")

if(nrow(extreme_disparities) > 0) {
  cat("\nStatistical significance available for", 
      sum(extreme_disparities$ci_data_available > 0), "subgroups\n")
  
  sig_count <- sum(extreme_disparities$significant_observations > 0, na.rm = TRUE)
  cat("Subgroups with statistically significant disparities:", sig_count, "\n")
  
  # Show top extreme cases
  cat("\nTop 3 extreme disparities:\n")
  print(extreme_disparities %>% 
          select(category_type, subgroup, avg_ratio_vs_all, total_enrollment, 
                 sample_reliability, pct_significant) %>%
          head(3))
}

# -------- Create output flags -----------------------------------------------
merged_summary <- demo_summary_by_setting %>%
  mutate(
    data_source = "demographic",
    high_swd_disparity = swd_ratio > 2,
    high_gender_disparity = abs(male_female_ratio - 1) > 0.5,
    high_sed_disparity = sed_ratio > 1.5,
    high_foster_disparity = foster_ratio > 10,
    high_el_disparity = el_ratio > 1.5,
    high_homeless_disparity = homeless_ratio > 2
  )

# -------- Generate outputs --------------------------------------------------
outdir <- here("outputs")
dir.create(outdir, recursive = TRUE, showWarnings = FALSE)

# Standardized output files
output_files <- list(
  "15_demographic_disparities.xlsx" = demo_disparities,
  "15_within_category_spreads.xlsx" = within_category_disparities,
  "15_disparity_rankings.xlsx" = demo_disparity_rank,
  "15_summary_by_setting.xlsx" = demo_summary_by_setting,
  "15_extreme_disparities.xlsx" = extreme_disparities,
  "15_demographic_flags.csv" = merged_summary
)

# Write individual files
iwalk(output_files, ~ {
  filepath <- file.path(outdir, .y)
  if(str_ends(.y, ".xlsx")) {
    write_xlsx(.x, filepath)
  } else {
    write_csv(.x, filepath)
  }
  cat("Wrote:", .y, "\n")
})

# Combined Excel workbook
write_xlsx(
  list(
    disparities = demo_disparities,
    spreads = within_category_disparities,
    rankings = demo_disparity_rank,
    summary = demo_summary_by_setting,
    extreme = extreme_disparities,
    flags = merged_summary
  ),
  path = file.path(outdir, "15_demographic_analysis_complete.xlsx")
)

# -------- Console summary ---------------------------------------------------
cat("\n========== DEMOGRAPHIC ANALYSIS SUMMARY ==========\n")
cat("Categories analyzed:", paste(sort(unique(demo_disparities$category_type)), collapse=", "), "\n")
cat("Settings with data:", dplyr::n_distinct(demo_disparities$school_level, demo_disparities$locale_simple), "\n")
cat("Years covered:", paste(sort(unique(demo_disparities$academic_year)), collapse=", "), "\n")

cat("\nHighest disparities by category:\n")
top_disparities <- demo_disparity_rank %>% 
  group_by(category_type) %>%
  slice_max(avg_ratio_vs_all, n = 1, with_ties = FALSE) %>%
  select(category_type, subgroup, avg_ratio_vs_all, total_enrollment)
print(top_disparities, n = Inf)

cat("\nLargest within-category gaps:\n")
max_gaps <- within_category_disparities %>% 
  group_by(category_type) %>%
  slice_max(spread_ratio, n = 1, with_ties = FALSE, na_rm = TRUE) %>%
  select(category_type, school_level, locale_simple, spread_ratio, highest_group, lowest_group)
print(max_gaps, n = Inf)

cat("\nExtreme disparities (>5x rate):\n")
if(nrow(extreme_disparities) > 0) {
  print(extreme_disparities %>% select(category_type, subgroup, avg_ratio_vs_all, settings_n, total_enrollment))
} else {
  cat("None found.\n")
}

# Validation checks
expected_cols <- c("male_female_ratio", "swd_ratio", "sed_ratio", "el_ratio")
missing_cols <- setdiff(expected_cols, names(demo_summary_by_setting))

if(length(missing_cols) > 0) {
  warning("Missing required columns: ", paste(missing_cols, collapse=", "))
} else {
  cat("\n✓ All required ratio columns present\n")
}

cat("✓ Analysis complete. Generated", nrow(demo_summary_by_setting), "setting-year combinations\n")
cat("✓ Merge-ready summary available for joining with race data on: school_level, locale_simple, academic_year\n")
cat("=================================================\n")

# EOF