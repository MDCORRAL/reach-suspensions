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
  library(writexl)
  
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

# GUARDRAIL + quick print HERE 
must_have <- c("Sex","Special Education","Socioeconomic",
               "English Learner","Foster","Migrant","Homeless","Total")
missing <- setdiff(must_have, unique(demo_data$category_type))
if (length(missing)) {
  stop("[15] Missing categories after read: ", paste(missing, collapse = ", "),
       "\nCheck OTH_PARQUET path or re-run 01b.")
}

# --- DEBUG: what's actually in 15_merge right now?
cat("\n[15] Categories present right after read:\n")
print(demo_data %>% count(category_type) %>% arrange(desc(n)))

cat("\n[15] Example codes per category:\n")
print(demo_data %>% group_by(category_type) %>% summarise(codes = paste(head(unique(subgroup_code),6), collapse=", ")))

# --- Normalize subgroup labels and enforce category_type ----------------------
# Put this immediately after: demo_data <- pad_keys(demo_data)

# helper: quick string standardization
norm <- function(x) {
  x %>%
    stringr::str_replace_all("\u2013|\u2014", "-") %>%  # en/em dash -> hyphen
    stringr::str_squish()
}

demo_data <- demo_data %>%
  mutate(
    subgroup_raw = subgroup,
    subgroup = norm(subgroup),
    category_type = norm(category_type)
  )

# Canonicalize common subgroup variants (extend as needed)
demo_data <- demo_data %>%
  mutate(
    subgroup_std = dplyr::case_when(
      # Sex/gender
      stringr::str_to_lower(subgroup) %in% c("female") ~ "Female",
      stringr::str_to_lower(subgroup) %in% c("male") ~ "Male",
      stringr::str_detect(stringr::str_to_lower(subgroup), "non[- ]?binary") ~ "Non-Binary",
      stringr::str_to_lower(subgroup) %in% c("missing gender") ~ "Missing Gender",
      stringr::str_to_lower(subgroup) %in% c("not reported") ~ "Not Reported",
      
      # Special Education
      stringr::str_to_lower(subgroup) %in% c("students with disabilities","swd") ~ "Students with Disabilities",
      stringr::str_to_lower(subgroup) %in% c("special education") ~ "Students with Disabilities",
      
      # Socioeconomic
      stringr::str_detect(subgroup, regex("^socioeconomically disadvantaged$", ignore_case=TRUE)) ~ "Socioeconomically Disadvantaged",
      
      # English Learner (if present in OTH)
      stringr::str_to_lower(subgroup) %in% c("english learner","el") ~ "English Learner",
      stringr::str_to_lower(subgroup) %in% c("english only","eo") ~ "English Only",
      
      # Foster / Homeless (if present)
      stringr::str_to_lower(subgroup) %in% c("foster youth","foster") ~ "Foster Youth",
      stringr::str_to_lower(subgroup) %in% c("homeless") ~ "Homeless",
      
      TRUE ~ subgroup
    )
  )

# Map subgroup -> enforced category_type
demo_data <- demo_data %>%
  mutate(
    category_type_enforced = dplyr::case_when(
      subgroup_std %in% c("Female","Male","Non-Binary","Missing Gender","Not Reported") ~ "Sex",
      subgroup_std %in% c("Students with Disabilities")                                ~ "Special Education",
      subgroup_std %in% c("Socioeconomically Disadvantaged")                           ~ "Socioeconomic",
      subgroup_std %in% c("English Learner","English Only")                            ~ "English Learner",
      subgroup_std %in% c("Foster Youth")                                              ~ "Foster",
      subgroup_std %in% c("Homeless")                                                  ~ "Homeless",
      TRUE ~ category_type  # keep original for everything else (including "Total")
    )
  )

# For sex-like subgroups, DROP any row whose category_type != Sex (same logic for others)
demo_data <- demo_data %>%
  mutate(
    keep_row = dplyr::case_when(
      category_type_enforced == "Sex"             ~ category_type == "Sex",
      category_type_enforced == "Special Education" ~ category_type == "Special Education",
      category_type_enforced == "Socioeconomic"     ~ category_type == "Socioeconomic",
      category_type_enforced == "English Learner"   ~ category_type == "English Learner",
      category_type_enforced == "Foster"            ~ category_type == "Foster",
      category_type_enforced == "Homeless"          ~ category_type == "Homeless",
      TRUE ~ TRUE
    )
  ) %>%
  filter(keep_row) %>%
  select(-keep_row)

# Replace original fields with standardized ones
demo_data <- demo_data %>%
  mutate(
    subgroup = subgroup_std,
    category_type = category_type_enforced
  ) %>%
  select(-subgroup_std, -category_type_enforced)

# Optional: assert uniqueness before aggregation
# One row per unit-year-category-subgroup (prevents silent double counting)
dup_check <- demo_data %>%
  count(academic_year, district_code, school_code, category_type, subgroup) %>%
  filter(n > 1)
if (nrow(dup_check) > 0) {
  warning("Duplicate unit-year-category-subgroup rows remain after normalization. Inspect `dup_check`.")
}

# Year ordering (prefer TA in race_data if present; otherwise use OTH)
year_levels <- race_data %>%
  filter(if ("reporting_category" %in% names(.)) reporting_category == "TA" else TRUE) %>%
  distinct(academic_year) %>% arrange(academic_year) %>% pull()
if (!length(year_levels)) {
  year_levels <- demo_data %>% distinct(academic_year) %>% arrange(academic_year) %>% pull()
}

# -------- Now bring canonical school attributes onto demo_data -------------------
# Source of truth: race_data (susp_v5.parquet already loaded & padded above)
canon_keys <- race_data %>%
  dplyr::select(dplyr::any_of(c(
    "academic_year","county_code","district_code","school_code",
    "ed_ops_name","school_level_final","level_strict3","locale_simple"
  ))) %>%
  dplyr::distinct()

# Join using whatever keys exist in demo_data (prefer full 4-key match)
join_keys <- intersect(
  c("academic_year","county_code","district_code","school_code"),
  names(demo_data)
)

demo_data <- demo_data %>%
  dplyr::left_join(
    canon_keys,
    by = join_keys,
    relationship = "many-to-one"
  ) %>%
  dplyr::mutate(
    setting = dplyr::case_when(
      ed_ops_name == "Traditional" ~ "Traditional",
      !is.na(ed_ops_name)          ~ "Non-traditional",
      TRUE                         ~ NA_character_
    )
  ) %>%
  {
    # Keep the district-level fallback ONLY for level/locale (not ed_ops_name/school_level_final)
    missing_idx <- is.na(.$level_strict3) | is.na(.$locale_simple)
    if (any(missing_idx)) {
      fill_df <- .[missing_idx, ] %>%
        dplyr::select(-level_strict3, -locale_simple) %>%
        dplyr::left_join(
          canon_keys %>%
            dplyr::select(academic_year, district_code, level_strict3, locale_simple) %>%
            dplyr::distinct(),
          by = c("academic_year","district_code"),
          relationship = "many-to-one"
        )
      .[missing_idx, c("level_strict3","locale_simple")] <- fill_df[, c("level_strict3","locale_simple")]
    }
    .
  }

# Quick sanity: attributes should now be present
demo_data %>%
  dplyr::summarise(
    pct_na_ed_ops         = mean(is.na(ed_ops_name)) * 100,
    pct_na_school_level   = mean(is.na(school_level_final)) * 100,
    pct_na_level_strict3  = mean(is.na(level_strict3)) * 100,
    pct_na_locale_simple  = mean(is.na(locale_simple)) * 100
  ) %>% print()
# -------- Sanity: demo_data should have susp/enroll counts --------------------

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

# --- Helper: pooled-rate ratio from code sets (robust to missing sides) -----
ratio_from_codes <- function(df_counts, category_name, num_codes, den_codes, out_col) {
  keys <- c("level_strict3", "locale_simple", "academic_year")
  
  num <- df_counts %>%
    dplyr::filter(category_type == category_name, subgroup_code %in% num_codes) %>%
    dplyr::group_by(dplyr::across(dplyr::all_of(keys))) %>%
    dplyr::summarise(susp_num = sum(susp, na.rm = TRUE),
                     enroll_num = sum(enroll, na.rm = TRUE), .groups = "drop")
  
  den <- df_counts %>%
    dplyr::filter(category_type == category_name, subgroup_code %in% den_codes) %>%
    dplyr::group_by(dplyr::across(dplyr::all_of(keys))) %>%
    dplyr::summarise(susp_den = sum(susp, na.rm = TRUE),
                     enroll_den = sum(enroll, na.rm = TRUE), .groups = "drop")
  
  dplyr::full_join(num, den, by = keys) %>%
    dplyr::mutate(
      !!out_col := safe_rate(susp_num, enroll_num, MIN_ENROLLMENT_THRESHOLD) /
        safe_rate(susp_den, enroll_den, MIN_ENROLLMENT_THRESHOLD)
    ) %>%
    dplyr::select(dplyr::all_of(keys), !!out_col)
}

# -------- Intersectional summary (pooled-count ratios) -----------------------
cat("\n=== Intersectional pooled ratios (Sex, SPED, Socioecon, EL, Foster, Migrant, Homeless) ===\n")

counts <- demo_data %>%
  filter(category_type %in% c(
    "Sex","Special Education","Socioeconomic",
    "English Learner","Foster","Migrant","Homeless"
  )) %>%
  group_by(level_strict3, locale_simple, academic_year, category_type, subgroup_code) %>%
  summarise(
    susp   = sum(total_suspensions, na.rm = TRUE),
    enroll = sum(cumulative_enrollment, na.rm = TRUE),
    .groups = "drop"
  )

sex_ratios     <- ratio_from_codes(counts, "Sex",              "SM", "SF", "male_female_ratio")
sped_ratio     <- ratio_from_codes(counts, "Special Education","SE", "SN", "sped_ratio")
sed_ratio      <- ratio_from_codes(counts, "Socioeconomic",    "SD", "NS", "sed_ratio")
el_ratio <- ratio_from_codes(
  counts, "English Learner",
  num_codes = c("EL"),
  den_codes = c("EO","IFEP","RFEP","TBD"),
  out_col   = "el_ratio"
)
foster_ratio   <- ratio_from_codes(counts, "Foster",           "FY", "NF", "foster_ratio")
migrant_ratio  <- ratio_from_codes(counts, "Migrant",          "MG", "NM", "migrant_ratio")
homeless_ratio <- ratio_from_codes(counts, "Homeless",         "HL", "NH", "homeless_ratio")

demo_summary_by_setting <- list(
  sex_ratios, sped_ratio, sed_ratio,
  el_ratio, foster_ratio, migrant_ratio, homeless_ratio
) %>%
  Reduce(function(x, y) left_join(x, y,
                                  by = c("level_strict3","locale_simple","academic_year")), .)


# -------- Output -------------------------------------------------------------
outdir <- here("outputs"); dir.create(outdir, recursive = TRUE, showWarnings = FALSE)
ts <- format(Sys.time(), "%Y%m%d_%H%M%S")

writexl::write_xlsx(demo_disparities,
                    file.path(outdir, paste0("15_EDA_demographic_disparities_", ts, ".xlsx")))
writexl::write_xlsx(within_category_disparities,
                    file.path(outdir, paste0("15_EDA_within_demographic_spreads_", ts, ".xlsx")))
writexl::write_xlsx(demo_disparity_rank,
                    file.path(outdir, paste0("15_EDA_demographic_disparity_rankings_", ts, ".xlsx")))
writexl::write_xlsx(demo_summary_by_setting,
                    file.path(outdir, paste0("15_EDA_demographic_summary_by_setting_", ts, ".xlsx")))
writexl::write_xlsx(
  list(
    disparities = demo_disparities,
    spreads     = within_category_disparities,
    rankings    = demo_disparity_rank,
    summary     = demo_summary_by_setting
  ),
  path = file.path(outdir, paste0("15_EDA_outputs_", ts, ".xlsx"))
)

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