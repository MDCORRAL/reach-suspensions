# R/04_feature_black_prop_quartiles.R
# Adds proportion-Black (per school-year) and year-specific quartiles.
# v1_noall drops charter=='All'; prop_black is NA when RB is missing or TA≤0.”
# Quiet core libs
suppressPackageStartupMessages({
  library(here)     # project-root paths
  library(arrow)    # parquet I/O
  library(dplyr)    # data wrangling
  library(stringr)  # string helpers (if needed elsewhere)
  library(tidyr)    # pivot_wider, replace_na
})

message(">>> Running from project root: ", here::here())

# -------- helper: safe first non-NA numeric --------
first_non_na_num <- function(x) {
  y <- suppressWarnings(as.numeric(x))
  y <- y[!is.na(y)]
  if (length(y)) y[1] else NA_real_
}

# Load latest stage (has locale_simple + TA size quartiles)
v2 <- arrow::read_parquet(here::here("data-stage", "susp_v2.parquet"))

# --- 1) Pull RB (Black) and TA totals at SCHOOL x YEAR ---
rb_ta <- v2 %>%
  filter(reporting_category %in% c("RB", "TA")) %>%
  select(
    academic_year, county_code, district_code, school_code,
    reporting_category, cumulative_enrollment
  ) %>%
  group_by(academic_year, county_code, district_code, school_code, reporting_category) %>%
  summarise(
    enroll = first_non_na_num(cumulative_enrollment),
    .groups = "drop"
  ) %>%
  pivot_wider(
    names_from  = reporting_category,
    values_from = enroll,
    names_prefix = "enroll_"
  ) %>%
  mutate(
    enroll_RB = enroll_RB,  # leave missing as NA, no replacement
    enroll_TA = enroll_TA                  # keep TA as-is; guard below
  )

# --- 2) Proportion Black per school-year ---
rb_ta <- rb_ta %>%
  mutate(
    prop_black = if_else(!is.na(enroll_TA) & enroll_TA > 0 & !is.na(enroll_RB),
                         enroll_RB / enroll_TA,
                         NA_real_)
  )

# --- 3) Year-specific quartiles on prop_black ---
rb_q <- rb_ta %>%
  group_by(academic_year) %>%
  mutate(
    black_prop_q4 = if_else(!is.na(prop_black), ntile(prop_black, 4L), NA_integer_),
    black_prop_q_label = case_when(
      is.na(black_prop_q4) ~ "Unknown",
      black_prop_q4 == 1L  ~ "Q1 (Lowest % Black)",
      black_prop_q4 == 2L  ~ "Q2",
      black_prop_q4 == 3L  ~ "Q3",
      black_prop_q4 == 4L  ~ "Q4 (Highest % Black)"
    )
  ) %>%
  ungroup()

# --- 4) Join back to all race rows and save ---
v3 <- v2 %>%
  left_join(rb_q, by = c("academic_year","county_code","district_code","school_code"))

arrow::write_parquet(v3, here::here("data-stage", "susp_v3.parquet"))
message(">>> 04_feature_black_prop_quartiles: wrote susp_v3.parquet")

# --- Quick sanity checks you’ll see in Console ---
# (A) No duplicate school-year keys in RB/TA wide:
stopifnot(
  rb_ta %>%
    count(academic_year, county_code, district_code, school_code) %>%
    pull(n) %>% max(na.rm = TRUE) == 1
)

# (B) Quartile counts per year (one per school-year)
v3 %>%
  distinct(academic_year, county_code, district_code, school_code, black_prop_q_label) %>%
  count(academic_year, black_prop_q_label) %>%
  arrange(academic_year, black_prop_q_label) %>%
  print(n = 30)

# (C) Unknowns = cases where TA is NA/0 (i.e., no denominator)
v3 %>%
  mutate(
    unknown_reason = case_when(
      is.na(prop_black) & (is.na(enroll_TA) | enroll_TA <= 0) ~ "TA missing/zero",
      is.na(prop_black) &  is.na(enroll_RB)                   ~ "RB missing",
      TRUE                                                    ~ "Not unknown"
    )
  ) %>%
  count(academic_year, unknown_reason) %>%
  arrange(academic_year, unknown_reason) %>%
  print(n = 50)

# Extra checks
v3 %>%
  distinct(academic_year, county_code, district_code, school_code,
           enroll_RB, enroll_TA, prop_black) %>%
  summarise(
    any_prop_oob = any(prop_black < 0 | prop_black > 1, na.rm = TRUE),
    any_RB_gt_TA = any(enroll_RB > enroll_TA, na.rm = TRUE),
    .by = academic_year
  ) %>%
  print(n = 30)  # Both should be FALSE

invisible(TRUE)
