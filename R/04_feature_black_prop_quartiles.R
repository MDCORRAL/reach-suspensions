# R/04_feature_black_prop_quartiles.R
# Adds proportion-Black and proportion-White (per school-year) and year-specific quartiles.

suppressPackageStartupMessages({
  library(here)     # project-root paths
  library(arrow)    # parquet I/O
  library(dplyr)    # data wrangling
  library(stringr)  # string helpers (if needed)
  library(tidyr)    # pivot_wider
})

message(">>> Running from project root: ", here::here())

# -------- helper: safe first non-NA numeric --------
first_non_na_num <- function(x) {
  y <- suppressWarnings(as.numeric(x))
  y <- y[!is.na(y)]
  if (length(y)) y[1] else NA_real_
}

# ---- read input ------------------------------------------------------------
v2 <- arrow::read_parquet(here::here("data-stage", "susp_v2.parquet"))

# Input guard
stopifnot(all(c("reporting_category","cumulative_enrollment") %in% names(v2)))

# Optional heads-up if White code isn't present as "RW"
if (!any(v2$reporting_category == "RW", na.rm = TRUE)) {
  warning("No rows with reporting_category == 'RW' found. If your White code differs, update the filter below.")
}

# --- 1) Pull RB (Black), RW (White), and TA totals at SCHOOL x YEAR --------
# NOTE: RB = Black; RW = White; TA = Total Enrollment
rb_rw_ta <- v2 %>%
  filter(reporting_category %in% c("RB", "RW", "TA")) %>%
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
    names_from  = reporting_category,  # RB, RW, TA
    values_from = enroll,
    names_prefix = "enroll_"           # enroll_RB, enroll_RW, enroll_TA
  )

# --- 2) Proportion Black & White per school-year ---------------------------
# Option A: leave RB/RW missing as NA; TA must be > 0 to compute proportions
rb_rw_ta <- rb_rw_ta %>%
  mutate(
    prop_black = if_else(!is.na(enroll_TA) & enroll_TA > 0 & !is.na(enroll_RB),
                         enroll_RB / enroll_TA, NA_real_),
    prop_white = if_else(!is.na(enroll_TA) & enroll_TA > 0 & !is.na(enroll_RW),
                         enroll_RW / enroll_TA, NA_real_)
  )

# --- 3) Year-specific quartiles on prop_black & prop_white -----------------
rbw_q <- rb_rw_ta %>%
  group_by(academic_year) %>%
  mutate(
    black_prop_q4 = if_else(!is.na(prop_black), ntile(prop_black, 4L), NA_integer_),
    white_prop_q4 = if_else(!is.na(prop_white), ntile(prop_white, 4L), NA_integer_),
    
    black_prop_q_label = case_when(
      is.na(black_prop_q4) ~ "Unknown",
      black_prop_q4 == 1L  ~ "Q1 (Lowest % Black)",
      black_prop_q4 == 2L  ~ "Q2",
      black_prop_q4 == 3L  ~ "Q3",
      black_prop_q4 == 4L  ~ "Q4 (Highest % Black)"
    ),
    white_prop_q_label = case_when(
      is.na(white_prop_q4) ~ "Unknown",
      white_prop_q4 == 1L  ~ "Q1 (Lowest % White)",
      white_prop_q4 == 2L  ~ "Q2",
      white_prop_q4 == 3L  ~ "Q3",
      white_prop_q4 == 4L  ~ "Q4 (Highest % White)"
    )
  ) %>%
  ungroup()

# --- 4) Join back to all race rows and save --------------------------------
v3 <- v2 %>%
  left_join(rbw_q,
            by = c("academic_year","county_code","district_code","school_code"))

# Row-count must be stable through the join
stopifnot(nrow(v3) == nrow(v2))

arrow::write_parquet(v3, here::here("data-stage", "susp_v3.parquet"))
message(">>> 04_feature_black_prop_quartiles: wrote susp_v3.parquet (now includes White)")

# --- Quick sanity checks in Console ----------------------------------------
# (A) No duplicate school-year keys in wide table
stopifnot(
  rb_rw_ta %>%
    count(academic_year, county_code, district_code, school_code) %>%
    pull(n) %>% max(na.rm = TRUE) == 1
)

# (B) Quartile counts per year (Black & White)
v3 %>%
  distinct(academic_year, county_code, district_code, school_code,
           black_prop_q_label, white_prop_q_label) %>%
  count(academic_year, black_prop_q_label, white_prop_q_label) %>%
  arrange(academic_year, black_prop_q_label, white_prop_q_label) %>%
  print(n = 60)

# (C) Why Unknown? (TA missing/zero or RB/RW missing)
v3 %>%
  mutate(
    unknown_black_reason = case_when(
      is.na(prop_black) & (is.na(enroll_TA) | enroll_TA <= 0) ~ "TA missing/zero",
      is.na(prop_black) &  is.na(enroll_RB)                   ~ "RB missing",
      TRUE                                                    ~ "Not unknown"
    ),
    unknown_white_reason = case_when(
      is.na(prop_white) & (is.na(enroll_TA) | enroll_TA <= 0) ~ "TA missing/zero",
      is.na(prop_white) &  is.na(enroll_RW)                   ~ "RW missing",
      TRUE                                                    ~ "Not unknown"
    )
  ) %>%
  count(academic_year, unknown_black_reason, unknown_white_reason) %>%
  arrange(academic_year, unknown_black_reason, unknown_white_reason) %>%
  print(n = 60)

# (D) Bounds: proportions in [0,1], RB/RW <= TA
v3 %>%
  distinct(academic_year, county_code, district_code, school_code,
           enroll_RB, enroll_RW, enroll_TA, prop_black, prop_white) %>%
  summarise(
    any_black_oob = any(prop_black < 0 | prop_black > 1, na.rm = TRUE),
    any_white_oob = any(prop_white < 0 | prop_white > 1, na.rm = TRUE),
    any_RB_gt_TA  = any(enroll_RB > enroll_TA, na.rm = TRUE),
    any_RW_gt_TA  = any(enroll_RW > enroll_TA, na.rm = TRUE),
    .by = academic_year
  ) %>%
  print(n = 60)  # All should be FALSE

invisible(TRUE)
