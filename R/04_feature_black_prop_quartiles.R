# R/04_feature_black_prop_quartiles.R
# Adds proportion-Black (per school-year) and year-specific quartiles.

library(dplyr)
library(arrow)
library(tidyr)

# Load latest stage (has locale_simple + TA size quartiles)
v2 <- arrow::read_parquet("data-stage/susp_v2.parquet")

# --- 1) Pull RB (Black) and TA totals at SCHOOL x YEAR ---
rb_ta <- v2 %>%
  filter(reporting_category %in% c("RB", "TA")) %>%
  select(academic_year, county_code, district_code, school_code,
         reporting_category, cumulative_enrollment) %>%
  mutate(
    # keep just one row per code; if multiples ever appear, collapse
    cumulative_enrollment = as.numeric(cumulative_enrollment)
  ) %>%
  group_by(academic_year, county_code, district_code, school_code, reporting_category) %>%
  summarise(enroll = first(na.omit(cumulative_enrollment)), .groups = "drop") %>%
  pivot_wider(
    names_from = reporting_category,
    values_from = enroll,
    names_prefix = "enroll_"
  )

# Ensure columns exist even if a year has no RB in the raw
rb_ta <- rb_ta %>%
  mutate(
    enroll_RB = replace_na(enroll_RB, 0),   # if RB suppressed/absent, treat as 0 enrolled for prop calc
    enroll_TA = enroll_TA                   # TA kept as is; we’ll guard against 0/NA below
  )

# --- 2) Proportion Black per school-year ---
rb_ta <- rb_ta %>%
  mutate(
    prop_black = dplyr::if_else(!is.na(enroll_TA) & enroll_TA > 0,
                                enroll_RB / enroll_TA,
                                NA_real_)
  )

# --- 3) Year-specific quartiles on prop_black ---
rb_q <- rb_ta %>%
  group_by(academic_year) %>%
  mutate(
    black_prop_q4 = dplyr::if_else(!is.na(prop_black),
                                   ntile(prop_black, 4L),
                                   NA_integer_),
    black_prop_q_label = dplyr::case_when(
      is.na(black_prop_q4)         ~ "Unknown",
      black_prop_q4 == 1L          ~ "Q1 (Lowest % Black)",
      black_prop_q4 == 2L          ~ "Q2",
      black_prop_q4 == 3L          ~ "Q3",
      black_prop_q4 == 4L          ~ "Q4 (Highest % Black)"
    )
  ) %>%
  ungroup()

# --- 4) Join back to all race rows and save ---
v3 <- v2 %>%
  left_join(rb_q,
            by = c("academic_year","county_code","district_code","school_code"))

arrow::write_parquet(v3, "data-stage/susp_v3.parquet")

# --- Quick sanity checks you’ll see in Console ---
# (A) No duplicate school-year keys in RB/TA wide:
stopifnot(
  rb_ta %>%
    count(academic_year, county_code, district_code, school_code) %>%
    pull(n) %>% max() == 1
)

# (B) Quartile counts per year (one per school-year)
v3 %>%
  distinct(academic_year, county_code, district_code, school_code, black_prop_q_label) %>%
  count(academic_year, black_prop_q_label) %>%
  arrange(academic_year, black_prop_q_label) %>%
  print(n = 30)

# (C) Unknowns = cases where TA is NA/0 (i.e., no denominator)
v3 %>%
  filter(black_prop_q_label == "Unknown") %>%
  distinct(enroll_TA) %>%
  arrange(enroll_TA) %>%
  print(n = 30)

invisible(TRUE)

# --- Some explicit sanity checks you can see in Console ---
# 1) Quartiles cover all non-missing proportions
v3 %>%
  distinct(academic_year, county_code, district_code, school_code,
           prop_black, black_prop_q_label) %>%
  summarise(
    n_nonmissing = sum(!is.na(prop_black)),
    n_quartiled  = sum(black_prop_q_label %in% c("Q1 (Lowest % Black)","Q2","Q3","Q4 (Highest % Black)")),
    n_unknown    = sum(black_prop_q_label == "Unknown"),
    .by = academic_year
  ) %>% print(n = 30)

# 2) Proportions are well-formed (0–1), and RB never exceeds TA
v3 %>%
  distinct(academic_year, county_code, district_code, school_code, enroll_RB, enroll_TA, prop_black) %>%
  summarise(
    any_prop_oob = any(prop_black < 0 | prop_black > 1, na.rm = TRUE),
    any_RB_gt_TA = any(enroll_RB > enroll_TA, na.rm = TRUE),
    .by = academic_year
  ) %>% print(n = 30)
# Both should be FALSE
