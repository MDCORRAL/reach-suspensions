# R/03_feature_size_quartiles.R (fixed)
library(dplyr)
library(arrow)

v1 <- arrow::read_parquet("data-stage/susp_v1.parquet")

# 1) Collapse to SCHOOL × YEAR totals (sum across races)
school_year_enroll <- v1 %>%
  group_by(academic_year, county_code, district_code, school_code) %>%
  summarise(
    enroll_total = sum(coalesce(cumulative_enrollment, 0)),
    any_enroll_reported = any(!is.na(cumulative_enrollment)),
    .groups = "drop"
  )

# 2) Assign quartiles *within year* based on enroll_total (only if >0 and reported)
school_year_enroll <- school_year_enroll %>%
  group_by(academic_year) %>%
  mutate(
    enroll_q4 = if_else(any_enroll_reported & enroll_total > 0,
                        ntile(enroll_total, 4),
                        NA_integer_),
    enroll_q_label = case_when(
      is.na(enroll_q4) ~ "Unknown",
      enroll_q4 == 1   ~ "Q1 (Smallest)",
      enroll_q4 == 2   ~ "Q2",
      enroll_q4 == 3   ~ "Q3",
      enroll_q4 == 4   ~ "Q4 (Largest)"
    )
  ) %>%
  ungroup()

# 3) Attach these school-year quartiles back to every race row
v2 <- v1 %>%
  left_join(school_year_enroll,
            by = c("academic_year","county_code","district_code","school_code"))

arrow::write_parquet(v2, "data-stage/susp_v2.parquet")

# Count *schools* per quartile per year
school_year_enroll %>% count(academic_year, enroll_q_label) %>% arrange(academic_year, enroll_q_label)

# If you want to count from the race-level table, deduplicate first:
arrow::read_parquet("data-stage/susp_v2.parquet") %>%
  distinct(academic_year, county_code, district_code, school_code, enroll_q_label) %>%
  count(academic_year, enroll_q_label)
