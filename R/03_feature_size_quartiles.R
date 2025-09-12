# R/03_feature_size_quartiles.R

# Quiet core libs
suppressPackageStartupMessages({
  library(here)     # project-root paths
  library(arrow)    # parquet I/O
  library(dplyr)    # data wrangling
})

message(">>> Running from project root: ", here::here())

# 1) Read race-level table
v1 <- arrow::read_parquet(here::here("data-stage", "susp_v1.parquet"))

# 2) Collapse to SCHOOL × YEAR totals (sum across races)
school_year_enroll <- v1 %>%
  group_by(academic_year, county_code, district_code, school_code) %>%
  summarise(
    enroll_total = sum(coalesce(cumulative_enrollment, 0)),
    any_enroll_reported = any(!is.na(cumulative_enrollment)),
    .groups = "drop"
  )

# 3) Assign quartiles within year
school_year_enroll <- school_year_enroll %>%
  group_by(academic_year) %>%
  mutate(
    enroll_q = if_else(any_enroll_reported & enroll_total > 0,
                       ntile(enroll_total, 4L),
                       NA_integer_),
    enroll_q_label = case_when(
      is.na(enroll_q) ~ "Unknown",
      enroll_q == 1   ~ "Q1 (Smallest)",
      enroll_q == 2   ~ "Q2",
      enroll_q == 3   ~ "Q3",
      enroll_q == 4   ~ "Q4 (Largest)"
    )
  ) %>%
  ungroup()

# 4) Attach quartiles back to every race row
v2 <- v1 %>%
  left_join(
    school_year_enroll,
    by = c("academic_year", "county_code", "district_code", "school_code")
  )

# 5) Write out
arrow::write_parquet(v2, here::here("data-stage", "susp_v2.parquet"))
message(">>> 03_feature_size_quartiles: wrote susp_v2.parquet")

# 6) Quick tallies
print(
  school_year_enroll %>%
    count(academic_year, enroll_q_label) %>%
    arrange(academic_year, enroll_q_label),
  n = 200
)

# If you prefer to verify from the saved table:
arrow::read_parquet(here::here("data-stage", "susp_v2.parquet")) %>%
  distinct(academic_year, county_code, district_code, school_code, enroll_q_label) %>%
  count(academic_year, enroll_q_label) %>%
  arrange(academic_year, enroll_q_label) %>%
  print(n = 200)

invisible(TRUE)
