library(dplyr)
library(arrow)

v1 <- arrow::read_parquet("data-stage/susp_v1_noall.parquet")

# TA per school x year (force uniqueness)
ta_unique <- v1 %>%
  filter(reporting_category == "TA") %>%
  group_by(academic_year, county_code, district_code, school_code) %>%
  summarise(
    n_rows = n(),
    ta_enroll = first(na.omit(cumulative_enrollment)),
    .groups = "drop"
  )

# Warn if we collapsed duplicates
dups <- ta_unique %>% filter(n_rows > 1)
if (nrow(dups) > 0) {
  warning("Collapsed ", nrow(dups), " duplicate TA rows to one per school-year.")
}

# Quartiles within year, based on TA enrollment
ta_q <- ta_unique %>%
  group_by(academic_year) %>%
  mutate(
    enroll_q4 = if_else(!is.na(ta_enroll) & ta_enroll > 0, ntile(ta_enroll, 4L), NA_integer_),
    enroll_q_label = case_when(
      is.na(enroll_q4) ~ "Unknown",
      enroll_q4 == 1 ~ "Q1 (Smallest)",
      enroll_q4 == 2 ~ "Q2",
      enroll_q4 == 3 ~ "Q3",
      enroll_q4 == 4 ~ "Q4 (Largest)"
    )
  ) %>%
  ungroup() %>%
  select(-n_rows)

# Join back to all race rows
v2 <- v1 %>%
  left_join(ta_q, by = c("academic_year","county_code","district_code","school_code"))

arrow::write_parquet(v2, "data-stage/susp_v2.parquet")

# ---- Explicit print so you see a table in the Console ----
out <- v2 %>%
  distinct(academic_year, county_code, district_code, school_code, enroll_q_label) %>%
  count(academic_year, enroll_q_label) %>%
  arrange(academic_year, enroll_q_label)

print(out, n = 30)