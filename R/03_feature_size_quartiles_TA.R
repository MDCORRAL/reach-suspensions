# R/03_feature_size_quartiles_TA.R
# Build TA (total enrollment) quartiles per year and attach to all rows.
library(dplyr)
library(arrow)

# Turn console summary on/off
SHOW_SUMMARY <- TRUE

# Read input
v1 <- arrow::read_parquet("data-stage/susp_v1_noall.parquet")

# --- TA per school x year (force uniqueness) -------------------------------
ta <- v1 %>%
  filter(reporting_category == "TA") %>%
  transmute(
    academic_year, county_code, district_code, school_code,
    ta_enroll = cumulative_enrollment
  )

# Collapse duplicates if they exist (keep first non-NA cumulative_enrollment)
ta_unique <- ta %>%
  group_by(academic_year, county_code, district_code, school_code) %>%
  summarise(
    n_rows = n(),
    ta_enroll = first(na.omit(ta_enroll)),
    .groups = "drop"
  )

dups <- ta_unique %>% filter(n_rows > 1)
if (nrow(dups) > 0) {
  warning("Collapsed ", nrow(dups), " duplicate TA rows to one per school-year.")
}
ta_unique <- ta_unique %>% select(-n_rows)

# --- Quartiles within year, based on TA enrollment -------------------------
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
  ungroup()

# --- Join back to all race rows --------------------------------------------
v2 <- v1 %>%
  left_join(ta_q, by = c("academic_year","county_code","district_code","school_code"))

# quick validation: ensure TA uniqueness
stopifnot(
  ta_unique %>%
    count(academic_year, county_code, district_code, school_code) %>%
    pull(n) %>% max() == 1
)

# write out v2
arrow::write_parquet(v2, "data-stage/susp_v2.parquet")

# ---- Explicit prints and sanity checks (only after v2 exists) -------------
if (isTRUE(SHOW_SUMMARY)) {
  out <- v2 %>%
    distinct(academic_year, county_code, district_code, school_code, enroll_q_label) %>%
    count(academic_year, enroll_q_label) %>%
    arrange(academic_year, enroll_q_label)
  print(out, n = 200)
  
  # Sanity checks
  message("\nSanity checks:")
  # 1) No duplicate school-year keys in TA
  ta_dups <- ta_unique %>% count(academic_year, county_code, district_code, school_code) %>% filter(n > 1)
  message("TA duplicate school-year rows (should be 0): ", nrow(ta_dups))
  
  # 2) Quartiles are year-specific (quick view)
  print(v2 %>% count(academic_year, enroll_q_label) %>% arrange(academic_year, enroll_q_label), n = 200)
  
  # 3) Unknowns only where TA enroll is NA/0
  unk <- v2 %>%
    filter(enroll_q_label == "Unknown") %>%
    distinct(ta_enroll) %>%
    arrange(ta_enroll)
  message("Distinct ta_enroll values for 'Unknown' quartile (should be NA or 0):")
  print(unk, n = 50)
}

invisible(TRUE)


