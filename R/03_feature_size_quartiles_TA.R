# R/03_feature_size_quartiles_TA.R
# Build TA (total enrollment) quartiles per year and attach to all rows.

suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
})

SHOW_SUMMARY <- TRUE
source(here::here("R","utils_keys_filters.R"))
message(">>> Running from project root: ", here::here())

# --- helper ------------------------------------------------------------------
first_non_na <- function(x) {
  y <- x[!is.na(x)]
  if (length(y)) y[1] else NA_real_
}

# --- 1) Read + normalize keys ------------------------------------------------
v1 <- arrow::read_parquet(here::here("data-stage", "susp_v1_noall.parquet")) %>%
  build_keys()

# --- 2) TA per campus x year (campus-only; force uniqueness) -----------------
ta_unique <- v1 %>%
  filter_campus_only() %>%
  filter(reporting_category == "TA") %>%
  transmute(
    academic_year,
    cds_school,
    ta_enroll = cumulative_enrollment
  ) %>%
  group_by(academic_year, cds_school) %>%
  summarise(
    ta_enroll = first_non_na(ta_enroll),
    .groups = "drop"
  )

# --- 3) Quartiles within year (based on positive TA) -------------------------
ta_q <- ta_unique %>%
  group_by(academic_year) %>%
  mutate(
    enroll_q4 = if_else(!is.na(ta_enroll) & ta_enroll > 0, ntile(ta_enroll, 4L), NA_integer_),
    enroll_q_label = case_when(
      is.na(enroll_q4) ~ "Unknown",
      enroll_q4 == 1L ~ "Q1 (Smallest)",
      enroll_q4 == 2L ~ "Q2",
      enroll_q4 == 3L ~ "Q3",
      enroll_q4 == 4L ~ "Q4 (Largest)"
    )
  ) %>% 
  ungroup()

# --- 4) Join back to all rows (by campus-year) -------------------------------
v2 <- v1 %>%
  left_join(ta_q, by = c("academic_year","cds_school"))

# quick validation: ensure one TA per campus-year
stopifnot(
  ta_unique %>%
    count(academic_year, cds_school) %>%
    pull(n) %>% max() == 1
)

# --- 5) Write out ------------------------------------------------------------
arrow::write_parquet(v2, here::here("data-stage", "susp_v2.parquet"))
message(">>> 03_feature_size_quartiles_TA: wrote susp_v2.parquet")

# --- 6) Optional summary -----------------------------------------------------
if (isTRUE(SHOW_SUMMARY)) {
  out <- v2 %>%
    filter_campus_only() %>%
    distinct(academic_year, cds_school, enroll_q_label) %>%
    count(academic_year, enroll_q_label) %>%
    arrange(academic_year, enroll_q_label)
  print(out, n = 200)
  
  message("\nSanity checks:")
  ta_dups <- ta_unique %>%
    count(academic_year, cds_school) %>%
    filter(n > 1)
  message("TA duplicate campus-year rows (should be 0): ", nrow(ta_dups))
  
  print(
    v2 %>%
      filter_campus_only() %>%
      count(academic_year, enroll_q_label) %>%
      arrange(academic_year, enroll_q_label),
    n = 200
  )
  
  unk <- v2 %>%
    filter(enroll_q_label == "Unknown") %>%
    distinct(ta_enroll) %>%
    arrange(ta_enroll)
  message("Distinct ta_enroll values for 'Unknown' quartile (should be NA or 0):")
  print(unk, n = 50)
}

invisible(TRUE)
