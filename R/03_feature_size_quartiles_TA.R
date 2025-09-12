# R/03_feature_size_quartiles_TA.R
# Build total enrollment (All Students) quartiles per year and attach to all rows.

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
  build_keys() %>%
  mutate(subgroup = canon_race_label(reporting_category))

# --- 2) Total enrollment per campus x year (campus-only; force uniqueness) ---
all_unique <- v1 %>%
  filter_campus_only() %>%
  filter(subgroup == "All Students") %>%
  transmute(
    academic_year,
    cds_school,
    all_enroll = cumulative_enrollment
  ) %>%
  group_by(academic_year, cds_school) %>%
  summarise(
    all_enroll = first_non_na(all_enroll),
    .groups = "drop"
  )

# --- 3) Quartiles within year (based on positive total enrollment) -----------
all_q <- all_unique %>%
  group_by(academic_year) %>%
  mutate(
    enroll_q4 = if_else(!is.na(all_enroll) & all_enroll > 0, ntile(all_enroll, 4L), NA_integer_),
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
  left_join(all_q, by = c("academic_year","cds_school"))

# quick validation: ensure one total-enrollment row per campus-year
stopifnot(
  all_unique %>%
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
  all_dups <- all_unique %>%
    count(academic_year, cds_school) %>%
    filter(n > 1)
  message("Total enrollment duplicate campus-year rows (should be 0): ", nrow(all_dups))
  
  print(
    v2 %>%
      filter_campus_only() %>%
      count(academic_year, enroll_q_label) %>%
      arrange(academic_year, enroll_q_label),
    n = 200
  )
  
  unk <- v2 %>%
    filter(enroll_q_label == "Unknown") %>%
    distinct(all_enroll) %>%
    arrange(all_enroll)
  message("Distinct all_enroll values for 'Unknown' quartile (should be NA or 0):")
  print(unk, n = 50)
}

invisible(TRUE)
