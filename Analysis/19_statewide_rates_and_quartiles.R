# analysis/19_statewide_rates_and_quartiles.R
#
##codex/create-statewide-data-frame-analysis-ke1b6
# Build statewide suspension/enrollment totals prior to any filtering.
# Also derive quartile summaries (e.g., by enrollment, racial proportion)
# independent from those statewide totals.


suppressPackageStartupMessages({
  library(here)
  library(dplyr)
  library(arrow)
})

try(here::i_am("Analysis/19_statewide_rates_and_quartiles.R"), silent = TRUE)

source(here::here("R", "utils_keys_filters.R"))

# ---- Load raw long file ------------------------------------------------------
v6 <- read_parquet(here("data-stage", "susp_v6_long.parquet")) %>%
  build_keys() %>%
##odex/create-statewide-data-frame-analysis-ke1b6u
  filter_campus_only() %>%
  mutate(
    school_group = if_else(
      school_level %in% c("Elementary", "Middle", "High"),
      "Traditional",
      "Non-traditional"
    )
  )

need_cols <- c(
  "category_type", "subgroup", "academic_year", "school_group",
  "total_suspensions", "cumulative_enrollment"
)
stopifnot(all(need_cols %in% names(v6)))

# ---- Statewide totals --------------------------------------------------------
statewide <- v6 %>%
  group_by(subgroup, academic_year) %>%

  summarise(
    total_suspensions = sum(total_suspensions, na.rm = TRUE),
    total_enrollment  = sum(cumulative_enrollment, na.rm = TRUE),
    statewide_rate    = if_else(total_enrollment > 0, total_suspensions / total_enrollment, NA_real_),
    .groups = "drop"
  )

write_parquet(statewide, here("data-stage", "statewide_totals.parquet"))

# ---- Quartile helpers --------------------------------------------------------
##codex/create-statewide-data-frame-analysis-ke1b6u
# Quartiles by total school enrollment (All Students baseline)
school_enroll <- v6 %>%
  filter(category_type == "Race/Ethnicity", subgroup == "All Students") %>%
  select(cds_school, academic_year, total_enrollment_all = cumulative_enrollment) %>%
  group_by(academic_year) %>%
  mutate(enrollment_q = ntile(total_enrollment_all, 4)) %>%
  ungroup()

v6_enroll_q <- v6 %>%
  left_join(school_enroll %>% select(cds_school, academic_year, enrollment_q),
    by = c("cds_school", "academic_year"))

v6_enroll_q_all <- bind_rows(v6_enroll_q, v6_enroll_q %>% mutate(school_group = "All"))

by_enrollment <- v6_enroll_q_all %>%
  filter(!is.na(enrollment_q)) %>%
  group_by(category_type, subgroup, academic_year, enrollment_q, school_group) %>%

  summarise(
    suspensions = sum(total_suspensions, na.rm = TRUE),
    enrollment  = sum(cumulative_enrollment, na.rm = TRUE),
    rate        = if_else(enrollment > 0, suspensions / enrollment, NA_real_),
    .groups     = "drop"
  )

write_parquet(by_enrollment, here("data-stage", "quartile_rates_by_enrollment.parquet"))

##codex/create-statewide-data-frame-analysis-ke1b6u

# Quartiles by racial proportion example: Black share of enrollment
# Compute proportion of Black enrollment out of school total and derive quartiles
all_enroll <- v6 %>%
  filter(subgroup == "All Students") %>%
  select(cds_school, academic_year, total_enrollment_all = cumulative_enrollment)

black_prop <- v6 %>%
  filter(subgroup == "Black/African American") %>%
  left_join(all_enroll, by = c("cds_school", "academic_year")) %>%
  mutate(black_prop = if_else(total_enrollment_all > 0, cumulative_enrollment / total_enrollment_all, NA_real_))

by_black_prop <- black_prop %>%
  group_by(academic_year) %>%
  mutate(black_prop_q = ntile(black_prop, 4)) %>%
  group_by(academic_year, black_prop_q) %>%

  summarise(
    suspensions = sum(total_suspensions, na.rm = TRUE),
    enrollment  = sum(cumulative_enrollment, na.rm = TRUE),
    rate        = if_else(enrollment > 0, suspensions / enrollment, NA_real_),
    .groups     = "drop"
  )

write_parquet(by_black_prop, here("data-stage", "quartile_rates_by_black_prop.parquet"))

# End of file
