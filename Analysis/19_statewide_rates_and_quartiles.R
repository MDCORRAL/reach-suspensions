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
    school_group = school_level,
    school_type = case_when(
      school_level %in% c("Adult", "Alternative", "Community Day", "Juvenile Court", "Other") ~ "Non-traditional",
      TRUE ~ "Traditional"
    )
  )

need_cols <- c(
  "category_type", "subgroup", "academic_year", "school_group",
  "school_level", "school_type", "total_suspensions", "cumulative_enrollment"
)
stopifnot(all(need_cols %in% names(v6)))

v6_all <- bind_rows(
  v6,
  v6 %>% mutate(school_group = "All"),
  v6 %>% mutate(school_type = "All"),
  v6 %>% mutate(school_group = "All", school_type = "All")
)

# ---- Statewide totals --------------------------------------------------------

statewide_all <- v6_all %>%
  group_by(academic_year, subgroup, school_group, school_type) %>%
  summarise(
    total_suspensions = sum(total_suspensions, na.rm = TRUE),
    total_enrollment  = sum(cumulative_enrollment, na.rm = TRUE),
    statewide_rate    = if_else(total_enrollment > 0, total_suspensions / total_enrollment, NA_real_),
    .groups = "drop"
  )

# overall statewide totals only (both group and type = "All")
statewide <- statewide_all %>%
  filter(school_group == "All", school_type == "All")

write_parquet(statewide, here("data-stage", "statewide_totals.parquet"))

# breakdowns by school_group and school_type
statewide_breakdowns <- statewide_all %>%
  filter(!(school_group == "All" & school_type == "All"))

write_parquet(
  statewide_breakdowns,
  here("data-stage", "statewide_totals_breakdowns.parquet")
)

# ---- Quartile helpers --------------------------------------------------------
##codex/create-statewide-data-frame-analysis-ke1b6u
# Quartiles by total school enrollment (All Students baseline)
school_enroll <- v6 %>%
  filter(category_type == "Race/Ethnicity", subgroup == "All Students") %>%
  group_by(cds_school, academic_year) %>%
  summarise(
    total_enrollment_all = sum(cumulative_enrollment, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  group_by(academic_year) %>%
  mutate(enrollment_q = ntile(total_enrollment_all, 4)) %>%
  ungroup()

v6_enroll_q <- v6 %>%
  left_join(
    school_enroll %>% select(cds_school, academic_year, enrollment_q),
    by = c("cds_school", "academic_year"),
    relationship = "one-to-one"
  )

v6_enroll_q_all <- bind_rows(
  v6_enroll_q,
  v6_enroll_q %>% mutate(school_group = "All"),
  v6_enroll_q %>% mutate(school_type = "All"),
  v6_enroll_q %>% mutate(school_group = "All", school_type = "All")
)

by_enrollment <- v6_enroll_q_all %>%
  filter(!is.na(enrollment_q)) %>%
  group_by(category_type, subgroup, academic_year, enrollment_q, school_group, school_type) %>%

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
  group_by(cds_school, academic_year) %>%
  summarise(
    total_enrollment_all = sum(cumulative_enrollment, na.rm = TRUE),
    .groups = "drop"
  )

black_prop <- v6 %>%
  filter(subgroup == "Black/African American") %>%
  left_join(
    all_enroll,
    by = c("cds_school", "academic_year"),
    relationship = "one-to-one"
  ) %>%
  mutate(
    black_prop = if_else(
      total_enrollment_all > 0,
      cumulative_enrollment / total_enrollment_all,
      NA_real_
    )
  )

by_black_prop <- black_prop %>%
  group_by(academic_year) %>%
  mutate(black_prop_q = ntile(black_prop, 4)) %>%
  filter(!is.na(black_prop_q)) %>%
  group_by(academic_year, black_prop_q) %>%

  summarise(
    suspensions = sum(total_suspensions, na.rm = TRUE),
    enrollment  = sum(cumulative_enrollment, na.rm = TRUE),
    rate        = if_else(enrollment > 0, suspensions / enrollment, NA_real_),
    .groups     = "drop"
  )

write_parquet(by_black_prop, here("data-stage", "quartile_rates_by_black_prop.parquet"))

# End of file
