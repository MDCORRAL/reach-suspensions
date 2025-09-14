# analysis/19_statewide_rates_and_quartiles.R
#
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
  # Keep only campus-level records
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
# ---- Statewide totals --------------------------------------------------------

statewide_by_group_type <- v6 %>%
  group_by(academic_year, subgroup, school_group, school_type) %>%
  summarise(
    total_suspensions = sum(total_suspensions, na.rm = TRUE),
    total_enrollment  = sum(cumulative_enrollment, na.rm = TRUE),
    .groups = "drop"
  )

statewide_by_type <- v6 %>%
  group_by(academic_year, subgroup, school_type) %>%
  summarise(
    total_suspensions = sum(total_suspensions, na.rm = TRUE),
    total_enrollment  = sum(cumulative_enrollment, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(school_group = "All")

statewide_by_group <- v6 %>%
  group_by(academic_year, subgroup, school_group) %>%
  summarise(
    total_suspensions = sum(total_suspensions, na.rm = TRUE),
    total_enrollment  = sum(cumulative_enrollment, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(school_type = "All")

statewide_overall <- v6 %>%
  group_by(academic_year, subgroup) %>%
  summarise(
    total_suspensions = sum(total_suspensions, na.rm = TRUE),
    total_enrollment  = sum(cumulative_enrollment, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(school_group = "All", school_type = "All")

statewide_all <- bind_rows(
  statewide_by_group_type,
  statewide_by_type,
  statewide_by_group,
  statewide_overall
) %>%
  mutate(
    statewide_rate = if_else(total_enrollment > 0, total_suspensions / total_enrollment, NA_real_)
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
# Quartiles by total school enrollment (All Students baseline)
# Ensure one row per school-year before assigning quartiles
school_enroll <- v6 %>%
  filter(category_type == "Race/Ethnicity", subgroup == "All Students") %>%
  group_by(cds_school, academic_year) %>%
  summarise(
    total_enrollment_all = first(cumulative_enrollment),
    .groups = "drop"
  ) %>%
  group_by(academic_year) %>%
  mutate(enrollment_q = ntile(total_enrollment_all, 4)) %>%
  ungroup()

v6_enroll_q <- v6 %>%
  left_join(
    school_enroll %>% select(cds_school, academic_year, enrollment_q),
    by = c("cds_school", "academic_year"),
    relationship = "many-to-one"
  )
by_enrollment_group_type <- v6_enroll_q %>%
  filter(!is.na(enrollment_q)) %>%
  group_by(category_type, subgroup, academic_year, enrollment_q, school_group, school_type) %>%
  summarise(
    suspensions = sum(total_suspensions, na.rm = TRUE),
    enrollment  = sum(cumulative_enrollment, na.rm = TRUE),
    rate        = if_else(enrollment > 0, suspensions / enrollment, NA_real_),
    .groups     = "drop"
  )

by_enrollment_type <- v6_enroll_q %>%
  filter(!is.na(enrollment_q)) %>%
  group_by(category_type, subgroup, academic_year, enrollment_q, school_type) %>%
  summarise(
    suspensions = sum(total_suspensions, na.rm = TRUE),
    enrollment  = sum(cumulative_enrollment, na.rm = TRUE),
    rate        = if_else(enrollment > 0, suspensions / enrollment, NA_real_),
    .groups     = "drop"
  ) %>%
  mutate(school_group = "All")

by_enrollment_group <- v6_enroll_q %>%
  filter(!is.na(enrollment_q)) %>%
  group_by(category_type, subgroup, academic_year, enrollment_q, school_group) %>%
  summarise(
    suspensions = sum(total_suspensions, na.rm = TRUE),
    enrollment  = sum(cumulative_enrollment, na.rm = TRUE),
    rate        = if_else(enrollment > 0, suspensions / enrollment, NA_real_),
    .groups     = "drop"
  ) %>%
  mutate(school_type = "All")

by_enrollment_overall <- v6_enroll_q %>%
  filter(!is.na(enrollment_q)) %>%
  group_by(category_type, subgroup, academic_year, enrollment_q) %>%
  summarise(
    suspensions = sum(total_suspensions, na.rm = TRUE),
    enrollment  = sum(cumulative_enrollment, na.rm = TRUE),
    rate        = if_else(enrollment > 0, suspensions / enrollment, NA_real_),
    .groups     = "drop"
  ) %>%
  mutate(school_group = "All", school_type = "All")

by_enrollment <- bind_rows(
  by_enrollment_group_type,
  by_enrollment_type,
  by_enrollment_group,
  by_enrollment_overall
)

write_parquet(by_enrollment, here("data-stage", "quartile_rates_by_enrollment.parquet"))


# Quartiles by racial proportion example: Black share of enrollment
# Compute proportion of Black enrollment out of school total and derive quartiles

# verify no duplicate campus-year entries with differing enrollment
enrollment_keys <- c("cds_school", "academic_year")
extra_keys <- intersect(c("reason", "grade"), names(v6))
# drop optional keys that are entirely NA to avoid join mismatches
extra_keys <- extra_keys[vapply(v6[extra_keys], function(x) any(!is.na(x)), logical(1))]
enrollment_keys <- c(enrollment_keys, extra_keys)

dup_check <- v6 %>%
  filter(subgroup == "All Students") %>%
  group_by(across(all_of(enrollment_keys))) %>%
  summarise(n_enroll = n_distinct(cumulative_enrollment), .groups = "drop") %>%
  filter(n_enroll > 1)
stopifnot(nrow(dup_check) == 0)

all_enroll <- v6 %>%
  filter(subgroup == "All Students") %>%
  group_by(across(all_of(enrollment_keys))) %>%
  summarise(
    total_enrollment_all = first(cumulative_enrollment),
    .groups = "drop"
  )

black_prop <- v6 %>%
  # Join Black student data with totals to compute proportions
  filter(subgroup == "Black/African American") %>%
  left_join(
    all_enroll,
    by = enrollment_keys,
    relationship = "one-to-one",
    na_matches = "na"
  ) %>%
  group_by(cds_school, academic_year) %>%
  summarise(
    total_suspensions = sum(total_suspensions, na.rm = TRUE),
    black_enrollment   = sum(cumulative_enrollment, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  left_join(all_enroll, by = c("cds_school", "academic_year")) %>%
  mutate(
    black_prop = if_else(
      total_enrollment_all > 0,
      black_enrollment / total_enrollment_all,
      NA_real_
    )
  ) %>%
  rename(cumulative_enrollment = black_enrollment)

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

####Tests to ensure accuracy####

#1. Test A##
dup_rows <- v6 %>%
  filter(category_type == "Race/Ethnicity",
         subgroup == "Black/African American") %>%
  count(cds_school, academic_year, cumulative_enrollment) %>%
  add_count(cds_school, academic_year, name = "n_rows") %>%
  filter(n_rows > 1)

nrow(dup_rows)


##2. Test 2##
# after running the script
file.exists(here("data-stage", "statewide_totals.parquet"))
file.exists(here("data-stage", "statewide_totals_breakdowns.parquet"))
file.exists(here("data-stage", "quartile_rates_by_enrollment.parquet"))
file.exists(here("data-stage", "quartile_rates_by_black_prop.parquet"))

###END TEST###
