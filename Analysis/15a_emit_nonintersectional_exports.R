# analysis/15a_emit_nonintersectional_exports.R
# === Non-intersectional exports for tail/pareto ===
suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(janitor)
  library(stringr)
  library(tidyr)
  library(here)
})

try(here::i_am("Analysis/15a_emit_nonintersectional_exports.R"), silent = TRUE)

# Load repository utilities
source(here("R", "utils_keys_filters.R"))

# -------------------------------------------------------------------
# Config + helpers
# -------------------------------------------------------------------
REASON_COLS <- c("r_vi","r_vn","r_wp","r_id","r_def","r_oth")

# select only columns that exist (avoids select() errors)
select_existing <- function(df, wanted) {
  keep <- intersect(wanted, names(df))
  dplyr::select(df, dplyr::all_of(keep))
}

# canonicalize "setting" from ed_ops_name
setting_from_ops <- function(ed_ops) dplyr::case_when(
  ed_ops == "Traditional" ~ "Traditional",
  !is.na(ed_ops)          ~ "Non-traditional",
  TRUE                    ~ NA_character_
)

# Resolve “unduplicated students suspended (total)” aliases to one canonical name
canonicalize_undup <- function(df) {
  if ("unduplicated_count_of_students_suspended_total" %in% names(df)) return(df)
  # common variants you might see in OTH
  alias_list <- c(
    "unduplicated_count_students_suspended_total",
    "unduplicated_students_suspended_total",
    "unduplicated_count_of_students_suspended",      # missing “_total”
    "unduplicated_students_suspended",
    "unduplicated_count_suspended_total",
    "undup_students_total",
    "undup_total"
  )
  hit <- intersect(alias_list, names(df))
  if (length(hit) == 0) {
    # permissive regex fallback
    rx <- "(?i)unduplicat.*student.*suspend.*(total)?"
    hit <- names(df)[stringr::str_detect(names(df), rx)]
  }
  if (length(hit)) {
    df <- df %>%
      mutate(unduplicated_count_of_students_suspended_total = .data[[hit[1]]])
  } else {
    df$unduplicated_count_of_students_suspended_total <- NA_real_
  }
  # ensure numeric if possible
  df %>%
    mutate(unduplicated_count_of_students_suspended_total =
             suppressWarnings(as.numeric(unduplicated_count_of_students_suspended_total)))
}

# -------------------------------------------------------------------
# 1) Read race-long (has reason columns)
# -------------------------------------------------------------------
RACE_LONG_PATH <- here("data-stage", "susp_v6_long.parquet")
stopifnot(file.exists(RACE_LONG_PATH))
####
RACE_LONG_CANDIDATES <- c(
  here("data-stage", "susp_v6_long_strict.parquet"),
  here("data-stage", "susp_v6_long.parquet")
)
RACE_LONG_PATH <- RACE_LONG_CANDIDATES[which(file.exists(RACE_LONG_CANDIDATES))][1]
if (is.na(RACE_LONG_PATH)) {
  stop("[15a] Needed long race file missing. Expected one of:\n  - data-stage/susp_v6_long_strict.parquet\n  - data-stage/susp_v6_long.parquet")
}
#######

race_long <- arrow::read_parquet(RACE_LONG_PATH) %>%
  clean_names() %>%
  build_keys()

race_long <- race_long %>%
  mutate(year = suppressWarnings(as.integer(substr(as.character(academic_year), 1, 4))))

present_reason_cols <- intersect(REASON_COLS, names(race_long))
if ("can_assume_zero" %in% names(race_long) && length(present_reason_cols)) {
  race_long <- race_long %>%
    mutate(across(all_of(present_reason_cols),
                  ~ ifelse(can_assume_zero, dplyr::coalesce(.x, 0), .x)))
}

# -------------------------------------------------------------------
# 2) Read OTH/demographic long (Sex, SPED, SED, EL, Foster, Migrant, Homeless)
# -------------------------------------------------------------------
OTH_PARQUET <- here("data-stage", "oth_long.parquet")
if (!file.exists(OTH_PARQUET)) {
  stop("[15a] Missing file: data-stage/oth_long.parquet. Run analysis/01b_ingest_demographics.R first.")
}
demo_data <- arrow::read_parquet(OTH_PARQUET) %>%
  clean_names() %>%
  build_keys() %>%
  mutate(year = suppressWarnings(as.integer(substr(as.character(academic_year), 1, 4)))) %>%
  canonicalize_undup()

present_reason_cols_oth <- intersect(REASON_COLS, names(demo_data))
if ("can_assume_zero" %in% names(demo_data) && length(present_reason_cols_oth)) {
  demo_data <- demo_data %>%
    mutate(across(all_of(present_reason_cols_oth),
                  ~ ifelse(can_assume_zero, dplyr::coalesce(.x, 0), .x)))
}

# -------------------------------------------------------------------
# 3) Canonical school/year attributes from v6 (source of truth)
#     join with suffix ".v6", then coalesce, then drop extras
# --- v6 keys + robust campus filter (with fallback) ---
v6_path <- here("data-stage", "susp_v6_long.parquet")
stopifnot(file.exists(v6_path))

# 1) Read and keep aggregate_level if present
v6_keys_raw <- arrow::read_parquet(v6_path) %>%
  clean_names() %>%
  build_keys() %>%
  select(
    academic_year, county_code, district_code, school_code,
    county_name, district_name, school_name,
    ed_ops_name, school_level, locale_simple,
    any_of("aggregate_level")          # <- keep if present
  ) %>%
  distinct()

# 2) Fallback: infer aggregate_level == "S" for real campuses if missing/empty
needs_fallback <- (!"aggregate_level" %in% names(v6_keys_raw)) ||
  all(is.na(v6_keys_raw$aggregate_level))

v6_keys <- if (needs_fallback) {
  v6_keys_raw %>%
    mutate(
      aggregate_level = dplyr::case_when(
        nchar(school_code) == 7 & !school_code %in% c("0000000","0000001") ~ "S",
        TRUE ~ "X"  # unknown / non-school
      )
    )
} else {
  # normalize case/whitespace just in case
  v6_keys_raw %>%
    mutate(aggregate_level = stringr::str_trim(stringr::str_to_upper(aggregate_level)))
}

# 3) Campus-only filter (excludes district agg 0000000 and NPS 0000001)
v6_schools_only <- v6_keys %>%
  filter(
    stringr::str_to_upper(aggregate_level) == "S",
    nchar(school_code) == 7,
    !school_code %in% c("0000000", "0000001")
  )
        # -------------------------------------------------------------------
                    # ---- race_long + v6-------------#
race_joined <- left_join(
  race_long, v6_keys,
  by = c("academic_year","county_code","district_code","school_code"),
  relationship = "many-to-one",
  suffix = c("", ".v6")
)

race_long <- race_joined %>%
  mutate(
    county_name   = coalesce(!!!rlang::syms(intersect(c("county_name","county_name.v6","county_name.x","county_name.y"), names(race_joined)))),
    district_name = coalesce(!!!rlang::syms(intersect(c("district_name","district_name.v6","district_name.x","district_name.y"), names(race_joined)))),
    school_name   = coalesce(!!!rlang::syms(intersect(c("school_name","school_name.v6","school_name.x","school_name.y"), names(race_joined)))),
    ed_ops_name   = coalesce(!!!rlang::syms(intersect(c("ed_ops_name","ed_ops_name.v6","ed_ops_name.x","ed_ops_name.y"), names(race_joined)))),
    school_level  = coalesce(!!!rlang::syms(intersect(c("school_level","school_level.v6","school_level.x","school_level.y"), names(race_joined)))),
    locale_simple = coalesce(!!!rlang::syms(intersect(c("locale_simple","locale_simple.v6","locale_simple.x","locale_simple.y"), names(race_joined)))),
    setting       = setting_from_ops(ed_ops_name)
  ) %>%
  select(-any_of(c(
    "county_name.v6","district_name.v6","school_name.v6",
    "ed_ops_name.v6","school_level.v6","locale_simple.v6",
    "county_name.x","district_name.x","school_name.x",
    "ed_ops_name.x","school_level.x","locale_simple.x",
    "county_name.y","district_name.y","school_name.y",
    "ed_ops_name.y","school_level.y","locale_simple.y"
  )))

# ---- demo_data + v6
demo_joined <- left_join(
  demo_data, v6_keys,
  by = c("academic_year","county_code","district_code","school_code"),
  relationship = "many-to-one",
  suffix = c("", ".v6")
)

demo_data <- demo_joined %>%
  mutate(
    county_name   = coalesce(!!!rlang::syms(intersect(c("county_name","county_name.v6","county_name.x","county_name.y"), names(demo_joined)))),
    district_name = coalesce(!!!rlang::syms(intersect(c("district_name","district_name.v6","district_name.x","district_name.y"), names(demo_joined)))),
    school_name   = coalesce(!!!rlang::syms(intersect(c("school_name","school_name.v6","school_name.x","school_name.y"), names(demo_joined)))),
    ed_ops_name   = coalesce(!!!rlang::syms(intersect(c("ed_ops_name","ed_ops_name.v6","ed_ops_name.x","ed_ops_name.y"), names(demo_joined)))),
    school_level  = coalesce(!!!rlang::syms(intersect(c("school_level","school_level.v6","school_level.x","school_level.y"), names(demo_joined)))),
    locale_simple = coalesce(!!!rlang::syms(intersect(c("locale_simple","locale_simple.v6","locale_simple.x","locale_simple.y"), names(demo_joined)))),
    setting       = setting_from_ops(ed_ops_name)
  ) %>%
  select(-any_of(c(
    "county_name.v6","district_name.v6","school_name.v6",
    "ed_ops_name.v6","school_level.v6","locale_simple.v6",
    "county_name.x","district_name.x","school_name.x",
    "ed_ops_name.x","school_level.x","locale_simple.x",
    "county_name.y","district_name.y","school_name.y",
    "ed_ops_name.y","school_level.y","locale_simple.y"
  )))

# Sanity
message("[15a] missing attrs in race_long: ",
        sum(is.na(race_long$ed_ops_name) |
              is.na(race_long$school_level) |
              is.na(race_long$locale_simple)))
message("[15a] missing attrs in OTH: ",
        sum(is.na(demo_data$ed_ops_name) |
              is.na(demo_data$school_level) |
              is.na(demo_data$locale_simple)))

# -------------------------------------------------------------------
# 4) Build outputs
# -------------------------------------------------------------------
# TA-only (All Students): one row per school×year
present_reason_cols <- intersect(REASON_COLS, names(race_long))

all_students <- race_long %>%
  filter(category_type == "Race/Ethnicity", subgroup == "All Students") %>%
  inner_join(
    v6_schools_only %>% select(academic_year, county_code, district_code, school_code),
    by = c("academic_year","county_code","district_code","school_code")
  ) %>%
  select_existing(c(
    "year","academic_year",
    "county_code","district_code","school_code",
    "county_name","district_name","school_name",
    "ed_ops_name","setting","school_level",
    "cumulative_enrollment","total_suspensions",
    "unduplicated_count_of_students_suspended_total",
    present_reason_cols
  )) %>%
  rename(undup_total = unduplicated_count_of_students_suspended_total) %>%
  distinct() # drop exact duplicates across these fields

# Non-intersectional stack (Race/Ethnicity + Other)
# Race/Ethnicity (campus-only)
race_non_ta <- race_long %>%
  filter(subgroup != "All Students") %>%
  inner_join(
    v6_schools_only %>% select(academic_year, county_code, district_code, school_code),
    by = c("academic_year","county_code","district_code","school_code")
  ) %>%
  transmute(
    year, academic_year,
    county_code, district_code, school_code,
    county_name, district_name, school_name,
    ed_ops_name, setting, school_level,
    reporting_domain = "Race/Ethnicity",
    subgroup_label = coalesce(subgroup_description, subgroup),
    cumulative_enrollment, total_suspensions,
    undup_total = unduplicated_count_of_students_suspended_total,
    !!!syms(present_reason_cols)
  ) %>%
  distinct()

# OTH (campus-only)
oth_norm <- demo_data %>%
  inner_join(
    v6_schools_only %>% select(academic_year, county_code, district_code, school_code),
    by = c("academic_year","county_code","district_code","school_code")
  ) %>%
  transmute(
    year, academic_year,
    county_code, district_code, school_code,
    county_name, district_name, school_name,
    ed_ops_name, setting, school_level,
    reporting_domain = category_type,
    subgroup_label = coalesce(subgroup, subgroup_code),
    cumulative_enrollment, total_suspensions,
    undup_total = unduplicated_count_of_students_suspended_total,
    !!!syms(present_reason_cols_oth)
  ) %>%
  distinct()

non_intersectional <- bind_rows(race_non_ta, oth_norm)

#sanity check
dup_ta <- all_students %>% count(year, school_code, name = "n") %>% filter(n > 1)
nrow(dup_ta)  # should be 0 if working properly

# -------------------------------------------------------------------
# 5) Write files
# -------------------------------------------------------------------
out_dir <- here("outputs", "data-merged")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
arrow::write_parquet(all_students, file.path(out_dir, "school_year_allstudents.parquet"))
arrow::write_parquet(non_intersectional, file.path(out_dir, "school_year_subgroups_nonintersectional.parquet"))

message("[15a] Wrote: ", file.path(out_dir, "school_year_allstudents.parquet"))
message("[15a] Wrote: ", file.path(out_dir, "school_year_subgroups_nonintersectional.parquet"))

# ---- updated ----
present_reason_cols <- intersect(REASON_COLS, names(race_long))

nps_by_district <- race_long %>%
  filter(category_type == "Race/Ethnicity", subgroup == "All Students", school_code == "0000001") %>%
  group_by(
    year, academic_year,
    county_code, district_code,
    county_name, district_name
  ) %>%
  summarise(
    cumulative_enrollment = sum(cumulative_enrollment, na.rm = TRUE),
    total_suspensions     = sum(total_suspensions, na.rm = TRUE),
    undup_total           = sum(unduplicated_count_of_students_suspended_total, na.rm = TRUE),
    across(all_of(present_reason_cols), ~ sum(.x, na.rm = TRUE)),
    .groups = "drop"
  )

arrow::write_parquet(nps_by_district, file.path(out_dir, "district_year_nps.parquet"))
message("[15a] Wrote: ", file.path(out_dir, "district_year_nps.parquet"))

# Quick sanity check: no TA dup rows per school×year
try({
  dup_ta <- all_students %>%
    count(year, school_code, name = "n") %>%
    filter(n > 1)
  if (nrow(dup_ta)) warning("[15a] TA duplicates found; inspect `dup_ta`.")
})


# End of file

#----------------------------##test###-------------------###
# Check the duplicates
dup_ta <- all_students %>%
  count(year, school_code, name = "n") %>%
  filter(n > 1)

# See how many duplicates and which schools
nrow(dup_ta)
head(dup_ta)

# Look at the actual duplicate rows
example_dup <- all_students %>%
  filter(school_code %in% head(dup_ta$school_code, 3)) %>%
  arrange(school_code, year)
View(example_dup)

#----------------------------------###test 2####
# any TA dupes left?
dup_ta <- all_students %>%
  count(year, school_code, name = "n") %>%
  filter(n > 1)

nrow(dup_ta)
head(dup_ta)
