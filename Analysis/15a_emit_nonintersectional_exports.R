# === [NEW] Non-intersectional exports for tail/pareto ===
suppressPackageStartupMessages({
  library(arrow); library(dplyr); library(janitor); library(stringr)
  library(tidyr); library(here)
})

# -------------------------------------------------------------------
# Config + helpers
# -------------------------------------------------------------------
REASON_COLS <- c("r_vi","r_vn","r_wp","r_id","r_def","r_oth")

# Safe key padding if pad_keys() isn’t in scope
norm_code <- function(x, width) {
  x_chr <- as.character(x)
  x_num <- stringr::str_replace_all(x_chr, "\\D", "")
  stringr::str_pad(x_num, width = width, side = "left", pad = "0")
}
pad_keys_local <- function(df) {
  df %>%
    mutate(
      academic_year = as.character(academic_year),
      across(any_of("county_code"),   ~ norm_code(.x, 2)),
      across(any_of("district_code"), ~ norm_code(.x, 5)),
      across(any_of("school_code"),   ~ norm_code(.x, 7))
    )
}
maybe_pad <- function(df) if (exists("pad_keys")) pad_keys(df) else pad_keys_local(df)

# -------------------------------------------------------------------
# 1) Pick + READ a race-long source (has reason columns)
# -------------------------------------------------------------------
RACE_LONG_CANDIDATES <- c(
  here("data-stage", "susp_v5_long_strict.parquet"),
  here("data-stage", "susp_v5_long.parquet")
)
RACE_LONG_PATH <- RACE_LONG_CANDIDATES[which(file.exists(RACE_LONG_CANDIDATES))][1]
if (is.na(RACE_LONG_PATH)) {
  stop("[15a] Needed long race file missing. Expected one of:\n  - data-stage/susp_v5_long_strict.parquet\n  - data-stage/susp_v5_long.parquet")
}

race_long <- arrow::read_parquet(RACE_LONG_PATH) %>%
  janitor::clean_names() %>%
  maybe_pad() %>%
  mutate(year = suppressWarnings(as.integer(substr(as.character(academic_year), 1, 4))))

# Reason columns: fill zeros only when can_assume_zero==TRUE and col exists
present_reason_cols <- intersect(REASON_COLS, names(race_long))
if ("can_assume_zero" %in% names(race_long) && length(present_reason_cols)) {
  race_long <- race_long %>%
    mutate(across(all_of(present_reason_cols),
                  ~ ifelse(can_assume_zero, dplyr::coalesce(.x, 0), .x)))
}

# -------------------------------------------------------------------
# 2) Read OTH/demographic long (for Sex, SPED, SED, EL, Foster, Migrant, Homeless)
# -------------------------------------------------------------------
OTH_PARQUET <- here("data-stage", "oth_long.parquet")
if (!file.exists(OTH_PARQUET)) {
  stop("[15a] Missing file: data-stage/oth_long.parquet. Run analysis/01b_ingest_demographics.R first.")
}
demo_data <- arrow::read_parquet(OTH_PARQUET) %>%
  janitor::clean_names() %>%
  maybe_pad()

present_reason_cols_oth <- intersect(REASON_COLS, names(demo_data))
if ("can_assume_zero" %in% names(demo_data) && length(present_reason_cols_oth)) {
  demo_data <- demo_data %>%
    mutate(across(all_of(present_reason_cols_oth),
                  ~ ifelse(can_assume_zero, dplyr::coalesce(.x, 0), .x)))
}

# -------------------------------------------------------------------
# 3) Canonical school/year attributes from v5 (source of truth)
# -------------------------------------------------------------------
v5_path <- here("data-stage", "susp_v5.parquet")
stopifnot(file.exists(v5_path))

v5_keys <- arrow::read_parquet(v5_path) %>%
  janitor::clean_names() %>%
  maybe_pad() %>%
  select(
    academic_year, county_code, district_code, school_code,
    ed_ops_name, school_level_final, level_strict3, locale_simple
  ) %>%
  distinct()

# Apply to race_long
race_long <- race_long %>%
  maybe_pad() %>%
  left_join(
    v5_keys,
    by = c("academic_year","county_code","district_code","school_code"),
    relationship = "many-to-one"
  ) %>%
  mutate(
    setting = dplyr::case_when(
      ed_ops_name == "Traditional" ~ "Traditional",
      !is.na(ed_ops_name)          ~ "Non-traditional",
      TRUE                         ~ NA_character_
    )
  )

# Apply to OTH
demo_data <- demo_data %>%
  maybe_pad() %>%
  left_join(
    v5_keys,
    by = c("academic_year","county_code","district_code","school_code"),
    relationship = "many-to-one"
  ) %>%
  mutate(
    setting = dplyr::case_when(
      ed_ops_name == "Traditional" ~ "Traditional",
      !is.na(ed_ops_name)          ~ "Non-traditional",
      TRUE                         ~ NA_character_
    )
  )

# Optional sanity for missing attrs
race_missing <- any(is.na(race_long$ed_ops_name) | is.na(race_long$school_level_final) |
                      is.na(race_long$level_strict3) | is.na(race_long$locale_simple))
demo_missing <- any(is.na(demo_data$ed_ops_name) | is.na(demo_data$school_level_final) |
                      is.na(demo_data$level_strict3) | is.na(demo_data$locale_simple))
if (race_missing) warning("[15a] Some race_long rows still missing school attributes after join.")
if (demo_missing) warning("[15a] Some OTH rows still missing school attributes after join.")

# -------------------------------------------------------------------
# 4) Build outputs
# -------------------------------------------------------------------
# TA-only (All Students): one row per school×year
all_students <- race_long %>%
  filter(reporting_category == "TA") %>%
  select(
    year, academic_year,
    county_code, district_code, school_code,
    county_name, district_name, school_name,
    ed_ops_name, setting, school_level_final,
    cumulative_enrollment, total_suspensions,
    unduplicated_count_of_students_suspended_total,
    all_of(present_reason_cols)
  ) %>%
  rename(undup_total = unduplicated_count_of_students_suspended_total)

# Race non-TA stack
race_non_ta <- race_long %>%
  filter(reporting_category != "TA") %>%
  transmute(
    year, academic_year,
    county_code, district_code, school_code,
    county_name, district_name, school_name,
    ed_ops_name, setting, school_level_final,
    reporting_domain = "Race/Ethnicity",
    subgroup_label = dplyr::coalesce(reporting_category_description, reporting_category),
    cumulative_enrollment, total_suspensions,
    undup_total = unduplicated_count_of_students_suspended_total,
    !!!dplyr::syms(present_reason_cols)
  )

# OTH side (Sex, SPED, SED, EL, Foster, Migrant, Homeless)
oth_norm <- demo_data %>%
  mutate(year = suppressWarnings(as.integer(substr(as.character(academic_year), 1, 4)))) %>%
  transmute(
    year, academic_year,
    county_code, district_code, school_code,
    county_name, district_name, school_name,
    ed_ops_name, setting, school_level_final,
    reporting_domain = category_type,       # e.g., Sex, Special Education, etc.
    subgroup_label = dplyr::coalesce(subgroup, subgroup_code),
    cumulative_enrollment, total_suspensions,
    undup_total = unduplicated_count_of_students_suspended_total,
    !!!dplyr::syms(present_reason_cols_oth)
  )

non_intersectional <- dplyr::bind_rows(race_non_ta, oth_norm)

# -------------------------------------------------------------------
# 5) Write files
# -------------------------------------------------------------------
out_dir <- here("outputs", "data-merged")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
arrow::write_parquet(all_students, file.path(out_dir, "school_year_allstudents.parquet"))
arrow::write_parquet(non_intersectional, file.path(out_dir, "school_year_subgroups_nonintersectional.parquet"))

message("[15a] Wrote: ", file.path(out_dir, "school_year_allstudents.parquet"))
message("[15a] Wrote: ", file.path(out_dir, "school_year_subgroups_nonintersectional.parquet"))

# -------------------------------------------------------------------
# 6) Quick sanity: no duplicate TA rows per school×year
# -------------------------------------------------------------------
try({
  dup_ta <- all_students %>%
    count(year, school_code, name = "n") %>%
    filter(n > 1)
  if (nrow(dup_ta)) warning("[15a] TA duplicates found; inspect `dup_ta`.")
})
# End of file
