# R/01c_ingest_teacher_demographics.R
# Ingest teacher demographic TXT extracts, standardize, and stage as Parquet.

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(stringr)
  library(janitor)
  library(purrr)
  library(arrow)
  library(here)
  library(tibble)
  library(tidyr)
})

source("R/ingest_helpers.R")
source("R/teacher_processing.R")
source("R/utils_keys_filters.R")

TEACHER_RAW_DIR <- Sys.getenv("TEACHER_RAW_DIR")
if (!nzchar(TEACHER_RAW_DIR)) {
  TEACHER_RAW_DIR <- here("data-raw")
}

OUT_PARQUET <- here("data-stage", "teacher_staff_long.parquet")

if (!dir.exists(TEACHER_RAW_DIR)) {
  stop("Teacher raw directory not found: ", TEACHER_RAW_DIR,
       "\nSet TEACHER_RAW_DIR or place stre*.txt under data-raw/.")
}

files <- list.files(TEACHER_RAW_DIR, pattern = "^stre\\d{4}\\.txt$", full.names = TRUE)
if (!length(files)) {
  stop("No stre*.txt teacher files located under ", TEACHER_RAW_DIR)
}

message("[01c] Found ", length(files), " teacher TXT files.")

rename_first <- function(df, new_name, candidates) {
  hit <- pick_col(df, c(new_name, candidates), required = FALSE)
  if (!is.na(hit) && hit != new_name) {
    df <- dplyr::rename(df, !!new_name := !!rlang::sym(hit))
  }
  df
}

derive_year_from_file <- function(path) {
  digits <- stringr::str_extract(basename(path), "(?<=stre)\\d{4}")
  if (is.na(digits)) return(list(year = NA_integer_, academic_year = NA_character_))
  start <- suppressWarnings(as.integer(paste0("20", substr(digits, 1, 2))))
  end   <- suppressWarnings(as.integer(paste0("20", substr(digits, 3, 4))))
  list(
    year = end,
    academic_year = if (!is.na(start) && !is.na(end))
      paste0(start, "-", substr(end, 3, 4)) else NA_character_
  )
}

# CDE-allowed grade span codes (user-provided)
ALLOWED_GRADE_SPANS <- c("ALL","GS_K6","GS_69","GS_912","GS_K12")

read_teacher_txt <- function(path) {
  message("  - reading ", basename(path))
  raw <- tryCatch(
    readr::read_delim(
      file = path,
      delim = "\t",
      col_types = readr::cols(.default = readr::col_character()),
      na = c("", "NA", "N/A", "NULL", "*"),
      progress = FALSE,
      quote = "",
      escape_double = FALSE,
      escape_backslash = FALSE,
      trim_ws = TRUE,
      show_col_types = FALSE
    ),
    error = function(e) {
      message("    readr::read_delim failed (", conditionMessage(e), ")")
      message("    falling back to utils::read.delim")
      utils::read.delim(
        file = path,
        sep = "\t",
        header = TRUE,
        stringsAsFactors = FALSE,
        na.strings = c("", "NA", "N/A", "NULL", "*"),
        check.names = FALSE,
        quote = ""
      ) |> tibble::as_tibble()
    }
  ) |> janitor::clean_names()
  
  # After: ) |> janitor::clean_names()
  # Log parsing issues, if any
  if (inherits(raw, "spec_tbl_df")) {
    pb <- tryCatch(problems(raw), error = function(e) NULL)
    if (!is.null(pb) && nrow(pb)) {
      message("    NOTE: ", nrow(pb), " parsing issue(s) in ", basename(path), " (use problems() to inspect).")
    }
  }
  
  
  # Keep provenance
  raw <- raw |> mutate(source_file = basename(path))
  
  # Drop any leftover header lines that leaked into the data
  # e.g., staff_gender_code == "ACADEMIC YEAR" / "DISTRICT CODE" / "COUNTY CODE" / "SCHOOL CODE"
  if ("staff_gender_code" %in% names(raw)) {
    raw <- raw %>%
      filter(!staff_gender_code %in% c("ACADEMIC YEAR","DISTRICT CODE","COUNTY CODE","SCHOOL CODE"))
  }
  
  # Derive year fields from data or filename
  year_info <- derive_year(raw)
  file_info <- derive_year_from_file(path)
  
  if (!"year" %in% names(raw) || all(is.na(raw$year))) {
    raw$year <- year_info$year
  }
  if (!"academic_year" %in% names(raw) || all(is.na(raw$academic_year))) {
    raw$academic_year <- year_info$academic_year
  }
  if (all(is.na(raw$year))) raw$year <- file_info$year
  if (all(is.na(raw$academic_year))) raw$academic_year <- file_info$academic_year
  
  # Canonicalize common columns
  raw <- raw |>
    rename_first("county_code", c("county_cd", "cnty_cd", "countyid")) |>
    rename_first("district_code", c("district_cd", "districtid", "dist_cd")) |>
    rename_first("school_code", c("school_cd", "schoolid", "sch_cd")) |>
    rename_first("county_name", c("county", "county_nm")) |>
    rename_first("district_name", c("district", "district_nm")) |>
    rename_first("school_name", c("school", "school_nm")) |>
    rename_first("reporting_category", c("teacher_reporting_category", "report_cat")) |>
    rename_first("reporting_category_description", c("reporting_category_desc", "report_desc")) |>
    rename_first("charter_yn", c("charter_school", "charter")) |>
    rename_first("staff_gender_code", c("staff_gender", "gender_code")) |>
    rename_first("race_ethnicity", c("teacher_race_ethnicity", "ethnicity", "race")) |>
    rename_first("school_grade_span", c("grade_span", "grade_span_code", "school_grade_span_code"))
  
  raw <- raw |> mutate(
    across(any_of(c("county_code", "district_code", "school_code", "aggregate_level",
                    "charter_yn", "reporting_category", "reporting_category_description",
                    "race_ethnicity", "staff_gender_code", "district_name",
                    "school_name", "county_name", "academic_year", "school_grade_span")),
           ~ stringr::str_squish(as.character(.x)))
  )
  
  # Normalise charter
  if (!"charter_yn" %in% names(raw)) raw$charter_yn <- NA_character_
  raw <- raw |> mutate(
    charter_yn = stringr::str_to_lower(charter_yn),
    charter_yn = dplyr::case_when(
      charter_yn %in% c("yes","y") ~ "Yes",
      charter_yn %in% c("no","n")  ~ "No",
      charter_yn %in% c("all","a") ~ NA_character_,
      TRUE ~ charter_yn
    )
  )
  if (!all(is.na(raw$charter_yn))) {
    raw <- raw |> filter(charter_yn %in% c("Yes","No"))
  }
  
  # Aggregate level to "S"
  raw <- raw |> rename_first("aggregate_level", c("aggregation_level", "aggregation_type", "agg_level"))
  if (!"aggregate_level" %in% names(raw)) raw$aggregate_level <- NA_character_
  raw <- raw |> mutate(
    aggregate_level = stringr::str_squish(stringr::str_to_upper(aggregate_level)),
    aggregate_level = dplyr::case_when(
      aggregate_level %in% c("S","SCH","SCHOOL","SCHOOL LEVEL","SCHOOL-LEVEL") ~ "S",
      TRUE ~ aggregate_level
    )
  )
  
  # Staff gender label
  raw <- raw |> mutate(
    staff_gender_code = stringr::str_to_upper(dplyr::coalesce(staff_gender_code, "")),
    staff_gender = teacher_gender_label(staff_gender_code)
  )
  
  # School grade span: keep as categorical code; validate against CDE list
  if (!"school_grade_span" %in% names(raw)) raw$school_grade_span <- NA_character_
  raw <- raw |>
    mutate(
      school_grade_span = stringr::str_to_upper(stringr::str_squish(school_grade_span)),
      school_grade_span = ifelse(nzchar(school_grade_span), school_grade_span, NA_character_),
      school_grade_span = ifelse(!(school_grade_span %in% ALLOWED_GRADE_SPANS), NA_character_, school_grade_span),
      # For school-level rows, "ALL" is not applicable per CDE note
      school_grade_span = ifelse(aggregate_level == "S" & school_grade_span == "ALL", NA_character_, school_grade_span)
    )
  
  # Defensive check (non-fatal warning -> convert to message if preferred)
  bad_span <- raw |> filter(aggregate_level == "S", school_grade_span == "ALL")
  if (nrow(bad_span)) {
    warning("[01c] Found ", nrow(bad_span), " school-level rows with school_grade_span == 'ALL' (coercing to NA).")
  }
  
  # Numeric parsing: exclude IDs and text columns (incl. source_file & school_grade_span)
  numeric_candidates <- names(raw)[vapply(raw, teacher_numeric_like, logical(1))]
  id_cols <- intersect(names(raw), c(
    "county_code","district_code","school_code","cds_district","cds_school",
    "aggregate_level","charter_yn","reporting_category","staff_gender_code",
    "district_name","school_name","county_name","academic_year","source_file",
    "school_grade_span","reporting_category_description","race_ethnicity","staff_gender"
  ))
  numeric_cols <- setdiff(numeric_candidates, id_cols)
  if (length(numeric_cols)) {
    raw <- raw |> mutate(
      across(
        all_of(numeric_cols),
        ~ suppressWarnings(readr::parse_number(as.character(.x)))
      )
    )
  }
  
  raw <- raw |> mutate(
    year = suppressWarnings(as.integer(year)),
    source_file = as.character(source_file) # enforce
  )
  
  raw
}

teacher_list <- purrr::map(files, read_teacher_txt)
teacher_all  <- purrr::list_rbind(teacher_list)

if (!nrow(teacher_all)) stop("Teacher TXT files yielded no rows.")

teacher_all <- teacher_all |>
  mutate(
    academic_year = stringr::str_squish(as.character(academic_year)),
    aggregate_level = dplyr::coalesce(aggregate_level, "S")
  ) |>
  build_keys()

# Only campus rows
teacher_all <- teacher_all |> filter_campus_only()

# figure out which numeric value columns to sum
value_cols <- teacher_value_columns(teacher_all)

key_cols <- intersect(names(teacher_all), c(
  "academic_year","year","county_code","district_code","school_code",
  "cds_school","aggregate_level","charter_yn","reporting_category",
  "reporting_category_description","race_ethnicity","staff_gender_code","staff_gender",
  "school_grade_span"
))

# aggregate & preserve provenance
teacher_all <- teacher_all |>
  group_by(across(all_of(key_cols))) |>
  summarise(
    across(all_of(value_cols), ~ sum(.x, na.rm = TRUE)),
    source_file = paste(unique(source_file), collapse = ";"),
    .groups = "drop"
  ) |>
  arrange(dplyr::across(dplyr::any_of(c(
    "academic_year","cds_school","reporting_category",
    "staff_gender_code","race_ethnicity"
  ))))

# ensure gender labels are filled
teacher_all <- teacher_all |> mutate(
  staff_gender = teacher_gender_label(staff_gender_code, fallback = staff_gender)
)

# If total_staff duplicates total_staff_count, drop it
if (all(c("total_staff","total_staff_count") %in% names(teacher_all))) {
  same <- teacher_all %>%
    transmute(eq = (coalesce(total_staff, 0) == coalesce(total_staff_count, 0))) %>%
    pull(eq)
  if (all(same, na.rm = TRUE)) {
    teacher_all <- teacher_all |> select(-total_staff)
  }
}

# ---- Pivot to long format (true long) -----------------------------------------
race_cols <- intersect(names(teacher_all), c(
  "african_american",
  "american_indian_or_alaska_native",
  "asian",
  "filipino",
  "hispanic_or_latino",
  "pacific_islander",
  "white",
  "two_or_more_races",
  "not_reported"
))

if (!length(race_cols)) {
  warning("[01c] No race columns found to pivot. Writing the summarised wide table as-is.")
  teacher_long <- teacher_all
} else {
  teacher_long <- teacher_all |>
    # Drop redundant if still present
    select(-dplyr::any_of("total_staff")) |>
    pivot_longer(
      cols = all_of(race_cols),
      names_to = "race_ethnicity",
      values_to = "staff_count"
    ) |>
    # Optional: remove zeros to reduce size
    filter(!is.na(staff_count) & staff_count > 0)
}

# ---- Backfill totals for ALL gender rows from race components ------------------

# If some years/schools don't provide total_staff_count for ALL,
# compute it from the component race columns.
if (length(race_cols) && "total_staff_count" %in% names(teacher_all)) {
  teacher_all <- teacher_all |>
    mutate(
      calc_total_from_race = rowSums(across(all_of(race_cols)), na.rm = TRUE),
      total_staff_count = ifelse(
        staff_gender_code == "ALL" & (is.na(total_staff_count) | total_staff_count == 0),
        calc_total_from_race,
        total_staff_count
      )
    ) |>
    select(-calc_total_from_race)
}

# Optional: sanity check gender codes
valid_gender <- c("ALL", "GF", "GM", "GX", "GZ")
gn_extra <- setdiff(unique(teacher_all$staff_gender_code), valid_gender)
if (length(gn_extra)) {
  warning("[01c] Unexpected staff_gender_code values: ", paste(gn_extra, collapse = ", "))
}
# ---- Write staged Parquet -----------------------------------------------------

dir.create(dirname(OUT_PARQUET), recursive = TRUE, showWarnings = FALSE)
write_parquet(teacher_long, OUT_PARQUET)

message("[01c] Wrote ", OUT_PARQUET, " (rows: ", nrow(teacher_long), ")")

race_map <- c(
  african_american                 = "Black/African American",
  american_indian_or_alaska_native = "American Indian/Alaska Native",
  asian                            = "Asian",
  filipino                         = "Filipino",
  hispanic_or_latino               = "Hispanic/Latino",
  pacific_islander                 = "Native Hawaiian/Pacific Islander",
  white                            = "White",
  two_or_more_races                = "Two or More Races",
  not_reported                     = "Not Reported"
)

teacher_long <- teacher_long |>
  mutate(
    race_ethnicity = dplyr::recode(race_ethnicity, !!!race_map),
    race_ethnicity = factor(
      race_ethnicity,
      levels = c(
        "American Indian/Alaska Native","Asian","Filipino",
        "Hispanic/Latino","Native Hawaiian/Pacific Islander",
        "Black/African American","White","Two or More Races","Not Reported"
      )
    )
  )

# ---- Summary sanity check -----------------------------------------------------
# small sanity table
summary_counts <- teacher_long |>
  count(academic_year, name = "rows") |>
  arrange(academic_year)
print(summary_counts)

invisible(TRUE)

# ---- Inspect parsing problems -------------------------------------------------

# Examine first file's issues
problems(teacher_list[[1]]) %>%
  count(expected, actual) %>%
  print(n = 20)

# Check if specific columns affected
problems(teacher_list[[1]]) %>%
  count(col) %>%
  arrange(desc(n))
# ---- Additional data quality checks -------------------------------------------
# 1. Verify no duplicates on long keys
teacher_long %>%
  group_by(academic_year, cds_school, staff_gender_code, 
           race_ethnicity, charter_yn) %>%
  filter(n() > 1)  # Should be empty

# 2. Check totals reconcile with wide format
teacher_long %>%
  filter(staff_gender_code == "ALL") %>%
  group_by(academic_year, cds_school) %>%
  summarise(
    calc_total = sum(staff_count, na.rm = TRUE),
    file_total = first(total_staff_count),
    diff = abs(calc_total - file_total),
    .groups = "drop"
  ) %>%
  filter(diff > 1)  # Should be empty/minimal

# 3. Verify race/ethnicity factor levels
teacher_long %>% 
  count(race_ethnicity, .drop = FALSE) %>%
  print(n = 20)

# 4. Check source_file preservation
teacher_long %>%
  count(source_file) %>%
  print(n = 10)
# ---- Investigate total_staff_count discrepancies ------------------------------
# 1. Check if problem exists in other years
teacher_long %>%
  filter(staff_gender_code == "ALL") %>%
  group_by(academic_year, cds_school) %>%
  summarise(
    calc_total = sum(staff_count, na.rm = TRUE),
    file_total = first(total_staff_count),
    diff = abs(calc_total - file_total),
    .groups = "drop"
  ) %>%
  group_by(academic_year) %>%
  summarise(
    n_schools = n(),
    n_mismatch = sum(diff > 1),
    pct_mismatch = 100 * n_mismatch / n_schools
  )

# 2. Examine raw 2024-25 data
teacher_list[[6]] %>%
  select(academic_year, staff_gender_code, total_staff_count, 
         any_of(c("total_staff", "african_american", "asian", "hispanic_or_latino"))) %>%
  filter(staff_gender_code == "ALL") %>%
  head(20)

# 3. Check column names in stre2425.txt
names(teacher_list[[6]])

# ---- Fix total_staff_count where zero
# 1. Determine scope of total_staff_count problem
teacher_all %>%
  group_by(academic_year) %>%
  summarise(
    n_rows = n(),
    n_zero = sum(total_staff_count == 0, na.rm = TRUE),
    n_na = sum(is.na(total_staff_count)),
    pct_zero = 100 * n_zero / n_rows
  )

# 2. If total_staff_count unreliable, calculate from race sums
teacher_long <- teacher_long %>%
  group_by(academic_year, cds_school, staff_gender_code) %>%
  mutate(
    calculated_total = sum(staff_count, na.rm = TRUE),
    # Use file total if present, otherwise use calculated
    total_staff_count = if_else(
      total_staff_count == 0 & calculated_total > 0,
      calculated_total,
      total_staff_count
    )
  ) %>%
  ungroup()