# R/01c_ingest_teacher_demographics.R
# Ingest teacher demographic TXT extracts, standardize, and stage as Parquet.
#
# === CDE DATA SOURCE DOCUMENTATION ===
# Data Source: California Department of Education (CDE) Teacher Staff Demographics
# File Pattern: stre{YYZZ}.txt (e.g., stre1920.txt for 2019-20 academic year)
# Official Field Definitions: teacher_dem_definitions.xlsx
#
# KEY DIMENSIONS CAPTURED:
# - Academic Year: 2019-20 through 2024-25
# - Aggregate Level: S (School-level only)
#   * T = State, C = County, D = District, S = School
#   * Script filters to school-level (S) only
# - Charter Status: Yes/No (excludes aggregate "ALL")
#   * CDE rule: "Charter = ALL is not applicable for Aggregate Level = S"
#   * Script removes rows with charter="ALL" (district/county/state aggregates)
# - Staff Type: ALL/ADM/PSV/TCH/OTH (via reporting_category field)
#   * ALL = All Staff (aggregate across all staff types)
#   * ADM = Administrators (principals, assistant principals, district administrators)
#   * PSV = Pupil Services Staff (counselors, psychologists, social workers, nurses)
#   * TCH = Teachers (classroom teachers, instructional staff)
#   * OTH = Other Non-Instructional Support Staff (clerical, custodial, etc.)
#   * SOURCE: Raw data column "Staff Type" → renamed to "reporting_category"
#   * CRITICAL DIMENSION: Essential for equity analysis
#     - Teacher demographics affect daily student-staff interactions
#     - Administrator demographics signal leadership representation
#     - Different policy implications for recruitment and retention
#   * ANALYTICAL USES:
#     - "What % of teachers are Black at high-Black-enrollment schools?"
#     - "What % of administrators are Black at the same schools?"
#     - "Are Black students more likely to have Black teachers or Black admins?"
#     - "How does racial diversity differ between teachers and administrators?"
# - School Grade Span: GS_K6/GS_69/GS_912/GS_K12 (excludes aggregate "ALL")
#   * GS_K6 = Grade K–6 schools
#   * GS_69 = Grade 6–9 schools (middle schools)
#   * GS_912 = Grade 9–12 schools (high schools)
#   * GS_K12 = Grade K–12 Other schools
#   * CDE rule: "ALL" is valid only for aggregate-level, not school-level
# - Staff Gender: ALL/GF/GM/GX (GZ=Missing not present in data)
#   * ALL = All Staff
#   * GF = Female
#   * GM = Male
#   * GX = Non-Binary
#   * GZ = Missing (code exists but no records in data)
# - Race/Ethnicity: 9 categories per CDE standard
#   * African American
#   * American Indian or Alaska Native
#   * Asian
#   * Filipino
#   * Hispanic or Latino
#   * Native Hawaiian/Pacific Islander
#   * White
#   * Two or More Races
#   * Not Reported
#
# CDE COMPLIANCE VALIDATION:
# - Charter "ALL" values removed at school-level ✓
# - Grade span "ALL" values set to NA at school-level ✓
# - All race/ethnicity labels match CDE exact terminology ✓
# - Staff gender codes validated against CDE definitions ✓
# - Staff type codes validated against CDE definitions ✓
# - Invalid grade spans logged and set to NA ✓
# =========================================

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
  library(scales)  # for comma formatting
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

# Note: double-escape \d and \.
files <- list.files(TEACHER_RAW_DIR, pattern = "^stre\\d{4}\\.txt$", full.names = TRUE)
if (!length(files)) {
  stop("No stre*.txt teacher files located under ", TEACHER_RAW_DIR)
}
message("[01c] Found ", length(files), " teacher TXT files.")

# Utility: rename first matching column
rename_first <- function(df, new_name, candidates) {
  hit <- pick_col(df, c(new_name, candidates), required = FALSE)
  if (!is.na(hit) && hit != new_name) {
    df <- dplyr::rename(df, !!new_name := !!rlang::sym(hit))
  }
  df
}

# Derive academic year from file name
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

# Allowed CDE grade spans
ALLOWED_GRADE_SPANS <- c("ALL","GS_K6","GS_69","GS_912","GS_K12")

# Read and clean a single file
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
  )

  # *** Check problems IMMEDIATELY after reading, before transformations ***
  parsing_problems <- NULL
  if (inherits(raw, "spec_tbl_df")) {
    pb <- tryCatch(problems(raw), error = function(e) NULL)
    if (!is.null(pb) && nrow(pb) > 0) {
      message("    Parsing issues: ", nrow(pb), " problems detected")
      message(paste(capture.output(print(pb, n = 10)), collapse = "\n"))
      # Store problems for later logging
      parsing_problems <- pb |>
        mutate(source_file = basename(path)) |>
        select(source_file, everything())
    }
  }

  # Store parsing problems as attribute
  attr(raw, "parsing_problems") <- parsing_problems

  # Now apply transformations
  raw <- raw |> janitor::clean_names()

  # Keep provenance
  raw <- raw |> mutate(source_file = basename(path))

  # Normalize and drop leaked header rows from staff_gender_code
  if ("staff_gender_code" %in% names(raw)) {
    raw <- raw |>
      mutate(
        staff_gender_code = stringr::str_to_upper(stringr::str_squish(staff_gender_code))
      ) |>
      filter(!staff_gender_code %in% c(
        "ACADEMIC YEAR","DISTRICT CODE","COUNTY CODE","SCHOOL CODE","STAFF_GENDER_CODE"
      ))
  }

  # Derive year fields
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

  # Canonicalize column names
  raw <- raw |>
    rename_first("county_code", c("county_cd","cnty_cd","countyid")) |>
    rename_first("district_code", c("district_cd","districtid","dist_cd")) |>
    rename_first("school_code", c("school_cd","schoolid","sch_cd")) |>
    rename_first("county_name", c("county","county_nm")) |>
    rename_first("district_name", c("district","district_nm")) |>
    rename_first("school_name", c("school","school_nm")) |>
    # CRITICAL: Staff Type dimension for teacher vs. administrator analysis
    rename_first("reporting_category", c("staff_type","teacher_reporting_category","report_cat")) |>
    # Note: No description field exists in raw data for staff_type
    rename_first("charter_yn", c("charter_school","charter")) |>
    rename_first("staff_gender_code", c("staff_gender","gender_code")) |>
    rename_first("race_ethnicity", c("teacher_race_ethnicity","ethnicity","race")) |>
    rename_first("school_grade_span", c("grade_span","grade_span_code","school_grade_span_code"))

  # === DIAGNOSTIC: Verify staff_type was renamed to reporting_category ===
  if (basename(path) == "stre1920.txt") {
    if ("reporting_category" %in% names(raw)) {
      message("    ✓ staff_type successfully renamed to reporting_category")
      message("      Values: ", paste(sort(unique(raw$reporting_category)), collapse = ", "))
    } else {
      stop("    ✗ FAILED to rename staff_type to reporting_category")
    }
  }

  raw <- raw |> mutate(
    across(any_of(c(
      "county_code","district_code","school_code","aggregate_level",
      "charter_yn","reporting_category","reporting_category_description",
      "race_ethnicity","staff_gender_code","district_name",
      "school_name","county_name","academic_year","school_grade_span"
    )), ~ stringr::str_squish(as.character(.x)))
  )

  # Normalize and filter invalid values in reporting_category (Staff Type)
  # NOTE: Raw data contains header leaks (e.g., "Aggregate Level") and
  #       data entry errors (e.g., numeric codes like "122", "284")
  if ("reporting_category" %in% names(raw)) {
    n_before_staff_filter <- nrow(raw)

    # Normalize to upper case
    raw <- raw |>
      mutate(
        reporting_category = stringr::str_to_upper(stringr::str_squish(reporting_category))
      )

    # Identify invalid values BEFORE filtering (for diagnostic logging)
    invalid_staff_types <- raw |>
      filter(!is.na(reporting_category)) |>
      filter(!reporting_category %in% c("ALL", "ADM", "PSV", "TCH", "OTH")) |>
      count(reporting_category, name = "n_invalid")

    if (nrow(invalid_staff_types) > 0) {
      message("    Invalid reporting_category values found in ", basename(path), ":")
      message("      ", paste(paste0(invalid_staff_types$reporting_category,
                                     " (", invalid_staff_types$n_invalid, ")"),
                             collapse = ", "))
    }

    # Filter to keep only valid CDE staff type codes
    raw <- raw |>
      filter(is.na(reporting_category) | reporting_category %in% c("ALL", "ADM", "PSV", "TCH", "OTH"))

    n_dropped_staff <- n_before_staff_filter - nrow(raw)
    if (n_dropped_staff > 0) {
      message("    Dropped ", n_dropped_staff, " rows with invalid reporting_category")
    }
  }

  # Normalize charter; log dropped rows with diagnostic detail
  if (!"charter_yn" %in% names(raw)) raw$charter_yn <- NA_character_
  n_before <- nrow(raw)
  raw <- raw |> mutate(
    charter_yn = stringr::str_to_lower(charter_yn),
    charter_yn = case_when(
      charter_yn %in% c("yes","y") ~ "Yes",
      charter_yn %in% c("no","n")  ~ "No",
      charter_yn %in% c("all","a") ~ NA_character_,
      TRUE ~ charter_yn
    )
  )
  if (!all(is.na(raw$charter_yn))) {
    # Diagnose what we're dropping BEFORE filtering
    missing_charter <- raw |> filter(is.na(charter_yn) | !charter_yn %in% c("Yes","No"))
    if (nrow(missing_charter) > 0) {
      message("    ", nrow(missing_charter), " rows have missing/invalid charter_yn")
      if ("aggregate_level" %in% names(missing_charter)) {
        agg_summary <- table(missing_charter$aggregate_level, useNA = "ifany")
        message("      Aggregate levels: ", paste(names(agg_summary), "=", agg_summary, collapse = ", "))
      }
    }

    # Now filter
    raw <- raw |> filter(charter_yn %in% c("Yes","No"))
    n_dropped <- n_before - nrow(raw)
    if (n_dropped > 0) {
      message("    Dropped ", n_dropped, " rows with missing charter_yn in ", basename(path))
    }
  }

  # Normalize aggregate level
  raw <- raw |> rename_first("aggregate_level", c("aggregation_level","aggregation_type","agg_level"))
  if (!"aggregate_level" %in% names(raw)) raw$aggregate_level <- NA_character_
  raw <- raw |> mutate(
    aggregate_level = stringr::str_squish(stringr::str_to_upper(aggregate_level)),
    aggregate_level = case_when(
      aggregate_level %in% c("S","SCH","SCHOOL","SCHOOL LEVEL","SCHOOL-LEVEL") ~ "S",
      TRUE ~ aggregate_level
    )
  )

  # Staff gender label
  raw <- raw |> mutate(
    staff_gender_code = stringr::str_to_upper(coalesce(staff_gender_code, "")),
    staff_gender = teacher_gender_label(staff_gender_code)
  )

  # Validate school grade span
  if (!"school_grade_span" %in% names(raw)) raw$school_grade_span <- NA_character_

  # Log invalid grade spans BEFORE cleaning
  raw <- raw |> mutate(
    school_grade_span = stringr::str_to_upper(stringr::str_squish(school_grade_span)),
    school_grade_span = ifelse(nzchar(school_grade_span), school_grade_span, NA_character_)
  )
  invalid_spans <- raw |>
    filter(!is.na(school_grade_span) &
           !(school_grade_span %in% ALLOWED_GRADE_SPANS)) |>
    count(school_grade_span, name = "n_invalid")

  if (nrow(invalid_spans) > 0) {
    message("    Invalid school_grade_span values found in ", basename(path), ":")
    message("      ", paste(paste0(invalid_spans$school_grade_span, " (", invalid_spans$n_invalid, ")"), collapse = ", "))
  }

  # Now clean invalid values
  raw <- raw |> mutate(
    school_grade_span = ifelse(!(school_grade_span %in% ALLOWED_GRADE_SPANS), NA_character_, school_grade_span),
    school_grade_span = ifelse(aggregate_level == "S" & school_grade_span == "ALL", NA_character_, school_grade_span)
  )

  # Parse numeric columns (excluding IDs/text)
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
      across(all_of(numeric_cols), ~ suppressWarnings(parse_number(as.character(.x))))
    )
  }

  raw <- raw |> mutate(
    year = suppressWarnings(as.integer(year)),
    source_file = as.character(source_file)
  )

  raw
}

# Read all files (problems already logged in read_teacher_txt)
teacher_list <- purrr::map(files, read_teacher_txt)

# === DIAGNOSTIC: Verify reporting_category capture across all files ===
message("[01c] Verifying reporting_category (Staff Type) capture across all files:")
all_have_col <- TRUE
for (i in seq_along(teacher_list)) {
  df <- teacher_list[[i]]
  src <- unique(df$source_file)[1]

  if ("reporting_category" %in% names(df)) {
    unique_vals <- sort(unique(df$reporting_category))
    message("  ✓ ", src, " (values: ", paste(unique_vals, collapse = ", "), ")")
  } else {
    message("  ✗ ", src, " - MISSING reporting_category")
    all_have_col <- FALSE
  }
}

if (!all_have_col) {
  stop("[01c] CRITICAL: reporting_category missing from one or more files")
}
message("[01c] All files contain reporting_category ✓\n")

# === AUDIT TRAIL 1: Collect parsing issues log ===
parsing_log <- purrr::map_df(teacher_list, function(df) {
  pb <- attr(df, "parsing_problems")
  if (!is.null(pb) && nrow(pb) > 0) {
    pb |>
      group_by(source_file) |>
      summarise(
        n_problems = n(),
        problem_rows = paste(head(row, 5), collapse = ", "),
        .groups = "drop"
      )
  } else {
    tibble(
      source_file = unique(df$source_file)[1],
      n_problems = 0L,
      problem_rows = ""
    )
  }
})

# Track initial row counts for lineage
n_raw_total <- sum(purrr::map_int(teacher_list, nrow))

teacher_all <- purrr::list_rbind(teacher_list)
if (!nrow(teacher_all)) stop("Teacher TXT files yielded no rows.")

# Check column survived list_rbind
stopifnot("reporting_category must survive list_rbind" =
          "reporting_category" %in% names(teacher_all))

teacher_all <- teacher_all |> mutate(
  academic_year = stringr::str_squish(as.character(academic_year)),
  aggregate_level = coalesce(aggregate_level, "S")
) |> build_keys()

# === LINEAGE TRACKING ===
n_after_bind <- nrow(teacher_all)

# Keep only campus-level rows
teacher_all <- teacher_all |> filter_campus_only()
n_after_campus_filter <- nrow(teacher_all)

# Check column survived campus filter
stopifnot("reporting_category must survive campus filter" =
          "reporting_category" %in% names(teacher_all))
message("[01c] reporting_category survived campus filtering ✓")

# Determine value columns for aggregation
value_cols <- teacher_value_columns(teacher_all)

key_cols <- intersect(names(teacher_all), c(
  "academic_year","year","county_code","district_code","school_code",
  "cds_school","aggregate_level","charter_yn","reporting_category",
  "reporting_category_description","race_ethnicity","staff_gender_code",
  "staff_gender","school_grade_span"
))

# Aggregate and keep provenance
teacher_all <- teacher_all |>
  group_by(across(all_of(key_cols))) |>
  summarise(
    across(all_of(value_cols), ~ sum(.x, na.rm = TRUE)),
    source_file = paste(unique(source_file), collapse = ";"),
    .groups = "drop"
  ) |>
  arrange(across(any_of(c(
    "academic_year","cds_school","reporting_category",
    "staff_gender_code","race_ethnicity"
  ))))

n_after_aggregation <- nrow(teacher_all)

# Check column survived aggregation
stopifnot("reporting_category must survive aggregation" =
          "reporting_category" %in% names(teacher_all))
message("[01c] reporting_category survived aggregation ✓")

# Fill gender labels
teacher_all <- teacher_all |> mutate(
  staff_gender = teacher_gender_label(staff_gender_code, fallback = staff_gender)
)

# Backfill total_staff_count on ALL rows; count how many rows affected
race_cols <- intersect(names(teacher_all), c(
  "african_american","american_indian_or_alaska_native","asian","filipino",
  "hispanic_or_latino","pacific_islander","white","two_or_more_races","not_reported"
))
backfilled_n <- 0L
if (length(race_cols) && "total_staff_count" %in% names(teacher_all)) {
  teacher_all <- teacher_all |> mutate(
    calc_total_from_race = rowSums(across(all_of(race_cols)), na.rm = TRUE),
    backfilled = staff_gender_code == "ALL" &
                 (is.na(total_staff_count) | total_staff_count == 0),
    total_staff_count = ifelse(backfilled, calc_total_from_race, total_staff_count)
  )
  backfilled_n <- sum(teacher_all$backfilled, na.rm = TRUE)
  message("[01c] Backfilled total_staff_count for ", backfilled_n, " ALL gender rows")

  # Verify backfill correctness: check if race columns sum to total_staff_count
  validation_check <- teacher_all |>
    filter(staff_gender_code == "ALL") |>
    mutate(
      sum_check = rowSums(across(all_of(race_cols)), na.rm = TRUE),
      diff = abs(total_staff_count - sum_check)
    ) |>
    filter(diff > 1)  # Allow 1 unit difference for rounding

  if (nrow(validation_check) > 0) {
    message("[01c] WARNING: ", nrow(validation_check),
            " ALL gender rows have race sum != total_staff_count (diff > 1)")
    message("    Sample mismatches:")
    validation_check |>
      select(cds_school, academic_year, total_staff_count, sum_check, diff) |>
      head(5) |>
      print()
  } else {
    message("[01c] Backfill validation passed: race columns sum correctly")
  }

  teacher_all <- teacher_all |> select(-calc_total_from_race, -backfilled)
}

# === Validate and clean Staff Type dimension ===
message("[01c] Validating Staff Type (reporting_category) values:")

# Safety filter: Remove any invalid staff type codes that slipped through
valid_staff_types <- c("ALL", "ADM", "PSV", "TCH", "OTH")
n_before_final_filter <- nrow(teacher_all)
teacher_all <- teacher_all |>
  filter(is.na(reporting_category) | reporting_category %in% valid_staff_types)

n_dropped_final <- n_before_final_filter - nrow(teacher_all)
if (n_dropped_final > 0) {
  message("  Dropped ", n_dropped_final, " rows with invalid reporting_category in final cleanup")
}

staff_type_dist <- teacher_all |>
  count(reporting_category, sort = TRUE) |>
  mutate(pct = round(100 * n / sum(n), 1))

message("  Distribution after aggregation:")
print(staff_type_dist)

# Verify all remaining values are valid
actual_types <- unique(teacher_all$reporting_category) |> na.omit()
invalid_types <- setdiff(actual_types, valid_staff_types)

if (length(invalid_types) > 0) {
  stop("[01c] CRITICAL: Invalid staff type codes remain after filtering: ",
       paste(invalid_types, collapse = ", "))
}

message("  ✓ All staff type codes are valid CDE codes")

# Check for missing values
n_missing_staff_type <- sum(is.na(teacher_all$reporting_category))
if (n_missing_staff_type > 0) {
  warning("[01c] ", n_missing_staff_type, " rows have missing reporting_category")
} else {
  message("  ✓ No missing values in reporting_category")
}

# Document disaggregation capability
if (length(actual_types) > 1 && "TCH" %in% actual_types && "ADM" %in% actual_types) {
  message("  ✓ Multiple staff types present - can disaggregate teachers vs. administrators")
} else if (length(actual_types) == 1 && actual_types[1] == "ALL") {
  message("  NOTE: Only 'ALL' staff type present - cannot disaggregate by role")
} else {
  message("  WARNING: Unexpected staff type distribution")
}

# Check for duplicates after aggregation
duplicates <- teacher_all |>
  group_by(across(all_of(key_cols))) |>
  filter(n() > 1) |>
  ungroup()
if (nrow(duplicates) > 0) {
  warning("[01c] ", nrow(duplicates), " duplicate rows remain after aggregation")
  print(head(duplicates))
}

# Drop redundant total_staff if duplicated
if (all(c("total_staff","total_staff_count") %in% names(teacher_all))) {
  same <- teacher_all |>
    transmute(eq = (coalesce(total_staff, 0) == coalesce(total_staff_count, 0))) |>
    pull(eq)
  if (all(same, na.rm = TRUE)) {
    teacher_all <- teacher_all |> select(-total_staff)
  }
}

# Pivot race columns to long format
if (!length(race_cols)) {
  warning("[01c] No race columns found to pivot. Writing the summarised wide table as-is.")
  teacher_long <- teacher_all
} else {
  n_before_pivot <- nrow(teacher_all)
  teacher_long <- teacher_all |>
    select(-any_of("total_staff")) |>
    pivot_longer(
      cols = all_of(race_cols),
      names_to = "race_ethnicity",
      values_to = "staff_count"
    )

  # Document zero/NA filtering impact
  n_before_filter <- nrow(teacher_long)
  teacher_long <- teacher_long |>
    filter(!is.na(staff_count) & staff_count > 0)
  n_after_filter <- nrow(teacher_long)

  n_dropped <- n_before_filter - n_after_filter
  pct_dropped <- round(100 * n_dropped / n_before_filter, 1)
  message("[01c] Pivoted ", n_before_pivot, " rows to ", n_before_filter, " long rows")
  message("[01c] Filtered out ", comma(n_dropped),
          " zero/NA staff_count rows (", pct_dropped, "% of pivoted data)")
  message("[01c] Keeping ", comma(n_after_filter), " rows with staff_count > 0")

  # Check column survived pivot
  stopifnot("reporting_category must survive pivot" =
            "reporting_category" %in% names(teacher_long))
  message("[01c] reporting_category present in final dataset ✓")
}

# Map race slugs to readable labels (matching CDE official terminology)
race_map <- c(
  african_american                 = "African American",
  american_indian_or_alaska_native = "American Indian or Alaska Native",
  asian                            = "Asian",
  filipino                         = "Filipino",
  hispanic_or_latino               = "Hispanic or Latino",
  pacific_islander                 = "Native Hawaiian/Pacific Islander",
  white                            = "White",
  two_or_more_races                = "Two or More Races",
  not_reported                     = "Not Reported"
)
teacher_long <- teacher_long |>
  mutate(
    race_ethnicity = recode(race_ethnicity, !!!race_map),
    race_ethnicity = factor(
      race_ethnicity,
      levels = c(
        "American Indian or Alaska Native","Asian","Filipino",
        "Hispanic or Latino","Native Hawaiian/Pacific Islander",
        "African American","White","Two or More Races","Not Reported"
      )
    )
  )

# ---- Distribution and outlier checks ----
message("[01c] Staff count distribution:")
staff_summary <- summary(teacher_long$staff_count)
print(staff_summary)

# Check for extreme outliers (beyond 99th percentile)
q99 <- quantile(teacher_long$staff_count, 0.99, na.rm = TRUE)
outliers <- teacher_long |>
  filter(staff_count > q99)

if (nrow(outliers) > 0) {
  message("[01c] ", nrow(outliers), " rows (", round(100 * nrow(outliers) / nrow(teacher_long), 2),
          "%) have staff_count > 99th percentile (", round(q99, 1), ")")
  message("    Top 5 largest staff counts:")
  teacher_long |>
    arrange(desc(staff_count)) |>
    select(cds_school, academic_year, staff_gender_code, race_ethnicity, staff_count) |>
    head(5) |>
    print()
}

# ---- Summary diagnostics ----
message("[01c] Final dataset summary:")
message("  - Years: ", paste(sort(unique(teacher_long$academic_year)), collapse = ", "))
message("  - Schools: ", n_distinct(teacher_long$cds_school))
message("  - Gender codes: ", paste(sort(unique(teacher_long$staff_gender_code)), collapse = ", "))
message("  - Race categories: ", n_distinct(teacher_long$race_ethnicity))
message("  - Total staff count: ", comma(sum(teacher_long$staff_count, na.rm = TRUE)))

# Verify Staff Type dimension in final dataset
message("[01c] Staff Type (reporting_category) in final dataset:")

if ("reporting_category" %in% names(teacher_long)) {
  # Show distribution
  staff_dist_final <- teacher_long |>
    count(reporting_category, sort = TRUE) |>
    mutate(
      pct = round(100 * n / sum(n), 1),
      n_formatted = comma(n)
    )

  print(staff_dist_final)

  # Cross-tabulation with gender
  message("\n[01c] Staff Type × Gender cross-tabulation:")
  teacher_long |>
    filter(staff_gender_code != "ALL") |>  # Exclude aggregate
    count(reporting_category, staff_gender_code) |>
    tidyr::pivot_wider(
      names_from = staff_gender_code,
      values_from = n,
      values_fill = 0
    ) |>
    print()

  # Sample: Teachers vs. Administrators by race (2024-25)
  if ("2024-25" %in% teacher_long$academic_year) {
    message("\n[01c] Sample: Teachers vs. Administrators by race (2024-25):")
    teacher_long |>
      filter(
        academic_year == "2024-25",
        reporting_category %in% c("TCH", "ADM"),
        staff_gender_code == "ALL"  # Use aggregate for cleaner example
      ) |>
      group_by(reporting_category, race_ethnicity) |>
      summarise(total_staff = sum(staff_count, na.rm = TRUE), .groups = "drop") |>
      tidyr::pivot_wider(
        names_from = reporting_category,
        values_from = total_staff,
        values_fill = 0
      ) |>
      mutate(TCH_to_ADM_ratio = round(TCH / pmax(ADM, 1), 2)) |>
      print(n = 20)
  }

} else {
  stop("[01c] CRITICAL: reporting_category missing from final dataset!")
}
teacher_long |>
  summarise(
    pct_missing_year   = mean(is.na(academic_year)) * 100,
    pct_missing_cds    = mean(is.na(cds_school)) * 100,
    pct_missing_gender = mean(is.na(staff_gender_code)) * 100
  ) |>
  print()

# Final asserts - Basic data quality
stopifnot(
  "No rows in final dataset" = nrow(teacher_long) > 0,
  "Missing academic_year values" = !any(is.na(teacher_long$academic_year)),
  "Missing cds_school values" = !any(is.na(teacher_long$cds_school)),
  "staff_count should be positive" = all(teacher_long$staff_count > 0)
)

# Final asserts - CDE compliance validation
message("[01c] Running CDE compliance validation checks...")

# 1. Verify no "ALL" charter values remain at school level
if ("charter_yn" %in% names(teacher_long) && "aggregate_level" %in% names(teacher_long)) {
  stopifnot(
    "Charter ALL should not exist at school level" =
      !any(teacher_long$aggregate_level == "S" & teacher_long$charter_yn == "ALL", na.rm = TRUE)
  )
  message("  ✓ Charter validation passed: No 'ALL' values at school level")
}

# 2. Verify no "ALL" grade spans at school level
if ("school_grade_span" %in% names(teacher_long)) {
  stopifnot(
    "Grade span ALL should not exist at school level" =
      !any(teacher_long$school_grade_span == "ALL", na.rm = TRUE)
  )
  message("  ✓ Grade span validation passed: No 'ALL' values at school level")
}

# 3. Verify all gender codes are valid CDE codes
if ("staff_gender_code" %in% names(teacher_long)) {
  valid_gender_codes <- c("ALL", "GF", "GM", "GX", "GZ")
  invalid_genders <- teacher_long |>
    filter(!staff_gender_code %in% valid_gender_codes) |>
    distinct(staff_gender_code)

  if (nrow(invalid_genders) > 0) {
    warning("[01c] Invalid gender codes found: ",
            paste(invalid_genders$staff_gender_code, collapse = ", "))
  }

  stopifnot(
    "Only valid CDE gender codes allowed" =
      all(teacher_long$staff_gender_code %in% valid_gender_codes)
  )
  message("  ✓ Gender code validation passed: All codes are valid CDE codes")
}

# 4. Verify race/ethnicity categories match CDE standards
if ("race_ethnicity" %in% names(teacher_long)) {
  valid_race_categories <- c(
    "African American",
    "American Indian or Alaska Native",
    "Asian",
    "Filipino",
    "Hispanic or Latino",
    "Native Hawaiian/Pacific Islander",
    "White",
    "Two or More Races",
    "Not Reported"
  )

  invalid_races <- teacher_long |>
    mutate(race_ethnicity = as.character(race_ethnicity)) |>
    filter(!race_ethnicity %in% valid_race_categories) |>
    distinct(race_ethnicity)

  if (nrow(invalid_races) > 0) {
    warning("[01c] Invalid race/ethnicity categories found: ",
            paste(invalid_races$race_ethnicity, collapse = ", "))
  }

  stopifnot(
    "Only valid CDE race/ethnicity categories allowed" =
      all(as.character(teacher_long$race_ethnicity) %in% valid_race_categories)
  )
  message("  ✓ Race/ethnicity validation passed: All categories match CDE standards")
}

# 5. Verify staff type codes match CDE standards
if ("reporting_category" %in% names(teacher_long)) {
  valid_staff_types <- c("ALL", "ADM", "PSV", "TCH", "OTH")

  # Check for invalid types, excluding NA which is explicitly allowed
  invalid_types <- teacher_long |>
    filter(!is.na(reporting_category) & !reporting_category %in% valid_staff_types) |>
    distinct(reporting_category)

  if (nrow(invalid_types) > 0) {
    stop("[01c] Invalid staff type codes found: ",
         paste(invalid_types$reporting_category, collapse = ", "))
  }

  # Validate that all non-NA values are valid CDE codes
  stopifnot(
    "Only valid CDE staff type codes allowed" =
      all(teacher_long$reporting_category %in% valid_staff_types, na.rm = TRUE)
  )
  message("  ✓ Staff type validation passed: All codes are valid CDE codes")
} else {
  stop("[01c] CRITICAL: reporting_category missing - cannot validate staff types")
}

message("[01c] All CDE compliance validation checks passed ✓")

# === AUDIT TRAIL 2: Create data lineage summary ===
data_lineage <- tibble(
  step = c(
    "1. Raw files loaded",
    "2. After list_rbind",
    "3. After campus filtering",
    "4. After aggregation",
    "5. After pivot to long",
    "6. Final (zeros removed)"
  ),
  n_rows = c(
    n_raw_total,
    n_after_bind,
    n_after_campus_filter,
    n_after_aggregation,
    n_before_filter,
    n_after_filter
  ),
  pct_retained = c(
    100,
    round(100 * n_after_bind / n_raw_total, 1),
    round(100 * n_after_campus_filter / n_raw_total, 1),
    round(100 * n_after_aggregation / n_raw_total, 1),
    round(100 * n_before_filter / n_raw_total, 1),
    round(100 * n_after_filter / n_raw_total, 1)
  )
)

lineage_path <- here("data-stage", "teacher_data_lineage.csv")
write_csv(data_lineage, lineage_path)
message("[01c] Wrote data lineage summary to ", lineage_path)

# === AUDIT TRAIL 3: Flag large schools for verification ===
verification_needed <- teacher_long |>
  filter(staff_count > 1000) |>
  select(cds_school, academic_year, staff_gender_code, race_ethnicity, staff_count) |>
  arrange(desc(staff_count))

if (nrow(verification_needed) > 0) {
  verification_path <- here("data-stage", "teacher_large_schools_to_verify.csv")
  write_csv(verification_needed, verification_path)
  message("[01c] Flagged ", nrow(verification_needed),
          " school-year-category combinations with staff_count > 1000 for verification")
  message("[01c] Wrote verification list to ", verification_path)
} else {
  message("[01c] No schools with staff_count > 1000 found")
}

# === AUDIT TRAIL 4: Write parsing issues log ===
parsing_log_path <- here("data-stage", "teacher_parsing_log.csv")
write_csv(parsing_log, parsing_log_path)
message("[01c] Wrote parsing issues log to ", parsing_log_path)

# Write Parquet
dir.create(dirname(OUT_PARQUET), recursive = TRUE, showWarnings = FALSE)
write_parquet(teacher_long, OUT_PARQUET)
message("[01c] Wrote ", OUT_PARQUET, " (rows: ", nrow(teacher_long), ")")
invisible(TRUE)
