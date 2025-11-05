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

read_teacher_txt <- function(path) {
  message("  - reading ", basename(path))
  raw <- tryCatch(
    readr::read_delim(
      file = path,
      delim = "\t",
      col_types = readr::cols(.default = readr::col_character()),
      na = c("", "NA", "N/A", "NULL", "*"),
      progress = FALSE
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

  raw <- raw |> mutate(source_file = basename(path))

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
    rename_first("race_ethnicity", c("teacher_race_ethnicity", "ethnicity", "race"))

  raw <- raw |> mutate(
    across(any_of(c("county_code", "district_code", "school_code", "aggregate_level",
                    "charter_yn", "reporting_category", "reporting_category_description",
                    "race_ethnicity", "staff_gender_code", "district_name",
                    "school_name", "county_name", "academic_year")),
           ~ stringr::str_squish(as.character(.x)))
  )

  if (!"charter_yn" %in% names(raw)) raw$charter_yn <- NA_character_
  raw <- raw |> mutate(
    charter_yn = stringr::str_to_lower(charter_yn),
    charter_yn = dplyr::case_when(
      charter_yn %in% c("yes", "y") ~ "Yes",
      charter_yn %in% c("no", "n")  ~ "No",
      charter_yn %in% c("all", "a") ~ NA_character_,
      TRUE ~ charter_yn
    )
  )
  if (!all(is.na(raw$charter_yn))) {
    raw <- raw |> filter(charter_yn %in% c("Yes", "No"))
  }

  raw <- raw |> rename_first("aggregate_level", c("aggregation_level", "aggregation_type", "agg_level"))
  if (!"aggregate_level" %in% names(raw)) raw$aggregate_level <- NA_character_
  raw <- raw |> mutate(
    aggregate_level = stringr::str_squish(stringr::str_to_upper(aggregate_level)),
    aggregate_level = dplyr::case_when(
      aggregate_level %in% c("S", "SCH", "SCHOOL", "SCHOOL LEVEL", "SCHOOL-LEVEL") ~ "S",
      TRUE ~ aggregate_level
    )
  )

  raw <- raw |> mutate(
    staff_gender_code = stringr::str_to_upper(dplyr::coalesce(staff_gender_code, "")),
    staff_gender = teacher_gender_label(staff_gender_code)
  )

  numeric_candidates <- names(raw)[vapply(raw, teacher_numeric_like, logical(1))]
  id_cols <- intersect(names(raw), c("county_code", "district_code", "school_code",
                                     "cds_district", "cds_school", "aggregate_level",
                                     "charter_yn", "reporting_category", "staff_gender_code"))
  numeric_cols <- setdiff(numeric_candidates, id_cols)
  if (length(numeric_cols)) {
    raw <- raw |> mutate(
      across(
        all_of(numeric_cols),
        ~ suppressWarnings(readr::parse_number(as.character(.x)))
      )
    )
  }

  raw <- raw |> mutate(year = suppressWarnings(as.integer(year)))

  raw
}

teacher_list <- purrr::map(files, read_teacher_txt)
teacher_all <- purrr::list_rbind(teacher_list)

if (!nrow(teacher_all)) stop("Teacher TXT files yielded no rows.")

teacher_all <- teacher_all |>
  mutate(
    academic_year = stringr::str_squish(as.character(academic_year)),
    aggregate_level = dplyr::coalesce(aggregate_level, "S")
  ) |>
  build_keys()

teacher_all <- teacher_all |> filter_campus_only()

value_cols <- teacher_value_columns(teacher_all)

key_cols <- intersect(names(teacher_all), c(
  "academic_year", "year", "county_code", "district_code", "school_code",
  "cds_school", "aggregate_level", "charter_yn", "reporting_category",
  "reporting_category_description", "race_ethnicity", "staff_gender_code", "staff_gender"
))

teacher_all <- teacher_all |>
  group_by(across(all_of(key_cols))) |>
  summarise(across(all_of(value_cols), ~ sum(.x, na.rm = TRUE)), .groups = "drop") |>
  arrange(dplyr::across(dplyr::any_of(c(
    "academic_year", "cds_school", "reporting_category",
    "staff_gender_code", "race_ethnicity"
  ))))

teacher_all <- teacher_all |> mutate(
  staff_gender = teacher_gender_label(staff_gender_code, fallback = staff_gender)
)

dir.create(dirname(OUT_PARQUET), recursive = TRUE, showWarnings = FALSE)
write_parquet(teacher_all, OUT_PARQUET)

message("[01c] Wrote ", OUT_PARQUET, " (rows: ", nrow(teacher_all), ")")

summary_counts <- teacher_all |> 
  count(academic_year, name = "rows") |> arrange(academic_year)
print(summary_counts)

invisible(TRUE)
