# R/01b_ingest_demographics.R
# Read the OTH demographic XLSX (school-level), standardize, and write Parquet.

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(readxl)
  library(janitor)
  library(stringr)
  library(arrow)
  library(here)
})

source("R/ingest_helpers.R")
source("R/demographic_labels.R")

# -------- Config -------------------------------------------------------------
# Path to the raw demographics XLSX. Use an environment variable override
# if available, otherwise fall back to the copy stored under data-raw/.
DEMO_DATA_PATH <- Sys.getenv("OTH_RAW_PATH")
if (!nzchar(DEMO_DATA_PATH)) {
  DEMO_DATA_PATH <- here("data-raw", "copy_CDE_suspensions_1718-2324_sc_oth.xlsx")
}

OUT_PARQUET <- here("data-stage", "oth_long.parquet")
MIN_ENROLLMENT_THRESHOLD <- 10

# ---- Helpers ----------------------------------------------------------------
safe_rate <- function(susp, enroll, min_enroll = 0) ifelse(enroll > min_enroll, susp / enroll, NA_real_)
norm <- function(x) x %>%
  stringr::str_replace_all("\u2013|\u2014", "-") %>%
  stringr::str_squish()

# -------- Read XLSX ----------------------------------------------------------
if (!file.exists(DEMO_DATA_PATH)) stop("File not found: ", DEMO_DATA_PATH)

sheets <- readxl::excel_sheets(DEMO_DATA_PATH)
school_sheet <- sheets[grepl("consldt_school|school", tolower(sheets))]
if (!length(school_sheet)) stop("No school-level sheet. Found: ", paste(sheets, collapse=", "))
school_sheet <- school_sheet[1]
message("Using sheet: ", school_sheet)

raw <- readxl::read_excel(
  DEMO_DATA_PATH, sheet = school_sheet, na = c("", "NA", "N/A", "—", "-", "--")
) |> janitor::clean_names()

num_cols <- numeric_cols(raw)

# Identify key columns once
rc_col  <- pick_col(raw, c("reporting_category","reporting_category_code"), TRUE,  "reporting category")
rcd_col <- pick_col(raw, c("reporting_category_description","reporting_category_desc","reporting_category_descrip"), TRUE, "reporting category description")
year_info <- derive_year(raw)
yr <- year_info$year
academic_year <- year_info$academic_year

# -------- Longify + normalize + promote aliases ------------------------------
# ---- after `raw` is read and clean_names() applied ----

raw_norm <- raw |>
  mutate(
    year = yr,
    academic_year = academic_year,
    across(any_of(c("county_code","district_code","school_code",
                    "county_name","district_name","school_name")), as.character),
    across(any_of(num_cols), ~ readr::parse_number(as.character(.x))),
    rc_code = norm(.data[[rc_col]]),
    rc_desc = norm(.data[[rcd_col]])
  )


# Apply mapping
mapped <- raw_norm |>
  canonicalize_demo(desc_col = "rc_desc", code_col = "rc_code")

# Final tidy + collapse any duplicates created by recode
oth_long <- mapped |>
  rename(unduplicated_suspensions = unduplicated_count_of_students_suspended_total) |>
  mutate(rate = safe_rate(total_suspensions, cumulative_enrollment, MIN_ENROLLMENT_THRESHOLD)) |>
  select(
    academic_year, year,
    county_code, district_code, school_code,
    county_name, district_name, school_name,
    category_type, subgroup, subgroup_code,
    cumulative_enrollment, total_suspensions, unduplicated_suspensions, rate
  ) |>
  filter(!is.na(academic_year), !is.na(subgroup_code)) |>
  group_by(academic_year, county_code, district_code, school_code,
           category_type, subgroup, subgroup_code) |>
  summarise(
    cumulative_enrollment = sum(cumulative_enrollment, na.rm = TRUE),
    total_suspensions     = sum(total_suspensions,     na.rm = TRUE),
    unduplicated_suspensions = sum(unduplicated_suspensions, na.rm = TRUE),
    rate = safe_rate(total_suspensions, cumulative_enrollment, MIN_ENROLLMENT_THRESHOLD),
    .groups = "drop"
  )


# -------- Write --------------------------------------------------------------
dir.create(dirname(OUT_PARQUET), recursive = TRUE, showWarnings = FALSE)
arrow::write_parquet(oth_long, OUT_PARQUET)
message("Wrote: ", OUT_PARQUET, " (rows: ", nrow(oth_long), ")")

# -------- Sanity print (quick peek in console) -------------------------------
cat("\n[01b] Category types loaded:\n")
print(oth_long |> count(category_type) |> arrange(desc(n)))
cat("\n[01b] Sample subgroups per category (first 5):\n")
print(oth_long |> group_by(category_type) |> summarise(subgroups = paste(head(unique(subgroup),5), collapse=", ")))
