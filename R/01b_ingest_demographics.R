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

# -------- Config -------------------------------------------------------------
# Option A: absolute path (works today)
DEMO_DATA_PATH <- "/Users/michaelcorral/Library/CloudStorage/GoogleDrive-mdcorral@g.ucla.edu/.shortcut-targets-by-id/1qNAOKIg0UjuT3XWFlk4dkDLN6UPWJVGx/Center for the Transformation of Schools/Research/CA Race Education And Community Healing (REACH)/2. REACH Network (INTERNAL)/15. REACH Baseline Report_Summer 2025/6. R Data Analysis Project Folders/reach-suspensions/data-raw/copy_CDE_suspensions_1718-2324_sc_oth.xlsx"

# Option B: if you ever move the XLSX into your repo:
# DEMO_DATA_PATH <- here("data-stage", "copy_CDE_suspensions_1718-2324_sc_oth.xlsx")

OUT_PARQUET <- here("data-stage", "oth_long.parquet")
MIN_ENROLLMENT_THRESHOLD <- 10

# Codes -> human labels (kept for reference; analysis uses subgroup_code)
DEMO_CODES <- c(
  SM="Male", SF="Female",
  SE="Special Education", SN="Not Special Education",
  EL="English Learner", EO="English Only", IFEP="Initially Fluent English Proficient", RFEP="Reclassified Fluent English Proficient",
  MG="Migrant", NM="Not Migrant",
  FY="Foster Youth", NF="Not Foster Youth",
  HL="Homeless", NH="Not Homeless",
  SD="Socioeconomically Disadvantaged", NS="Not Socioeconomically Disadvantaged",
  TA="All Students"
)
demo_label <- function(code) dplyr::recode(code, !!!DEMO_CODES, .default = NA_character_)
safe_rate <- function(susp, enroll, min_enroll = 0) ifelse(enroll > min_enroll, susp / enroll, NA_real_)

# -------- Read XLSX ----------------------------------------------------------
if (!file.exists(DEMO_DATA_PATH)) stop("File not found: ", DEMO_DATA_PATH)

sheets <- readxl::excel_sheets(DEMO_DATA_PATH)
school_sheet <- sheets[grepl("consldt_school", tolower(sheets))]
if (!length(school_sheet)) stop("No school-level sheet. Found: ", paste(sheets, collapse=", "))
school_sheet <- school_sheet[1]
message("Using sheet: ", school_sheet)

raw <- readxl::read_excel(
  DEMO_DATA_PATH, sheet = school_sheet, na = c("", "NA", "N/A", "—", "-", "--")
) |> janitor::clean_names()

# Resolve likely column names
pick_col <- function(df, candidates, required=TRUE, label=NULL) {
  nm <- intersect(candidates, names(df))[1]
  if (is.na(nm) && required) stop("Missing expected column", if(!is.null(label)) paste0(" for ", label), ": ", paste(candidates, collapse=", "))
  nm
}
rc_col   <- pick_col(raw, c("reporting_category","reporting_category_code"), TRUE,  "reporting category")
rcd_col  <- pick_col(raw, c("reporting_category_description","reporting_category_desc"), FALSE)
ay_col   <- pick_col(raw, c("academic_year","academic_yr"), FALSE)
year_col <- pick_col(raw, c("year"), FALSE)

# Derive year safely
yr <- rep(NA_integer_, nrow(raw))
if (!is.na(year_col)) yr <- suppressWarnings(as.integer(raw[[year_col]]))
if (all(is.na(yr)) && !is.na(ay_col)) {
  ay <- as.character(raw[[ay_col]])
  yr <- ifelse(grepl("^\\d{4}-\\d{2}$", ay), as.integer(substr(ay,1,4)) + 1L, NA_integer_)
}

# Numeric fields
num_like <- c("cumulative_enrollment","total_suspensions","unduplicated_count_of_students_suspended_total","suspension_rate_total")

oth_long <- raw |>
  mutate(
    year = yr,
    academic_year = dplyr::coalesce(
      if (!is.na(ay_col)) .data[[ay_col]] else NA_character_,
      ifelse(!is.na(year), paste0(year - 1, "-", substr(year,3,4)), NA_character_)
    ),
    across(any_of(c("county_code","district_code","school_code",
                    "county_name","district_name","school_name")), as.character),
    across(any_of(num_like), ~ readr::parse_number(as.character(.x))),
    subgroup_code  = .data[[rc_col]],
    subgroup_label = demo_label(subgroup_code),
    subgroup       = dplyr::coalesce(subgroup_label,
                                     if (!is.na(rcd_col)) .data[[rcd_col]] else NA_character_,
                                     subgroup_code),
    category_type  = dplyr::case_when(
      subgroup_code %in% c("SM","SF") ~ "Sex",
      subgroup_code %in% c("SE","SN") ~ "Special Education",
      subgroup_code %in% c("EL","EO","IFEP","RFEP") ~ "English Learner",
      subgroup_code %in% c("MG","NM") ~ "Migrant",
      subgroup_code %in% c("FY","NF") ~ "Foster",
      subgroup_code %in% c("HL","NH") ~ "Homeless",
      subgroup_code %in% c("SD","NS") ~ "Socioeconomic",
      subgroup_code %in% c("TA")      ~ "Total",
      TRUE ~ "Other"
    )
  ) |>
  rename(
    unduplicated_suspensions = unduplicated_count_of_students_suspended_total
  ) |>
  mutate(
    # Always compute our own fraction-scale rate (0–1)
    rate = safe_rate(total_suspensions, cumulative_enrollment, MIN_ENROLLMENT_THRESHOLD)
  ) |>
  select(
    academic_year, year,
    county_code, district_code, school_code,
    county_name, district_name, school_name,
    category_type, subgroup, subgroup_code,
    cumulative_enrollment, total_suspensions, unduplicated_suspensions, rate
  ) |>
  filter(!is.na(academic_year), !is.na(subgroup))

dir.create(dirname(OUT_PARQUET), recursive = TRUE, showWarnings = FALSE)
arrow::write_parquet(oth_long, OUT_PARQUET)
message("Wrote: ", OUT_PARQUET, " (rows: ", nrow(oth_long), ")")