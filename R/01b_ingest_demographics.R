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
# Path to the OTH demographic XLSX. Can be overridden by OTH_RAW_PATH env var.
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

# Pick likely column names (tolerant to header variants)
pick_col <- function(df, candidates, required=TRUE, label=NULL) {
  nm <- intersect(candidates, names(df))[1]
  if (is.na(nm) && required) stop("Missing expected column", if(!is.null(label)) paste0(" for ", label), ": ", paste(candidates, collapse=", "))
  nm
}

# Numeric fields present in CDE files
NUMERIC_COLS <- c(
  "cumulative_enrollment",
  "total_suspensions",
  "unduplicated_count_of_students_suspended_total",
  "suspension_rate_total"
)

# Safely derive year and academic_year from possible columns
derive_year <- function(df) {
  ay_col   <- pick_col(df, c("academic_year","academic_yr"), FALSE)
  year_col <- pick_col(df, c("year"), FALSE)

  yr <- if (!is.na(year_col)) suppressWarnings(as.integer(df[[year_col]])) else NA_integer_
  ay <- if (!is.na(ay_col)) as.character(df[[ay_col]]) else NA_character_

  academic_year <- dplyr::coalesce(
    if (!all(is.na(ay))) ay else NA_character_,
    ifelse(!is.na(yr), paste0(yr - 1, "-", substr(yr,3,4)), NA_character_)
  )

  list(year = yr, academic_year = academic_year)
}

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

# Identify key columns once
rc_col  <- pick_col(raw, c("reporting_category","reporting_category_code"), TRUE,  "reporting category")
rcd_col <- pick_col(raw, c("reporting_category_description","reporting_category_desc","reporting_category_descrip"), TRUE, "reporting category description")
year_info <- derive_year(raw)
yr <- year_info$year
academic_year <- year_info$academic_year

# ---------------- Codebook (authoritative mapping) ---------------------------
# Includes aliases that often show up under "Other"
codebook <- tibble::tribble(
  ~subgroup_code, ~category_type,         ~subgroup,
  
  # Total
  "TA",           "Total",                "All Students",
  
  # Sex / Gender (canonical codes)
  "SF",           "Sex",                  "Female",
  "SM",           "Sex",                  "Male",
  "SNB",          "Sex",                  "Non-Binary",
  # Extra sex values that can show up
  "SGZ",          "Sex",                  "Missing Gender",
  "SNR",          "Sex",                  "Not Reported",
  
  # Aliases frequently found under "Other" that we need to promote
  "GF",           "Sex",                  "Female",
  "GM",           "Sex",                  "Male",
  "GX",           "Sex",                  "Non-Binary Gender (Beginning 2019–20)",
  "GZ",           "Sex",                  "Missing Gender",
  "RD",           "Sex",                  "Not Reported",
  
  # Special Education
  "SE",           "Special Education",    "Students with Disabilities",
  "SN",           "Special Education",    "Non-Students with Disabilities",
  
  # Socioeconomic
  "SD",           "Socioeconomic",        "Socioeconomically Disadvantaged",
  "NS",           "Socioeconomic",        "Not Socioeconomically Disadvantaged",
  # Alias under Other
  "SS",           "Socioeconomic",        "Socioeconomically Disadvantaged",
  
  # Homeless
  "HL",           "Homeless",             "Homeless",
  "NH",           "Homeless",             "Not Homeless",
  # Alias under Other
  "SH",           "Homeless",             "Homeless",
  
  # English Learner
  "EL",           "English Learner",      "English Learner",
  "EO",           "English Learner",      "English Only",
  "IFEP",         "English Learner",      "Initially Fluent English Proficient",
  "RFEP",         "English Learner",      "Reclassified Fluent English Proficient",
  
  # Foster
  "FY",           "Foster",               "Foster Youth",
  "NF",           "Foster",               "Not Foster Youth",
  
  # Migrant
  "MG",           "Migrant",              "Migrant",
  "NM",           "Migrant",              "Non-Migrant"
)

# -------- Longify + normalize + promote aliases ------------------------------
# ---- after `raw` is read and clean_names() applied ----

raw_norm <- raw |>
  mutate(
    year = yr,
    academic_year = academic_year,
    across(any_of(c("county_code","district_code","school_code",
                    "county_name","district_name","school_name")), as.character),
    across(any_of(NUMERIC_COLS), ~ readr::parse_number(as.character(.x))),
    rc_code = norm(.data[[rc_col]]),
    rc_desc = norm(.data[[rcd_col]])
  )

# ---- description-first → canonical (category_type, subgroup, subgroup_code) ----
# We map by DESCRIPTION first (because your workbook uses repurposed codes like SE/SF/SD),
# then fall back to code aliases (GF/GM/GX/GZ, SH, SS, etc.).

desc_to_canon <- function(desc) {
  d <- tolower(desc %||% "")
  dplyr::case_when(
    grepl("\\benglish\\s*learner", d)                                ~ "EL",
    grepl("\\benglish\\s*only", d)                                    ~ "EO",
    grepl("reclassified\\s*fluent", d)                                ~ "RFEP",
    grepl("initially\\s*fluent", d)                                   ~ "IFEP",
    
    grepl("\\bfoster\\b", d)                                          ~ "FY",
    grepl("\\bnot\\s*foster", d)                                      ~ "NF",
    
    grepl("\\bmigrant\\b", d)                                         ~ "MG",
    grepl("\\bnon[- ]?migrant|\\bnot\\s*migrant", d)                  ~ "NM",
    
    grepl("\\bhomeless\\b", d)                                        ~ "HL",
    grepl("\\bnot\\s*homeless", d)                                    ~ "NH",
    
    grepl("students?\\s*with\\s*disab|special\\s*education", d)       ~ "SE",
    grepl("\\bnot\\s*(students?\\s*with\\s*disab|special\\s*education)", d) ~ "SN",
    
    grepl("socioeconomically\\s*disadv", d)                           ~ "SD",
    grepl("\\bnot\\s*socioeconomically\\s*disadv", d)                 ~ "NS",
    
    grepl("\\bfemale\\b", d)                                          ~ "SF",
    grepl("\\bmale\\b", d)                                            ~ "SM",
    grepl("non[- ]?binary", d)                                        ~ "SNB",
    grepl("missing\\s*gender", d)                                     ~ "SGZ",
    grepl("^not\\s*reported$", d)                                     ~ "SNR",
    
    grepl("^all\\s*students?$", d)                                    ~ "TA",
    TRUE ~ NA_character_
  )
}

# Code aliases → canonical (when descriptions are vague)
code_alias_to_canon <- function(code) {
  c <- toupper(code %||% "")
  dplyr::case_when(
    # Sex aliases from "Other"
    c == "GF" ~ "SF",
    c == "GM" ~ "SM",
    c == "GX" ~ "SNB",
    c == "GZ" ~ "SGZ",
    c == "RD" ~ "SNR",
    # Socioeconomic alias
    c == "SS" ~ "SD",
    # Homeless alias
    c == "SH" ~ "HL",
    # Already canonical codes pass through
    grepl("^(SF|SM|SNB|SGZ|SNR|SE|SN|SD|NS|EL|EO|IFEP|RFEP|FY|NF|MG|NM|HL|NH|TA)$", c) ~ c,
    TRUE ~ NA_character_
  )
}

# Human label for canonical code
canon_label <- function(code) dplyr::recode(code,
                                            SF="Female", SM="Male", SNB="Non-Binary", SGZ="Missing Gender", SNR="Not Reported",
                                            SE="Students with Disabilities", SN="Non-Students with Disabilities",
                                            SD="Socioeconomically Disadvantaged", NS="Not Socioeconomically Disadvantaged",
                                            EL="English Learner", EO="English Only", IFEP="Initially Fluent English Proficient", RFEP="Reclassified Fluent English Proficient",
                                            FY="Foster Youth", NF="Not Foster Youth",
                                            MG="Migrant", NM="Non-Migrant",
                                            HL="Homeless", NH="Not Homeless",
                                            TA="All Students",
                                            .default = NA_character_
)

canon_category <- function(code) dplyr::recode(code,
                                               SF="Sex", SM="Sex", SNB="Sex", SGZ="Sex", SNR="Sex",
                                               SE="Special Education", SN="Special Education",
                                               SD="Socioeconomic", NS="Socioeconomic",
                                               EL="English Learner", EO="English Learner", IFEP="English Learner", RFEP="English Learner",
                                               FY="Foster", NF="Foster",
                                               MG="Migrant", NM="Migrant",
                                               HL="Homeless", NH="Homeless",
                                               TA="Total",
                                               .default = "Other"
)

# Apply mapping
mapped <- raw_norm |>
  mutate(
    canon_from_desc = desc_to_canon(rc_desc),
    canon_from_code = code_alias_to_canon(rc_code),
    subgroup_code   = dplyr::coalesce(canon_from_desc, canon_from_code),   # DESCRIPTION wins
    category_type   = canon_category(subgroup_code),
    subgroup        = dplyr::coalesce(canon_label(subgroup_code), rc_desc)
  )

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
