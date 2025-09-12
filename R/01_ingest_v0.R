# R/01_ingest_v0.R  — minimal update (stable paths + clear order)

# Core libs (quiet)
suppressPackageStartupMessages({
  library(readxl)
  library(readr)
  library(dplyr)
  library(stringr)
  library(janitor)
  library(arrow)
  library(here)
})

# Use your existing path config (provides `raw_path`)
source("R/00_paths.R")
source("R/ingest_helpers.R")

message(">>> Running from project root: ", here::here())
message(">>> Using raw Excel: ", raw_path)

# -------------------------
# 1) Read + clean
# -------------------------
raw0 <- readxl::read_excel(raw_path)

raw <- raw0 %>%
  janitor::clean_names()

year_info <- derive_year(raw)
yr <- year_info$year
academic_year <- year_info$academic_year
num_cols <- numeric_cols(raw)

# -------------------------
# 2) Column dictionary
# -------------------------
ev <- function(x){
  x <- as.character(x)
  y <- x[which(!is.na(x) & nzchar(x))]
  if (length(y)) y[1] else NA_character_
}

dict <- tibble::tibble(
  original_name = names(raw0),
  clean_name    = names(raw),
  example_value = vapply(raw, ev, FUN.VALUE = character(1))
)

# -------------------------
# 3) Numeric-like columns + suppression flags (pre-conversion)
# -------------------------
sup_flags <- dplyr::transmute(
  raw,
  dplyr::across(dplyr::all_of(num_cols), ~ .x == "*", .names = "sup_{.col}")
)

# -------------------------
# 4) Canonical v0 (no drops)
# -------------------------
v0 <- raw %>%
  mutate(
    year = yr,
    academic_year = academic_year,
    charter_yn      = stringr::str_trim(charter_yn),
    charter_is_all  = charter_yn == "All",
    charter_yn_std  = dplyr::if_else(charter_yn %in% c("Yes","No"), charter_yn, NA_character_),
    reporting_category_description = stringr::str_squish(reporting_category_description)
  ) %>%
  mutate(
    dplyr::across(
      dplyr::all_of(num_cols),
      ~ readr::parse_number(dplyr::na_if(.x, "*"))
    )
  ) %>%
  dplyr::bind_cols(sup_flags) %>%
  mutate(suppressed_any = dplyr::if_any(dplyr::starts_with("sup_"), identity))

message("# rows: ", nrow(v0), " | # cols: ", ncol(v0))
print(dplyr::count(v0, academic_year))
print(dplyr::count(v0, charter_yn, sort = TRUE))

# -------------------------
# 5) Write staged outputs
# -------------------------
dir.create(here::here("data-stage"), showWarnings = FALSE)

arrow::write_parquet(v0,  here::here("data-stage", "susp_v0.parquet"))
readr::write_csv(dict,   here::here("data-stage", "column_dictionary_v0.csv"))

message(">>> 01_ingest_v0: wrote data-stage/susp_v0.parquet and column_dictionary_v0.csv")
