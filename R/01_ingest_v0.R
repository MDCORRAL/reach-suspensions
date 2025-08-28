library(readxl); library(dplyr); library(stringr)
library(readr);  library(janitor); library(arrow)
source("R/00_paths.R")

# Read exactly as text
raw0 <- read_excel(raw_path, col_types = "text")
raw  <- janitor::clean_names(raw0)

# Column dictionary (original -> clean)
ev <- function(x){ x <- as.character(x); y <- x[which(!is.na(x) & nzchar(x))]; if (length(y)) y[1] else NA_character_ }
dict <- tibble::tibble(
  original_name = names(raw0),
  clean_name    = names(raw),
  example_value = vapply(raw, ev, FUN.VALUE = character(1))
)

# Identify numeric-like columns by pattern
num_cols <- names(raw)[grepl(
  "^cumulative_enrollment$|^total_suspensions$|^unduplicated_count_of_students_suspended|^suspension_",
  names(raw)
)]

# Per-column suppression flags BEFORE conversion
sup_flags <- dplyr::transmute(raw, dplyr::across(dplyr::all_of(num_cols), ~ .x == "*", .names = "sup_{.col}"))

# Canonical v0 (no drops)
v0 <- raw %>%
  mutate(
    charter_yn      = stringr::str_trim(charter_yn),
    charter_is_all  = charter_yn == "All",
    charter_yn_std  = dplyr::if_else(charter_yn %in% c("Yes","No"), charter_yn, NA_character_),
    reporting_category_description = stringr::str_squish(reporting_category_description)
  ) %>%
  mutate(dplyr::across(dplyr::all_of(num_cols), ~ readr::parse_number(dplyr::na_if(.x, "*")))) %>%
  dplyr::bind_cols(sup_flags) %>%
  mutate(suppressed_any = dplyr::if_any(dplyr::starts_with("sup_"), identity))

message("# rows: ", nrow(v0), " | # cols: ", ncol(v0))
print(dplyr::count(v0, academic_year))
print(dplyr::count(v0, charter_yn, sort = TRUE))

dir.create("data-stage", showWarnings = FALSE)
arrow::write_parquet(v0, "data-stage/susp_v0.parquet")
readr::write_csv(dict, "data-stage/column_dictionary_v0.csv")

