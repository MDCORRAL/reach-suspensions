# R/02_feature_locale_simple.R

# Quiet core libs
suppressPackageStartupMessages({
  library(here)     # project-root paths
  library(arrow)    # parquet I/O
  library(dplyr)    # data wrangling
  library(stringr)  # string helpers
})

source("R/00_paths.R")
source(here::here("R", "utils_keys_filters.R"))

message(">>> Running from project root: ", here::here())

# Load v0 from staged data (root-anchored path)
v0 <- arrow::read_parquet(here::here("data-stage", "susp_v0.parquet"))

# Add simplified locale
v1 <- v0 %>%
  mutate(
    locale_simple = case_when(
      !is.na(school_locale) & str_detect(str_to_lower(school_locale), "city")     ~ "City",
      !is.na(school_locale) & str_detect(str_to_lower(school_locale), "suburban") ~ "Suburban",
      !is.na(school_locale) & str_detect(str_to_lower(school_locale), "rural")    ~ "Rural",
      !is.na(school_locale) & str_detect(str_to_lower(school_locale), "town")     ~ "Town",
      TRUE ~ "Unknown"
    ),
    locale_simple = factor(locale_simple, levels = locale_levels)
  )

# Minimal, quiet diagnostics
message("locale_simple added. Counts:")
print(dplyr::count(v1, locale_simple, sort = TRUE), n = 5)

# Save v1 back to stage (root-anchored path)
arrow::write_parquet(v1, here::here("data-stage", "susp_v1.parquet"))

invisible(TRUE)
