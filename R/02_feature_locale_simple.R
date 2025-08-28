# R/02_feature_locale_simple.R
library(dplyr)
library(stringr)
library(arrow)

v0 <- arrow::read_parquet("data-stage/susp_v0.parquet")

v1 <- v0 %>%
  mutate(
    locale_simple = case_when(
      !is.na(school_locale) & str_detect(str_to_lower(school_locale), "city")     ~ "City",
      !is.na(school_locale) & str_detect(str_to_lower(school_locale), "suburban") ~ "Suburban",
      !is.na(school_locale) & str_detect(str_to_lower(school_locale), "rural")    ~ "Rural",
      !is.na(school_locale) & str_detect(str_to_lower(school_locale), "town")     ~ "Town",
      TRUE ~ "Unknown"
    )
  )

# minimal, quiet diagnostics
message("locale_simple added. Counts:")
print(dplyr::count(v1, locale_simple, sort = TRUE), n = 5)

arrow::write_parquet(v1, "data-stage/susp_v1.parquet")
invisible(TRUE)
