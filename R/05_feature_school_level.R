# R/05_feature_school_level.R
library(dplyr)
library(stringr)
library(arrow)

v2 <- arrow::read_parquet("data-stage/susp_v2.parquet")

# ---- helpers ---------------------------------------------------------------

# Map grade tokens to numeric ranks so we can compute min/max.
# PK/TK/K -> 0, 1..12 -> numeric, everything else -> NA
grade_token_to_num <- function(token) {
  token <- toupper(trimws(token))
  if (token %in% c("PK","PREK","PRE-K","TK","K")) return(0)
  if (str_detect(token, "^[0-9]{1,2}$")) return(as.numeric(token))
  NA_real_
}

# Extract min/max grade numbers from strings like "K-8", "6-8", "9-12", "TK-5", "K-12"
extract_min_max_grade <- function(gs) {
  if (is.na(gs) || gs == "") return(c(NA_real_, NA_real_))
  # Split on non-alphanumeric separators
  parts <- unlist(str_split(gs, "[^A-Za-z0-9]+", simplify = FALSE))
  parts <- parts[parts != ""]
  nums  <- vapply(parts, grade_token_to_num, numeric(1))
  # If it’s a range like "K-8" we’ll get c(0,8); if a set like "6 7 8" we still get min/max
  if (all(is.na(nums))) return(c(NA_real_, NA_real_))
  c(min(nums, na.rm = TRUE), max(nums, na.rm = TRUE))
}

# Vectorized wrappers
get_min_grade <- function(x) vapply(x, function(s) extract_min_max_grade(s)[1], numeric(1))
get_max_grade <- function(x) vapply(x, function(s) extract_min_max_grade(s)[2], numeric(1))

# Classify span
span_label <- function(gmin, gmax) {
  if (is.na(gmin) || is.na(gmax)) return("Other/Unknown")
  if (gmax <= 5) return("Elem-only")
  if (gmin >= 6 && gmax <= 8) return("Mid-only")
  if (gmin >= 9) return("High-only")
  if (gmin <= 0 && gmax == 8) return("K-8")
  if (gmin <= 0 && gmax >= 9) return("K-12")
  if (gmin <= 8 && gmax >= 9) return("7-12")
  "Other/Unknown"
}

# Collapse to strict 3-band using HIGHEST grade offered
strict3 <- function(gmax) {
  if (is.na(gmax)) return("Other/Unknown")
  if (gmax <= 5) return("Elementary")
  if (gmax <= 8) return("Middle")
  "High"
}

# Alternative settings flag (non-destructive; you can filter later)
is_alt <- function(school_type) {
  st <- tolower(school_type %||% "")
  any(str_detect(st, c("juvenile court", "community day", "alternative")))
}

`%||%` <- function(a,b) if (is.null(a)) b else a

# ---- build features --------------------------------------------------------

v3 <- v2 %>%
  mutate(
    grade_min_num = get_min_grade(grades_served),
    grade_max_num = get_max_grade(grades_served),
    level_span    = mapply(span_label, grade_min_num, grade_max_num),
    level_strict3 = vapply(grade_max_num, strict3, character(1)),
    level_override = if_else(is_alt(school_type), "Alternative", NA_character_)
  )

arrow::write_parquet(v3, "data-stage/susp_v3.parquet")

# Sanity checks in console
v3 %>%
  distinct(academic_year, county_code, district_code, school_code, level_strict3) %>%
  count(academic_year, level_strict3) %>%
  arrange(academic_year, level_strict3) %>%
  print(n = 40)

v3 %>%
  distinct(academic_year, county_code, district_code, school_code, level_span) %>%
  count(academic_year, level_span) %>%
  arrange(academic_year, level_span) %>%
  print(n = 60)
