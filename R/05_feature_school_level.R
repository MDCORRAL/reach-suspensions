# R/05_feature_school_level.R  — merged: strict3 + final mapping + Alternative override

# Toggle extra console diagnostics (A/B). Set to FALSE for quiet runs.
SHOW_SANITY <- TRUE

suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
  library(stringr)
  library(tibble)
})
source(here::here("R","utils_keys_filters.R"))
message(">>> Running from project root: ", here::here())

# --- read v3 ---------------------------------------------------------------
v3_in <- arrow::read_parquet(here::here("data-stage","susp_v3.parquet")) %>%
  build_keys() %>%
  filter_campus_only()

stopifnot(!any(stringr::str_detect(v3_in$cds_school, "0000000$|0000001$")))


# --- helpers ---------------------------------------------------------------
grade_token_to_num <- function(token) {
  token <- toupper(trimws(token))
  if (token %in% c("PK","PREK","PRE-K","TK","K")) return(0)
  if (str_detect(token, "^[0-9]{1,2}$")) return(as.numeric(token))
  NA_real_
}

extract_min_max_grade <- function(gs) {
  if (is.na(gs) || gs == "") return(c(NA_real_, NA_real_))
  parts <- unlist(str_split(gs, "[^A-Za-z0-9]+", simplify = FALSE))
  parts <- parts[parts != ""]
  nums  <- vapply(parts, grade_token_to_num, numeric(1))
  if (all(is.na(nums))) return(c(NA_real_, NA_real_))
  c(min(nums, na.rm = TRUE), max(nums, na.rm = TRUE))
}

get_min_grade <- function(x) vapply(x, function(s) extract_min_max_grade(s)[1], numeric(1))
get_max_grade <- function(x) vapply(x, function(s) extract_min_max_grade(s)[2], numeric(1))

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

strict3 <- function(gmax) {
  if (is.na(gmax)) return("Other/Unknown")
  if (gmax <= 5) return("Elementary")
  if (gmax <= 8) return("Middle")
  "High School"
}

is_alt <- function(school_type) {
  st <- tolower(ifelse(is.na(school_type), "", school_type))
  str_detect(st, "juvenile court|community day|alternative|continuation")
}

# --- build features ---------------------------------------------------------
v4 <- v3_in %>%
  mutate(
    grade_min_num = get_min_grade(grades_served),
    grade_max_num = get_max_grade(grades_served),
    
    # strict 3-band label
    level_strict3 = vapply(grade_max_num, strict3, character(1)),
    
    # granular final label
    school_level_final = case_when(
      !is.na(grade_min_num) & !is.na(grade_max_num) &
        grade_min_num <= 0 & grade_max_num == 12 ~ "K-12",
      
      grade_min_num == 6 & grade_max_num == 8 ~ "Middle (6-8)",
      grade_min_num == 7 & grade_max_num == 8 ~ "Middle (6-8)",
      grade_min_num == 5 & grade_max_num == 8 ~ "Middle (6-8)",
      grade_min_num == 4 & grade_max_num == 6 ~ "Middle (6-8)",
      grade_min_num == 8 & grade_max_num == 8 ~ "Middle (6-8)",
      grade_min_num == 6 & grade_max_num == 7 ~ "Middle (6-8)",
      grade_min_num == 5 & grade_max_num == 6 ~ "Middle (6-8)",
      
      grade_min_num <= 0 & grade_max_num == 8 ~ "Elementary",
      grade_min_num <= 0 & grade_max_num == 9 ~ "Elementary",
      grade_min_num <= 0 & grade_max_num <= 8 ~ "Elementary",
      grade_min_num <= 4 & grade_max_num <= 8 ~ "Elementary",
      grade_min_num >= 0 & grade_max_num <= 5 ~ "Elementary",
      
      grade_min_num %in% c(6,7) & grade_max_num == 9  ~ "Middle (6-8)",
      grade_min_num == 6 & grade_max_num == 10        ~ "Middle (6-8)",
      
      grade_min_num >= 9 ~ "High School",
      grade_min_num == 8 & grade_max_num %in% c(9,10,11,12) ~ "High School",
      grade_min_num == 7 & grade_max_num %in% c(10,11,12)   ~ "High School",
      grade_min_num == 5 & grade_max_num == 9               ~ "High School",
      grade_min_num == 6 & grade_max_num %in% c(11,12)      ~ "High School",
      grade_max_num == 12 & !is.na(grade_min_num) & grade_min_num >= 1 ~ "High School",
      
      grade_min_num == 6 & grade_max_num == 6 ~ "Middle (6-8)",
      grade_min_num == 4 & grade_max_num == 8 ~ "Elementary",
      grade_min_num == 3 & grade_max_num == 8 ~ "Elementary",
      grade_min_num == 1 & grade_max_num == 8 ~ "Elementary",
      
      TRUE ~ "Other/Unknown"
    ),
    
    # Alternative override applies to strict3 (optionally to final)
    level_strict3 = if_else(is_alt(school_type), "Alternative", level_strict3)
    # If you also want to force final label to Alternative, uncomment:
    # ,school_level_final = if_else(is_alt(school_type), "Alternative", school_level_final)
    
  )

# row-count must be stable through feature adds
stopifnot(nrow(v4) == nrow(v3_in))

# --- write ------------------------------------------------------------------
arrow::write_parquet(v4, here::here("data-stage", "susp_v4.parquet"))
message(">>> 05_feature_school_level (merged): wrote susp_v4.parquet")

# ---- optional sanity prints (A/B) ------------------------------------------
if (isTRUE(SHOW_SANITY)) {
  # topline counts by strict3
  v4 %>%
    distinct(academic_year, county_code, district_code, school_code, level_strict3) %>%
    count(academic_year, level_strict3) %>%
    arrange(academic_year, level_strict3) %>%
    print(n = 60)
  
  # counts by span (optional context)
  v4 %>%
    distinct(academic_year, county_code, district_code, school_code, level_span = mapply(span_label, grade_min_num, grade_max_num)) %>%
    count(academic_year, level_span) %>%
    arrange(academic_year, level_span) %>%
    print(n = 60)
  
  # A) strict3 vs final mapping
  v4 %>%
    distinct(academic_year, county_code, district_code, school_code,
             level_strict3, school_level_final) %>%
    count(level_strict3, school_level_final) %>%
    arrange(level_strict3, school_level_final) %>%
    print(n = 100)
  
  # B) unit-test several edge grade strings
  tibble(grades_served = c("K-8","7-12","PK-12","6","9-12","TK-5")) %>%
    mutate(
      gmin = get_min_grade(grades_served),
      gmax = get_max_grade(grades_served),
      strict3_lbl = vapply(gmax, strict3, character(1)),
      span_lbl    = mapply(span_label, gmin, gmax)
    ) %>%
    print()
}

invisible(TRUE)
# --- end --------------------------------------------------------------------
