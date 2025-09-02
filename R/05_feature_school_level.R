# R/05_feature_school_level.R  (merged: strict3 + final mapping + Alternative override)

suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
  library(stringr)
})

message(">>> Running from project root: ", here::here())

# --- read v3 ---------------------------------------------------------------
v3 <- arrow::read_parquet(here::here("data-stage", "susp_v3.parquet"))

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
  "High"
}

is_alt <- function(school_type) {
  st <- tolower(ifelse(is.na(school_type), "", school_type))
  str_detect(st, "juvenile court|community day|alternative|continuation")
}

# --- build features ---------------------------------------------------------
v4 <- v3 %>%
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
      
      grade_min_num %in% c(6,7) & grade_max_num == 9 ~ "Middle (6-8)",
      grade_min_num == 6 & grade_max_num == 10 ~ "Middle (6-8)",
      
      grade_min_num >= 9 ~ "High School",
      grade_min_num == 8 & grade_max_num %in% c(9,10,11,12) ~ "High School",
      grade_min_num == 7 & grade_max_num %in% c(10,11,12) ~ "High School",
      grade_min_num == 5 & grade_max_num == 9 ~ "High School",
      grade_min_num == 6 & grade_max_num %in% c(11,12) ~ "High School",
      grade_max_num == 12 & !is.na(grade_min_num) & grade_min_num >= 1 ~ "High School",
      
      grade_min_num == 6 & grade_max_num == 6 ~ "Middle (6-8)",
      grade_min_num == 4 & grade_max_num == 8 ~ "Elementary",
      grade_min_num == 3 & grade_max_num == 8 ~ "Elementary",
      grade_min_num == 1 & grade_max_num == 8 ~ "Elementary",
      
      TRUE ~ "Other/Unknown"
    ),
    # Alternative override applies to strict3 (and optionally to final)
    level_strict3 = if_else(is_alt(school_type), "Alternative", level_strict3)
    
    # If you also want to force final label to Alternative, uncomment:
    # ,school_level_final = if_else(is_alt(school_type), "Alternative", school_level_final)
  )

# --- write ------------------------------------------------------------------
arrow::write_parquet(v4, here::here("data-stage", "susp_v4.parquet"))
message(">>> 05_feature_school_level (merged): wrote susp_v4.parquet")

# quick counts (optional)
v4 %>%
  distinct(academic_year, county_code, district_code, school_code, level_strict3) %>%
  count(academic_year, level_strict3) %>%
  arrange(academic_year, level_strict3) %>%
  print(n = 60)
