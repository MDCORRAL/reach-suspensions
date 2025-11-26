# R/05_feature_school_levels.R
library(dplyr)
library(stringr)
library(arrow)

`%||%` <- function(a,b) if (is.null(a)) b else a

# Read v3 (with your earlier features)
v3 <- arrow::read_parquet("data-stage/susp_v3.parquet")

# --- Normalize "grades_served" text -----------------------------------------
norm_grades <- function(x) {
  x <- toupper(trimws(x %||% ""))
  ifelse(
    x %in% c("", "N/A", "NO DATA", "NA"),
    NA_character_,
    x |>
      stringr::str_replace_all("[–—−]", "-") |>
      stringr::str_replace_all("\\s+", " ") |>
      trimws()
  )
}

# --- Helpers to parse "K-8", "6-8", "10-12", "K-12" into numeric min/max ----
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

# --- Build v4 with your final level rules -----------------------------------
v4 <- v3 %>%
  mutate(
    grades_norm   = norm_grades(grades_served),
    grade_min_num = get_min_grade(grades_norm),
    grade_max_num = get_max_grade(grades_norm),
    
    school_level_final = case_when(
      # exact buckets
      !is.na(grade_min_num) & !is.na(grade_max_num) &
        grade_min_num <= 0 & grade_max_num == 12 ~ "K-12",
      
      # Middle (explicit)
      grade_min_num == 6 & grade_max_num == 8 ~ "Middle (6-8)",
      grade_min_num == 7 & grade_max_num == 8 ~ "Middle (6-8)",
      grade_min_num == 5 & grade_max_num == 8 ~ "Middle (6-8)",
      grade_min_num == 4 & grade_max_num == 6 ~ "Middle (6-8)",
      grade_min_num == 8 & grade_max_num == 8 ~ "Middle (6-8)",  # single 8
      grade_min_num == 6 & grade_max_num == 7 ~ "Middle (6-8)",   # 6–7
      grade_min_num == 5 & grade_max_num == 6 ~ "Middle (6-8)",   # 5–6
      
      # Elementary (explicit)
      grade_min_num <= 0 & grade_max_num == 8 ~ "Elementary",     # K–8
      grade_min_num <= 0 & grade_max_num == 9 ~ "Elementary",     # K–9
      grade_min_num <= 0 & grade_max_num <= 8 ~ "Elementary",     # K–* up to 8
      grade_min_num <= 4 & grade_max_num <= 8 ~ "Elementary",     # 1–8, 2–6, etc.
      grade_min_num >= 0 & grade_max_num <= 5 ~ "Elementary",     # single K..5
      
      # Middle (multi-year)
      grade_min_num %in% c(6,7) & grade_max_num == 9 ~ "Middle (6-8)", # 6–9, 7–9
      grade_min_num == 6 & grade_max_num == 10 ~ "Middle (6-8)",       # 6–10
      
      # High School (explicit)
      grade_min_num >= 9 ~ "High School",                     # 9–*, 10–*, 11–*, 12
      grade_min_num == 8 & grade_max_num %in% c(9,10,11,12) ~ "High School",  # 8–9..12
      grade_min_num == 7 & grade_max_num %in% c(10,11,12)   ~ "High School",  # 7–10..12
      grade_min_num == 5 & grade_max_num == 9               ~ "High School",  # 5–9
      grade_min_num == 6 & grade_max_num %in% c(11,12)      ~ "High School",  # 6–11/12
      grade_max_num == 12 & !is.na(grade_min_num) & grade_min_num >= 1 ~ "High School",  # 1–12..5–12
      
      # Single-grade fallbacks
      grade_min_num == 6 & grade_max_num == 6 ~ "Middle (6-8)",  # single 6
      grade_min_num == 4 & grade_max_num == 8 ~ "Elementary",    # 4–8
      grade_min_num == 3 & grade_max_num == 8 ~ "Elementary",    # 3–8
      grade_min_num == 1 & grade_max_num == 8 ~ "Elementary",    # 1–8
      
      TRUE ~ "Other/Unknown"
    )
  )

# Write out v4
arrow::write_parquet(v4, "data-stage/susp_v4.parquet")

# --- Quick sanity checks -----------------------------------------------------
cat("\nCounts by year x school_level_final:\n")
v4 %>%
  distinct(academic_year, county_code, district_code, school_code, school_level_final) %>%
  count(academic_year, school_level_final) %>%
  arrange(academic_year, school_level_final) %>%
  print(n = 60)

cat("\nTop raw grades_served -> final mapping (top 100):\n")
v4 %>%
  distinct(academic_year, county_code, district_code, school_code,
           grades_served, school_level_final) %>%
  count(grades_served, school_level_final, sort = TRUE) %>%
  print(n = 100)
