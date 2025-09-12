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

LEVEL_LABELS <- c("Elementary", "Middle", "High", "Other", "Alternative")

span_label <- function(gmin, gmax) {
  if (is.na(gmin) || is.na(gmax)) return("Other")
  if (gmin <= 0 && gmax >= 12) return("Other")
  if (gmax <= 5) return("Elementary")
  if (gmin >= 6 && gmax <= 8) return("Middle")
  if (gmax >= 9) return("High")
  if (gmin <= 0 && gmax <= 8) return("Elementary")
  "Other"
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

    # unified school level label
    school_level = factor(mapply(span_label, grade_min_num, grade_max_num),
                          levels = LEVEL_LABELS),

    # Alternative override
    school_level = if_else(is_alt(school_type), "Alternative", as.character(school_level)),
    school_level = factor(school_level, levels = LEVEL_LABELS),

    # legacy aliases
    level_strict3 = school_level,
    school_level_final = school_level
  )

# row-count must be stable through feature adds
stopifnot(nrow(v4) == nrow(v3_in))

# --- write ------------------------------------------------------------------
arrow::write_parquet(v4, here::here("data-stage", "susp_v4.parquet"))
message(">>> 05_feature_school_level (merged): wrote susp_v4.parquet")

# ---- optional sanity prints (A/B) ------------------------------------------
if (isTRUE(SHOW_SANITY)) {
  # topline counts by school level
  v4 %>%
    distinct(academic_year, county_code, district_code, school_code, school_level) %>%
    count(academic_year, school_level) %>%
    arrange(academic_year, school_level) %>%
    print(n = 60)

  # B) unit-test several edge grade strings
  tibble(grades_served = c("K-8","7-12","PK-12","6","9-12","TK-5")) %>%
    mutate(
      gmin = get_min_grade(grades_served),
      gmax = get_max_grade(grades_served),
      span_lbl = mapply(span_label, gmin, gmax)
    ) %>%
    print()
}

invisible(TRUE)
# --- end --------------------------------------------------------------------
