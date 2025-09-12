# R/04_feature_black_prop_quartiles.R
# Adds proportion-Black and proportion-White (per school-year) and year-specific quartiles.

suppressPackageStartupMessages({
  library(here)     # project-root paths
  library(arrow)    # parquet I/O
  library(dplyr)    # data wrangling
  library(stringr)  # string helpers (if needed)
  library(tidyr)    # pivot_wider
})

source(here::here("R","utils_keys_filters.R"))

message(">>> Running from project root: ", here::here())

# -------- helper: safe first non-NA numeric --------
first_non_na_num <- function(x) {
  y <- suppressWarnings(as.numeric(x))
  y <- y[!is.na(y)]
  if (length(y)) y[1] else NA_real_
}
# --- helper: safe ntile on possibly-all-NA vectors ---------------------------
safe_ntile <- function(x, n = 4L) {
  if (all(is.na(x))) return(rep(NA_integer_, length(x)))
  ntile(x, n)
}
# ---- read input ------------------------------------------------------------
v2 <- read_parquet(here("data-stage","susp_v2.parquet")) %>%
  build_keys() %>%
  filter_campus_only() %>%
  mutate(subgroup = canon_race_label(reporting_category))

# Input guard
stopifnot(all(c("subgroup","cumulative_enrollment") %in% names(v2)))

# Optional heads-up if White rows aren't present
if (!any(v2$subgroup == "White", na.rm = TRUE)) {
  warning("No rows with subgroup == 'White' found. If your White label differs, update the filter below.")
}

# --- 1) Pull Black, White, and Total enrollment at SCHOOL x YEAR --------
rb_rw_ta <- v2 %>%
  filter(subgroup %in%
           intersect(c("Black/African American", "White", "All Students"),
                     ALLOWED_RACES)) %>%
  select(academic_year, cds_school, subgroup, cumulative_enrollment) %>%
  mutate(subgroup = dplyr::recode(subgroup,
                                  "Black/African American" = "Black",
                                  "All Students" = "All")) %>%
  group_by(academic_year, cds_school, subgroup) %>%
  summarise(enroll = first_non_na_num(cumulative_enrollment), .groups="drop") %>%
  tidyr::pivot_wider(names_from = subgroup, values_from = enroll, names_prefix = "enroll_")

# --- 2) Proportion Black & White per school-year ---------------------------
# Leave Black/White missing as NA; All must be > 0 to compute proportions
rb_rw_ta <- rb_rw_ta %>%
  mutate(
    prop_black = if_else(!is.na(enroll_All) & enroll_All > 0 & !is.na(enroll_Black),
                         enroll_Black / enroll_All, NA_real_),
    prop_white = if_else(!is.na(enroll_All) & enroll_All > 0 & !is.na(enroll_White),
                         enroll_White / enroll_All, NA_real_)
  )

# --- 3) Year-specific quartiles on prop_black & prop_white -----------------
rbw_q <- rb_rw_ta %>%
  group_by(academic_year) %>%
  mutate(
    black_prop_q = if_else(!is.na(prop_black), safe_ntile(prop_black, 4L), NA_integer_),
    white_prop_q = if_else(!is.na(prop_white), safe_ntile(prop_white, 4L), NA_integer_),

    black_prop_q_label = get_quartile_label(black_prop_q, "Black"),
    white_prop_q_label = get_quartile_label(white_prop_q, "White")
  ) %>%
  ungroup()

# --- 4) Join back to all race rows and save --------------------------------
v3 <- v2 %>% left_join(rbw_q, by = c("academic_year","cds_school"))
stopifnot(nrow(v3) == nrow(v2))

# Freeze label order (optional but handy)
v3$black_prop_q_label <- factor(v3$black_prop_q_label,
                                levels = c(get_quartile_label(1:4, "Black"),"Unknown"))
v3$white_prop_q_label <- factor(v3$white_prop_q_label,
                                levels = c(get_quartile_label(1:4, "White"),"Unknown"))

# Quick ping that special codes aren’t present (campus-only should already handle this)
stopifnot(!any(stringr::str_detect(v3$cds_school, "0000000$|0000001$")))

# --- sanity checks----------------------------------------
# (A) No duplicate school-year keys in wide table
stopifnot(
  rb_rw_ta %>%
    count(academic_year, cds_school) %>%
    summarise(max_n = max(n, na.rm = TRUE), .groups = "drop") %>%
    pull(max_n) == 1
)
# or the simple base-R equivalent:
stopifnot(anyDuplicated(rb_rw_ta[c("academic_year","cds_school")]) == 0)

# (B) Quartile counts per year (Black & White)
v3 %>%
  distinct(academic_year, cds_school, black_prop_q_label, white_prop_q_label) %>%
  count(academic_year, black_prop_q_label, white_prop_q_label) %>%
  arrange(academic_year, black_prop_q_label, white_prop_q_label) %>%
  print(n = 60)

# (C) Why Unknown? (All missing/zero or subgroup missing)
v3 %>%
  mutate(
    unknown_black_reason = case_when(
      is.na(prop_black) & (is.na(enroll_All) | enroll_All <= 0) ~ "All missing/zero",
      is.na(prop_black) &  is.na(enroll_Black)                  ~ "Black missing",
      TRUE                                                      ~ "Not unknown"
    ),
    unknown_white_reason = case_when(
      is.na(prop_white) & (is.na(enroll_All) | enroll_All <= 0) ~ "All missing/zero",
      is.na(prop_white) &  is.na(enroll_White)                  ~ "White missing",
      TRUE                                                      ~ "Not unknown"
    )
  ) %>%
  count(academic_year, unknown_black_reason, unknown_white_reason) %>%
  arrange(academic_year, unknown_black_reason, unknown_white_reason) %>%
  print(n = 60)

# (D) Bounds: proportions in [0,1], Black/White <= All
rb_rw_ta %>%
  summarise(
    any_black_oob = any(prop_black < 0 | prop_black > 1, na.rm = TRUE),
    any_white_oob = any(prop_white < 0 | prop_white > 1, na.rm = TRUE),
    any_Black_gt_All  = any(enroll_Black > enroll_All, na.rm = TRUE),
    any_White_gt_All  = any(enroll_White > enroll_All, na.rm = TRUE),
    .by = academic_year
  ) %>%
  print(n = 60) # All should be FALSE

invisible(TRUE)
# --- write output -----------------------------------------------------------