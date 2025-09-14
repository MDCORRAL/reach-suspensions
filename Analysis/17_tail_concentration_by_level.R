# 17_tail_concentration_by_level.R
# Computes Pareto concentration metrics by school level and setting.
# Scans available parquet files in data-stage/ to ensure the correct
# suspension and feature datasets are used.

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(stringr)
  library(janitor)
  library(here)
  library(purrr)
  library(readr)
})

try(here::i_am("Analysis/17_tail_concentration_by_level.R"), silent = TRUE)

# ---------------------------------------------------------------------------
# Locate input parquet files
# ---------------------------------------------------------------------------
DATA_STAGE <- here("data-stage")

# Required columns for the core suspension dataset
req_cols <- c(
  "school_code", "academic_year", "cumulative_enrollment",
  "total_suspensions", "unduplicated_count_of_students_suspended_total",
  "subgroup"
)

# Find the first susp_v*_long.parquet file containing all required columns
susp_files <- list.files(DATA_STAGE, pattern = "^susp_v[0-9]+_long\\.parquet$", full.names = TRUE)
INPUT_PATH <- NULL
for (f in susp_files) {
  cols <- names(read_parquet(f, as_data_frame = FALSE))
  if (all(req_cols %in% cols)) {
    INPUT_PATH <- f
    message("Using suspension data: ", basename(f))
    break
  } else {
    message("Skipping ", basename(f), ": missing columns")
  }
}
if (is.null(INPUT_PATH)) stop("No susp_v*_long.parquet file with all required columns found.")

# Locate feature file for level/setting
V6_FEAT <- file.path(DATA_STAGE, "susp_v6_features.parquet")
if (!file.exists(V6_FEAT)) {
  stop("Missing susp_v6_features.parquet. Run R/22_build_v6_features.R first.")
} else {
  message("Using features: ", basename(V6_FEAT))
}

# ---------------------------------------------------------------------------
# Load and clean data
# ---------------------------------------------------------------------------
MEASURE <- "total_susp"                 # or "undup_susp" for unduplicated
TOP_PCT <- c(0.05, 0.10, 0.20)          # Top share cutoffs

# Helper to compute shares safely
safe_div <- function(n, d) ifelse(is.na(d) | d == 0, NA_real_, n / d)

# Helper to compute Pareto shares for a grouped data frame
pareto_shares <- function(df, top_ps = TOP_PCT) {
  n_schools <- n_distinct(df$school_id)
  if (n_schools == 0) return(tibble())

  df_sorted <- df %>%
    group_by(school_id, school_name, enrollment) %>%
    summarise(measure = sum(measure, na.rm = TRUE), .groups = "drop") %>%
    arrange(desc(measure)) %>%
    mutate(
      rank          = row_number(),
      cum_measure   = cumsum(measure),
      total_measure = sum(measure, na.rm = TRUE),
      share_cum     = safe_div(cum_measure, total_measure)
    )

  map_dfr(top_ps, function(p) {
    cutoff_n <- max(1, floor(p * n_schools))
    df_sorted %>%
      slice(1:cutoff_n) %>%
      summarise(
        top_schools   = cutoff_n,
        total_schools = n_schools,
        top_share     = safe_div(sum(measure, na.rm = TRUE), df_sorted$total_measure[1])
      ) %>%
      mutate(top_pct = p)
  })
}

# Load suspension data and restrict to Total/All Students
raw <- read_parquet(INPUT_PATH) %>% clean_names()

susp <- raw %>%
  filter(str_to_lower(subgroup) %in% c("total", "all students", "ta")) %>%
  transmute(
    school_id   = school_code,
    year        = academic_year,
    enrollment  = as.numeric(cumulative_enrollment),
    total_susp  = as.numeric(total_suspensions),
    undup_susp  = as.numeric(unduplicated_count_of_students_suspended_total),
    measure     = if (MEASURE == "undup_susp") undup_susp else total_susp,
    school_name = if ("school_name" %in% names(.)) school_name else school_code
  ) %>%
  mutate(year_num = as.integer(str_sub(year, 1, 4))) %>%
  filter(!is.na(year_num), enrollment > 0, !is.na(measure))

# Load features for level and setting
feat <- read_parquet(V6_FEAT) %>% clean_names() %>%
  transmute(
    school_code = as.character(school_code),
    year_char   = as.character(year),
    is_traditional,
    school_type
  )

# Join features and map fields
susp_feat <- susp %>%
  left_join(feat, by = c("school_id" = "school_code", "year" = "year_char")) %>%
  mutate(
    setting = ifelse(is.na(is_traditional) | !is_traditional, "Non-traditional", "Traditional"),
    level   = coalesce(school_type, "Other")
  )

# ---------------------------------------------------------------------------
# Pareto shares by year × level × setting
# ---------------------------------------------------------------------------
ps_y_ls <- susp_feat %>%
  filter(!is.na(level), !is.na(setting)) %>%
  group_by(year_num, level, setting) %>%
  group_modify(~pareto_shares(.x, TOP_PCT)) %>%
  ungroup() %>%
  mutate(measure_type = MEASURE)

# Slide-ready labels
ps_y_ls_lines <- ps_y_ls %>%
  mutate(
    top_label = paste0("Top ", scales::percent(top_pct)),
    share_pct = scales::percent(top_share, accuracy = 0.1)
  ) %>%
  select(year_num, level, setting, top_label, share_pct, top_schools, total_schools, measure_type)

# ---------------------------------------------------------------------------
# Write outputs
# ---------------------------------------------------------------------------
RUN_TAG <- format(Sys.time(), "%Y%m%d_%H%M")
OUT_DIR <- here("outputs", paste0("tail_concentration_by_level_", RUN_TAG))
if (!dir.exists(OUT_DIR)) dir.create(OUT_DIR, recursive = TRUE)

write_csv(ps_y_ls,       file.path(OUT_DIR, "pareto_shares_by_year_level_setting_raw.csv"))
write_csv(ps_y_ls_lines, file.path(OUT_DIR, "pareto_shares_by_year_level_setting_slide_ready.csv"))

message("Wrote outputs to ", OUT_DIR)

# End of file
