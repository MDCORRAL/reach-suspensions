suppressPackageStartupMessages({
  library(dplyr); library(stringr); library(forcats); library(janitor)
  library(arrow); library(here); library(tidyr); library(purrr)
})

# ---- Paths (same Google Drive data-stage) -----------------------------------
DATA_STAGE <- normalizePath(
  "~/Library/CloudStorage/GoogleDrive-mdcorral@g.ucla.edu/.shortcut-targets-by-id/1qNAOKIg0UjuT3XWFlk4dkDLN6UPWJVGx/Center for the Transformation of Schools/Research/CA Race Education And Community Healing (REACH)/2. REACH Network (INTERNAL)/15. REACH Baseline Report_Summer 2025/6. R Data Analysis Project Folders/reach-suspensions/data-stage",
  mustWork = TRUE
)
V5_PARQ      <- file.path(DATA_STAGE, "susp_v5.parquet")   # race/eth + black quartiles
OTH_PARQ     <- file.path(DATA_STAGE, "oth_long.parquet")  # other demo (SPED/EL/SEX/etc.)
V6_FEAT_PARQ <- file.path(DATA_STAGE, "susp_v6_features.parquet")
V6_LONG_PARQ <- file.path(DATA_STAGE, "susp_v6_long.parquet")

# ---- Helpers ----------------------------------------------------------------
safe_div <- function(n, d) ifelse(is.na(d) | d == 0, NA_real_, n / d)
find_col <- function(df, patterns) {
  nm <- names(df)
  for (rx in patterns) {
    hit <- grep(rx, nm, ignore.case = TRUE, value = TRUE)
    if (length(hit)) return(hit[1])
  }
  NA_character_
}

# ---- Load raw ---------------------------------------------------------------
v5  <- read_parquet(V5_PARQ)  |> clean_names()
oth <- read_parquet(OTH_PARQ) |> clean_names()

# ---- Standardize keys (school_code, academic_year) --------------------------
# Make both character, pad school_code to max width seen across both
key_cols <- list(
  v5_school  = find_col(v5,  c("^school_code$", "^cds_code$", "^school_id$")),
  v5_year    = find_col(v5,  c("^academic_year$", "^year$", "school_?year$", "^ay$")),
  oth_school = find_col(oth, c("^school_code$", "^cds_code$", "^school_id$")),
  oth_year   = find_col(oth, c("^academic_year$", "^year$", "school_?year$", "^ay$"))
)
stopifnot(!is.na(key_cols$v5_school), !is.na(key_cols$v5_year),
          !is.na(key_cols$oth_school), !is.na(key_cols$oth_year))

v5 <- v5 |>
  mutate(school_code = as.character(.data[[key_cols$v5_school]]),
         year        = as.character(.data[[key_cols$v5_year]]))

oth <- oth |>
  mutate(school_code = as.character(.data[[key_cols$oth_school]]),
         year        = as.character(.data[[key_cols$oth_year]]))

target_w <- max(nchar(c(v5$school_code, oth$school_code)), na.rm = TRUE)
v5  <- v5  |> mutate(school_code = str_pad(school_code, target_w, pad = "0"))
oth <- oth |> mutate(school_code = str_pad(school_code, target_w, pad = "0"))

# ---- Build roster (unique school-year) --------------------------------------
roster <- v5 |> distinct(school_code, year)

# ---- From v5: keep race/eth features you rely on ----------------------------
v5_feats <- v5 |>
  transmute(
    school_code, year,
    black_share = prop_black,
    black_q     = fct_relevel(as.factor(black_prop_q_label %||% paste0("Q", black_prop_q4)),
                              "Q1","Q2","Q3","Q4"),
    school_type
  )

# ---- From oth: build long metrics per (category, subgroup) ------------------
cat_col   <- find_col(oth, c("^category_type$", "reporting_category_description", "^reporting_category$"))
subg_col  <- find_col(oth, c("^subgroup$","subgroup_name","reporting_subgroup"))
num_undup <- find_col(oth, c("unduplicated.*suspend", "suspend.*unduplicated"))
num_total <- find_col(oth, c("^total.*susp", "susp.*total", "suspensions_total$"))
den_enr   <- find_col(oth, c("special.*education.*enroll", "^sped_?enr$",
                             "^subgroup_?enrollment$", "cumulative_?enroll", "^enrollment$"))

# If we can’t find suspensions or denominator, we still keep counts if present
use_num <- if (!is.na(num_undup)) num_undup else num_total
has_num <- !is.na(use_num)
has_den <- !is.na(den_enr)

oth_long_metrics <- oth |>
  mutate(
    category = if (!is.na(cat_col)) .data[[cat_col]] else NA_character_,
    subgroup = if (!is.na(subg_col)) .data[[subg_col]] else NA_character_,
    num      = if (has_num) .data[[use_num]] else NA_real_,
    den      = if (has_den) .data[[den_enr]]  else NA_real_,
    rate     = if (has_num && has_den) safe_div(num, den) else NA_real_
  ) |>
  select(school_code, year, category, subgroup, num, den, rate)

# ---- Promote a few headline features to wide columns (SPED/EL/SEX examples) --
# Adjust the patterns below to match your labels.
sel_rows <- function(df, pat) dplyr::filter(df, str_detect(str_to_lower(category), pat))

sped_wide <- sel_rows(oth_long_metrics, "special\\s*education|sped") |>
  group_by(school_code, year) |>
  summarise(sped_num = sum(num, na.rm = TRUE),
            sped_den = suppressWarnings(max(den, na.rm = TRUE)),
            sped_rate = safe_div(sped_num, sped_den),
            .groups = "drop")

ell_wide <- sel_rows(oth_long_metrics, "\\bell\\b|english.*learner|english\\slanguage\\slearner") |>
  group_by(school_code, year) |>
  summarise(ell_num = sum(num, na.rm = TRUE),
            ell_den = suppressWarnings(max(den, na.rm = TRUE)),
            ell_rate = safe_div(ell_num, ell_den),
            .groups = "drop")

# --- Sex-specific suspension rates (robust to your labels) -------------------
sex_rates <- oth_long_metrics %>%
  filter(stringr::str_to_lower(category) == "sex") %>%   # only the Sex category
  mutate(
    sg = dplyr::case_when(
      stringr::str_to_lower(subgroup) == "male"        ~ "male",
      stringr::str_to_lower(subgroup) == "female"      ~ "female",
      stringr::str_detect(stringr::str_to_lower(subgroup), "non[- ]?binary") ~ "non_binary",
      stringr::str_detect(stringr::str_to_lower(subgroup), "missing|not reported") ~ NA_character_, # drop
      TRUE ~ NA_character_
    )
  ) %>%
  filter(!is.na(sg)) %>%
  group_by(school_code, year, sg) %>%
  summarise(
    num = sum(num, na.rm = TRUE),
    den = suppressWarnings(max(den, na.rm = TRUE)),
    .groups = "drop"
  ) %>%
  mutate(rate = safe_div(num, den)) %>%
  select(school_code, year, sg, rate) %>%
  tidyr::pivot_wider(
    names_from = sg, values_from = rate,
    names_glue = "sex_{sg}_rate"      # -> sex_male_rate, sex_female_rate, sex_non_binary_rate
  )

# Example join into your v6_features build:
# v6_features <- v6_features %>% left_join(sex_rates, by = c("school_code","year"))


# (Add FRPM/SES when you confirm the label; pattern often includes "socioeconomic|frpm|low.*income".)

# ---- Assemble v6_features (one row per school-year) --------------------------
# After building `sex_rates` as we did:
# sex_rates: columns = school_code, year, sex_male_rate, sex_female_rate, sex_non_binary_rate (optional)
v6_features <- roster |>
  left_join(v5_feats,   by = c("school_code","year")) |>
  left_join(sped_wide,  by = c("school_code","year")) |>
  left_join(ell_wide,   by = c("school_code","year")) |>
  left_join(sex_rates,  by = c("school_code","year"))     # <— replace male_wide with sex_rates


# Optional: carry a clean traditional flag once, for all future analyses
non_trad_patterns <- c(
  "community day","juvenile","court","county community","continuation",
  "alternative","opportunity","adult","independent study","home","hospital",
  "state special","special education","jail","youth authorit"
)
v6_features <- v6_features |>
  mutate(
    stype_lower    = str_to_lower(school_type),
    is_non_trad    = str_detect(stype_lower, paste(non_trad_patterns, collapse = "|")),
    looks_trad     = str_detect(stype_lower, "traditional|regular|elementary|middle|high|k-12|k12"),
    is_traditional = looks_trad & !is_non_trad
  ) |>
  select(-stype_lower, -is_non_trad, -looks_trad)

# ---- Write outputs -----------------------------------------------------------
write_parquet(v6_features, V6_FEAT_PARQ)
write_parquet(oth_long_metrics, V6_LONG_PARQ)
message("Wrote:\n- ", V6_FEAT_PARQ, "\n- ", V6_LONG_PARQ)
