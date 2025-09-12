# R/06_feature_reason_shares.R
# Build reason-of-suspension shares (wide + long)

# Quiet core libs
suppressPackageStartupMessages({
  library(here)     # project-root paths
  library(arrow)    # parquet I/O
  library(dplyr)    # data wrangling
  library(tidyr)    # pivot_longer
})

message(">>> Running from project root: ", here::here())

# ---- read v4 ---------------------------------------------------------------
v4 <- arrow::read_parquet(here::here("data-stage", "susp_v4.parquet"))

# helper: compute p = numer/denom only when BOTH present and > 0; else NA
prop_raw_pos <- function(numer, denom) {
  dplyr::if_else(!is.na(numer) & !is.na(denom) & numer > 0 & denom > 0,
                 numer / denom, NA_real_)
}

v5 <- v4 %>%
  mutate(
    # raw totals + raw reasons (no sup_* anywhere)
    tot_raw = total_suspensions,
    
    r_vi  = suspension_count_violent_incident_injury,
    r_vn  = suspension_count_violent_incident_no_injury,
    r_wp  = suspension_count_weapons_possession,
    r_id  = suspension_count_illicit_drug_related,
    r_def = suspension_count_defiance_only,
    r_oth = suspension_count_other_reasons,
    
    # six proportion columns (rate only when BOTH raw values > 0)
    prop_susp_violent_injury      = prop_raw_pos(r_vi,  tot_raw),
    prop_susp_violent_no_injury   = prop_raw_pos(r_vn,  tot_raw),
    prop_susp_weapons_possession  = prop_raw_pos(r_wp,  tot_raw),
    prop_susp_illicit_drug        = prop_raw_pos(r_id,  tot_raw),
    prop_susp_defiance_only       = prop_raw_pos(r_def, tot_raw),
    prop_susp_other_reasons       = prop_raw_pos(r_oth, tot_raw)
  ) %>%
  # drop helper aliases; keep tot_raw if you want it visible
  select(-r_vi, -r_vn, -r_wp, -r_id, -r_def, -r_oth)

# optional: long format for analysis/plotting
v5_long <- v5 %>%
  pivot_longer(
    starts_with("prop_susp_"),
    names_to  = "reason",
    values_to = "prop_of_total_susp"
  ) %>%
  mutate(reason = sub("^prop_susp_", "", reason)) %>%
  add_reason_label()

# ---- write outputs ---------------------------------------------------------
arrow::write_parquet(v5,      here::here("data-stage", "susp_v5.parquet"))
arrow::write_parquet(v5_long, here::here("data-stage", "susp_v5_long.parquet"))
message(">>> 06_feature_reason_shares: wrote susp_v5.parquet and susp_v5_long.parquet")

# -------- quick checks you’ll see in Console --------
# 1) proportions exist only when both raw values > 0, and are within (0,1]
v5 %>%
  transmute(
    pmin = pmin(prop_susp_violent_injury, prop_susp_violent_no_injury,
                prop_susp_weapons_possession, prop_susp_illicit_drug,
                prop_susp_defiance_only, prop_susp_other_reasons, na.rm = TRUE),
    pmax = pmax(prop_susp_violent_injury, prop_susp_violent_no_injury,
                prop_susp_weapons_possession, prop_susp_illicit_drug,
                prop_susp_defiance_only, prop_susp_other_reasons, na.rm = TRUE)
  ) %>%
  summarise(
    any_below0 = any(pmin <= 0, na.rm = TRUE),  # should be FALSE (we only keep numer>0)
    any_above1 = any(pmax > 1,  na.rm = TRUE)
  ) %>%
  print()

# 2) how many non-NA proportions per year (sanity)
v5 %>%
  summarise(
    n_with_tot_raw_pos = sum(!is.na(tot_raw) & tot_raw > 0),
    n_vi  = sum(!is.na(prop_susp_violent_injury)),
    n_vn  = sum(!is.na(prop_susp_violent_no_injury)),
    n_wp  = sum(!is.na(prop_susp_weapons_possession)),
    n_id  = sum(!is.na(prop_susp_illicit_drug)),
    n_def = sum(!is.na(prop_susp_defiance_only)),
    n_oth = sum(!is.na(prop_susp_other_reasons)),
    .by = academic_year
  ) %>%
  print(n = 30)

invisible(TRUE)
