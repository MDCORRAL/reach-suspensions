# R/22_build_v6_features.R
# Build v6 features and analyze SPED suspension rate by Black enrollment quartile.

suppressPackageStartupMessages({
  library(dplyr); library(stringr); library(forcats); library(janitor)
  library(arrow); library(here); library(tidyr); library(purrr)
  library(ggplot2); library(openxlsx); library(scales)
})

# -------------------- Config ---------------------------------------------------
REBUILD_V6 <- TRUE  # set FALSE to skip rebuild if susp_v6_features.parquet already exists

DATA_STAGE   <- here("data-stage")
V5_PARQ      <- file.path(DATA_STAGE, "susp_v5.parquet")       # race/eth + quartiles
OTH_PARQ     <- file.path(DATA_STAGE, "oth_long.parquet")      # other demos (SPED/EL/SEX/etc.)
V6_FEAT_PARQ <- file.path(DATA_STAGE, "susp_v6_features.parquet")
V6_LONG_PARQ <- file.path(DATA_STAGE, "susp_v6_long.parquet")

# -------------------- Repo utilities (optional) -------------------------------
if (file.exists(here("R","utils_keys_filters.R"))) source(here("R","utils_keys_filters.R"))
if (file.exists(here("R","00_paths.R")))           source(here("R","00_paths.R"))

# -------------------- Helpers --------------------------------------------------
safe_div <- function(n, d) ifelse(is.na(d) | d <= 0, NA_real_, n / d)
safe_max <- function(x) if (length(x) == 0 || all(is.na(x))) NA_real_ else max(x, na.rm = TRUE)

find_col <- function(df, patterns) {
  nm <- names(df)
  for (rx in patterns) {
    hit <- grep(rx, nm, ignore.case = TRUE, value = TRUE)
    if (length(hit)) return(hit[1])
  }
  NA_character_
}

rng_ok <- function(x) all(is.na(x) | (x >= 0 & x <= 1))

keep_topline <- function(df, cat_pat, code_pat = NULL, subgroup_pat = NULL) {
  out <- df %>% filter(str_detect(str_to_lower(category), cat_pat))
  if (!is.null(code_pat) && any(!is.na(out$s_code))) {
    out <- out %>% filter(str_detect(str_to_lower(s_code), code_pat))
  } else if (!is.null(subgroup_pat)) {
    out <- out %>% filter(str_detect(str_to_lower(subgroup), subgroup_pat))
  }
  # pick a single record per school-year (prefer the largest denominator)
  out %>%
    group_by(school_code, year) %>%
    slice_max(order_by = den, n = 1, with_ties = FALSE) %>%
    ungroup()
}

drop_impossible <- function(df) {
  df %>%
    filter(!( (!is.na(num) & !is.na(den)) & (num < 0 | den <= 0 | num > den) ))
}

# -------------------- (A) BUILD v6_features -----------------------------------
if (REBUILD_V6 || !file.exists(V6_FEAT_PARQ)) {
  message(">>> Rebuilding v6_features from v5 + oth ...")
  stopifnot(file.exists(V5_PARQ), file.exists(OTH_PARQ))
  
  v5  <- read_parquet(V5_PARQ)  |> clean_names()
  oth <- read_parquet(OTH_PARQ) |> clean_names()
  
  # Standardize keys
  v5_school  <- find_col(v5,  c("^school_code$", "^cds_code$", "^school_id$"))
  v5_year    <- find_col(v5,  c("^academic_year$", "^year$", "school_?year$", "^ay$"))
  oth_school <- find_col(oth, c("^school_code$", "^cds_code$", "^school_id$"))
  oth_year   <- find_col(oth, c("^academic_year$", "^year$", "school_?year$", "^ay$"))
  stopifnot(!is.na(v5_school), !is.na(v5_year), !is.na(oth_school), !is.na(oth_year))
  
  v5  <- v5  |> mutate(school_code = as.character(.data[[v5_school]]),
                       year        = as.character(.data[[v5_year]]))
  oth <- oth |> mutate(school_code = as.character(.data[[oth_school]]),
                       year        = as.character(.data[[oth_year]]))
  
  # Keep leading zeros
  target_w <- suppressWarnings(max(nchar(c(v5$school_code, oth$school_code)), na.rm = TRUE))
  v5  <- v5  |> mutate(school_code = str_pad(school_code, target_w, pad = "0"))
  oth <- oth |> mutate(school_code = str_pad(school_code, target_w, pad = "0"))
  
  # Apply repo filters if available
  if (exists("build_keys")) {
    v5  <- v5  |> build_keys()
    oth <- oth |> build_keys()
  }
  if (exists("filter_campus_only") && "aggregate_level" %in% names(v5))  v5  <- v5  |> filter_campus_only()
  if (exists("filter_campus_only") && "aggregate_level" %in% names(oth)) oth <- oth |> filter_campus_only()
  
  # Roster
  roster <- v5 |> distinct(school_code, year)
  
  # v5: one row per school-year (All Students)
  v5_core <- if ("reporting_category" %in% names(v5)) {
    v5 %>%
      mutate(subgroup = dplyr::coalesce(subgroup, canon_race_label(reporting_category))) %>%
      filter(subgroup == "All Students") %>%
      distinct(school_code, year, .keep_all = TRUE)
  } else {
    v5 %>%
      group_by(school_code, year) %>%
      summarise(across(everything(), ~dplyr::first(na.omit(.))), .groups = "drop")
  }
  # Quartile label cleanup
  v5_feats <- v5_core %>%
    mutate(
      black_prop_q = as.integer(black_prop_q),
      black_prop_q_label = case_when(
        !is.na(black_prop_q_label) ~ str_replace(as.character(black_prop_q_label), "\\s*\\(.*\\)$", ""),
        !is.na(black_prop_q)      ~ paste0("Q", black_prop_q),
        TRUE ~ "Unknown"
      ),
      black_prop_q_label = factor(black_prop_q_label,
                                  levels = c("Q1","Q2","Q3","Q4","Unknown"),
                                  ordered = TRUE)
    ) %>%
    transmute(
      school_code, year,
      black_share = prop_black,
      black_prop_q,
      black_prop_q_label,
      school_type
    )
  
  # OTH → long metrics (category, subgroup)
  cat_col   <- find_col(oth, c("^category_type$", "reporting_category_description", "^reporting_category$"))
  subg_col  <- find_col(oth, c("^subgroup$","subgroup_name","reporting_subgroup"))
  code_col  <- find_col(oth, c("^subgroup_code$","subgroupid","subgroup_code_id"))
  num_undup <- find_col(oth, c("unduplicated.*suspend", "suspend.*unduplicated"))
  num_total <- find_col(oth, c("^total.*susp", "susp.*total", "suspensions_total$"))
  den_enr   <- find_col(oth, c("special.*education.*enroll", "^sped_?enr$",
                               "^subgroup_?enrollment$", "cumulative_?enroll", "^enrollment$"))
  
  use_num <- if (!is.na(num_undup)) num_undup else num_total
  has_num <- !is.na(use_num); has_den <- !is.na(den_enr)
  
  oth_long <- oth %>%
    mutate(
      category = if (!is.na(cat_col)) .data[[cat_col]] else NA_character_,
      subgroup = if (!is.na(subg_col)) .data[[subg_col]] else NA_character_,
      s_code   = if (!is.na(code_col)) .data[[code_col]] else NA_character_,
      num      = if (has_num) .data[[use_num]] else NA_real_,
      den      = if (has_den) .data[[den_enr]] else NA_real_,
      rate     = if (has_num && has_den) safe_div(num, den) else NA_real_
    ) %>%
    select(school_code, year, category, subgroup, s_code, num, den, rate)
  
  # SPED: Students with Disabilities (current)
  sped_top  <- keep_topline(
    oth_long,
    cat_pat  = "special\\s*education|\\bsped\\b|students? with disab",
    code_pat = "^(swd|se|sped)$",
    subgroup_pat = "students? with disab|\\bswd\\b|^sped$"
  ) %>% drop_impossible()
  
  sped_wide <- sped_top %>%
    group_by(school_code, year) %>%
    summarise(
      sped_num  = sum(num, na.rm = TRUE),
      sped_den  = safe_max(den),
      sped_rate = safe_div(sped_num, sped_den),
      .groups   = "drop"
    )
  
  # EL: current English Learner (exclude RFEP/IFEP histories)
  ell_top <- keep_topline(
    oth_long,
    cat_pat  = "\\bell\\b|english.*learner|english\\s+language\\s+learner",
    code_pat = "^(el|ell)$",
    subgroup_pat = "^el$|^english learner$|^current el$"
  ) %>% drop_impossible()
  
  ell_wide <- ell_top %>%
    group_by(school_code, year) %>%
    summarise(
      ell_num  = sum(num, na.rm = TRUE),
      ell_den  = safe_max(den),
      ell_rate = safe_div(ell_num, ell_den),
      .groups  = "drop"
    )

  # Migrant
  migrant_top <- keep_topline(
    oth_long,
    cat_pat  = "migrant",
    code_pat = "^(mg)$",
    subgroup_pat = "^migrant$"
  ) %>% drop_impossible()

  migrant_wide <- migrant_top %>%
    group_by(school_code, year) %>%
    summarise(
      migrant_num  = sum(num, na.rm = TRUE),
      migrant_den  = safe_max(den),
      migrant_rate = safe_div(migrant_num, migrant_den),
      .groups      = "drop"
    )

  # Foster
  foster_top <- keep_topline(
    oth_long,
    cat_pat  = "foster",
    code_pat = "^(fy)$",
    subgroup_pat = "foster youth"
  ) %>% drop_impossible()

  foster_wide <- foster_top %>%
    group_by(school_code, year) %>%
    summarise(
      foster_num  = sum(num, na.rm = TRUE),
      foster_den  = safe_max(den),
      foster_rate = safe_div(foster_num, foster_den),
      .groups     = "drop"
    )

  # Homeless
  homeless_top <- keep_topline(
    oth_long,
    cat_pat  = "homeless",
    code_pat = "^(hl)$",
    subgroup_pat = "^homeless$"
  ) %>% drop_impossible()

  homeless_wide <- homeless_top %>%
    group_by(school_code, year) %>%
    summarise(
      homeless_num  = sum(num, na.rm = TRUE),
      homeless_den  = safe_max(den),
      homeless_rate = safe_div(homeless_num, homeless_den),
      .groups       = "drop"
    )

  # Socioeconomically Disadvantaged
  sed_top <- keep_topline(
    oth_long,
    cat_pat  = "socio|disadv",
    code_pat = "^(sd)$",
    subgroup_pat = "socioeconomically disadvantaged"
  ) %>% drop_impossible()

  sed_wide <- sed_top %>%
    group_by(school_code, year) %>%
    summarise(
      sed_num  = sum(num, na.rm = TRUE),
      sed_den  = safe_max(den),
      sed_rate = safe_div(sed_num, sed_den),
      .groups  = "drop"
    )
  
  # SEX: male / female / (optional) non_binary
  # --- Start of Corrected Block 2 ---
  
  # SEX: male / female / (optional) non_binary
  sex_rates <- oth_long %>%
    filter(str_to_lower(category) %in% c("sex","gender")) %>%
    drop_impossible() %>% # <-- ADD THIS LINE
    mutate(
      sg = case_when(
        str_to_lower(subgroup) %in% c("male","m") ~ "male",
        str_to_lower(subgroup) %in% c("female","f") ~ "female",
        str_detect(str_to_lower(subgroup), "non[- ]?binary") ~ "non_binary",
        str_detect(str_to_lower(subgroup), "missing|not reported") ~ NA_character_,
        TRUE ~ NA_character_
      )
    ) %>%
    filter(!is.na(sg)) %>%
    group_by(school_code, year, sg) %>%
    summarise(num = sum(num, na.rm = TRUE),
              den = safe_max(den), .groups = "drop") %>%
    mutate(rate = safe_div(num, den)) %>%
    select(school_code, year, sg, rate) %>%
    pivot_wider(names_from = sg, values_from = rate, names_glue = "sex_{sg}_rate")
  
  # --- End of Corrected Block 2 ---
  
  # Assemble (one row per school-year)
  v6_features <- roster %>%
    left_join(v5_feats,   by = c("school_code","year")) %>%
    left_join(sped_wide,  by = c("school_code","year")) %>%
    left_join(ell_wide,   by = c("school_code","year")) %>%
    left_join(migrant_wide,  by = c("school_code","year")) %>%
    left_join(foster_wide,   by = c("school_code","year")) %>%
    left_join(homeless_wide, by = c("school_code","year")) %>%
    left_join(sed_wide,      by = c("school_code","year")) %>%
    left_join(sex_rates,     by = c("school_code","year"))
  
  # Traditional flag (robust: no NAs)
  # --- Start of Corrected Block 1 ---
  
  # Traditional flag (robust: no NAs)
  non_trad_patterns <- c(
    "community day","juvenile","court","county community","continuation",
    "alternative","opportunity","adult","independent study","home","hospital",
    "state special","special education","jail","youth authorit","detention","probation"
  )
  v6_features <- v6_features %>%
    mutate(
      stype_lower    = str_to_lower(coalesce(school_type, "")),
      looks_trad     = str_detect(stype_lower, "traditional|regular|elementary|middle|high|k-12|k12"),
      is_non_trad    = str_detect(stype_lower, paste(non_trad_patterns, collapse = "|")),
      # CORRECTED LOGIC: Use standard boolean operators and handle NAs.
      is_traditional = ifelse(is.na(looks_trad), FALSE, looks_trad) & 
        ifelse(is.na(is_non_trad), FALSE, !is_non_trad)
    ) %>% select(-stype_lower, -looks_trad, -is_non_trad)
  
  # --- End of Corrected Block 1 ---
  
  # Dedup guard
  dup_check <- v6_features %>% count(school_code, year) %>% filter(n > 1)
  if (nrow(dup_check) > 0) {
    message("Deduplicating duplicate school-year rows...")
    v6_features <- v6_features %>% group_by(school_code, year) %>% slice_head(n = 1) %>% ungroup()
  }
  stopifnot(dplyr::n_distinct(v6_features[c("school_code","year")]) == nrow(v6_features))
  
  # Range checks
  for (cc in c("sped_rate","ell_rate","migrant_rate","foster_rate","homeless_rate","sed_rate","sex_male_rate","sex_female_rate","sex_non_binary_rate")) {
    if (cc %in% names(v6_features) && !rng_ok(v6_features[[cc]])) {
      warning("Rate outside [0,1] in ", cc, " — check subgroup selection/denoms.")
    }
  }
  
  # Write outputs
  dir.create(DATA_STAGE, showWarnings = FALSE, recursive = TRUE)
  write_parquet(v6_features, V6_FEAT_PARQ)
  write_parquet(oth_long,    V6_LONG_PARQ)
  message("Wrote:\n- ", V6_FEAT_PARQ, "\n- ", V6_LONG_PARQ)
} else {
  message(">>> Using existing ", V6_FEAT_PARQ)
  v6_features <- read_parquet(V6_FEAT_PARQ) |> clean_names()
}

# -------------------- (B) ANALYZE: SPED rate by Black quartile ----------------
  need <- c("black_prop_q_label","black_prop_q","black_share","is_traditional","sped_num","sped_den","sped_rate","school_code","year","school_type")
miss <- setdiff(need, names(v6_features))
if (length(miss)) stop("Missing required columns in v6_features: ", paste(miss, collapse = ", "))

trad <- v6_features$is_traditional %in% TRUE

message("Rows traditional: ", sum(trad, na.rm = TRUE), " / ", nrow(v6_features))

  message("Quartile distribution (raw labels, traditional):")
  print(table(v6_features$black_prop_q_label[trad], useNA = "always"))

  # Clean labels → Q1..Q4 only for primary analysis
  v6_clean <- v6_features %>%
    filter(
      is_traditional %in% TRUE,
      !is.na(black_prop_q),
      !is.na(sped_rate), !is.na(sped_den), sped_den > 0
    ) %>%
    mutate(black_prop_q_label = factor(black_prop_q_label,
                                       levels = paste0("Q",1:4),
                                       ordered = TRUE))

# Sample size + enrollment distribution
total_trad <- sum(trad, na.rm = TRUE)
final_n    <- nrow(v6_clean)
message("Analysis sample: ", final_n, " of ", total_trad, " traditional rows (",
        ifelse(total_trad > 0, round(100*final_n/total_trad, 1), NA), "%)")

  enrollment_summary <- v6_clean %>%
    group_by(black_prop_q_label) %>%
  summarise(
    min_sped_den    = min(sped_den, na.rm = TRUE),
    q25_sped_den    = quantile(sped_den, 0.25, na.rm = TRUE),
    median_sped_den = median(sped_den, na.rm = TRUE),
    q75_sped_den    = quantile(sped_den, 0.75, na.rm = TRUE),
    max_sped_den    = max(sped_den, na.rm = TRUE),
    .groups = "drop"
  )

# Weighted (pooled) with Wilson CI
  plot_df_weighted <- v6_clean %>%
    group_by(black_prop_q_label) %>%
  summarise(
    n_schools = n(),
    events    = sum(sped_num, na.rm = TRUE),
    denom     = sum(sped_den, na.rm = TRUE),
    weighted_rate   = safe_div(events, denom),
    unweighted_rate = mean(sped_rate, na.rm = TRUE),
    median_rate     = median(sped_rate, na.rm = TRUE),
    sd_rate         = sd(sped_rate, na.rm = TRUE),
    median_enrollment = median(sped_den, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    z = 1.96, p_hat = weighted_rate, n_eff = denom,
    ci_low  = pmax(0, (p_hat + z^2/(2*n_eff) - z*sqrt(p_hat*(1-p_hat)/n_eff + z^2/(4*n_eff^2))) / (1 + z^2/n_eff)),
    ci_high = pmin(1, (p_hat + z^2/(2*n_eff) + z*sqrt(p_hat*(1-p_hat)/n_eff + z^2/(4*n_eff^2))) / (1 + z^2/n_eff))
  )

# Unweighted (mean-of-schools) with t-based CI
  plot_df_unweighted <- v6_clean %>%
    group_by(black_prop_q_label) %>%
  summarise(
    n_schools  = n(),
    mean_rate  = mean(sped_rate, na.rm = TRUE),
    sd_rate    = sd(sped_rate,   na.rm = TRUE),
    se_rate    = sd_rate / sqrt(n_schools),
    tcrit      = qt(0.975, df = pmax(n_schools - 1, 1)),
    ci_low     = pmax(0, mean_rate - tcrit * se_rate),
    ci_high    = pmin(1, mean_rate + tcrit * se_rate),
    median_rate = median(sped_rate, na.rm = TRUE),
    .groups = "drop"
  )

# Comparison message
if (nrow(plot_df_weighted)) {
  message("Weighted vs Unweighted (rate, pp diff):")
    print(plot_df_weighted %>%
            select(black_prop_q_label, weighted_rate, unweighted_rate) %>%
          mutate(diff = weighted_rate - unweighted_rate,
                 pct_diff = 100*diff/pmax(unweighted_rate, .Machine$double.eps)))
}

# Robust captions
  sizes_unw <- if (nrow(plot_df_unweighted)) plot_df_unweighted %>%
    mutate(kv = paste0(as.character(black_prop_q_label), "=", n_schools)) %>% pull(kv) %>% paste(collapse = ", ") else "No data"
  sizes_w   <- if (nrow(plot_df_weighted)) plot_df_weighted %>%
    mutate(kv = paste0(as.character(black_prop_q_label), "=", n_schools)) %>% pull(kv) %>% paste(collapse = ", ") else "No data"

# Plots
  p_unweighted <- ggplot(plot_df_unweighted, aes(x = black_prop_q_label, y = mean_rate)) +
  geom_point(size = 3) +
  geom_errorbar(aes(ymin = ci_low, ymax = ci_high), width = 0.15) +
  scale_y_continuous(labels = percent_format(accuracy = 0.1)) +
  labs(
    x = "Quartile of Black Student Enrollment (Q1 = lowest share)",
    y = "Special Education Suspension Rate",
    title = "SPED Suspension Rate by Black Enrollment Quartile (Unweighted)",
    caption = paste0("Traditional schools only; unweighted school-level means with 95% CI.\n",
                     "Sample sizes: ", sizes_unw)
  ) +
  theme_minimal(base_size = 12)

  p_weighted <- ggplot(plot_df_weighted, aes(x = black_prop_q_label, y = weighted_rate)) +
  geom_point(size = 3) +
  geom_errorbar(aes(ymin = ci_low, ymax = ci_high), width = 0.15) +
  scale_y_continuous(labels = percent_format(accuracy = 0.1)) +
  labs(
    x = "Quartile of Black Student Enrollment (Q1 = lowest share)",
    y = "Special Education Suspension Rate",
    title = "SPED Suspension Rate by Black Enrollment Quartile (Enrollment-Weighted)",
    caption = paste0("Traditional schools only; pooled rate with Wilson 95% CI.\n",
                     "Sample sizes: ", sizes_w)
  ) +
  theme_minimal(base_size = 12)

# -------------------- (C) OUTPUTS ---------------------------------------------
dir.create(here("outputs"), showWarnings = FALSE)

ggsave(here("outputs","21_sped_rate_by_black_quartile_unweighted.png"),
       p_unweighted, width = 8, height = 5.2, dpi = 300)
ggsave(here("outputs","21_sped_rate_by_black_quartile_weighted.png"),
       p_weighted, width = 8, height = 5.2, dpi = 300)

# Exclusions breakdown (why rows dropped)
excluded_schools <- v6_features %>%
  filter(is_traditional %in% TRUE) %>%
  mutate(
    included = school_code %in% v6_clean$school_code & year %in% v6_clean$year,
    quartile_status = case_when(
      is.na(black_prop_q_label) ~ "Unknown quartile",
      is.na(sped_rate) ~ "Missing SPED rate",
      is.na(sped_den) | sped_den == 0 ~ "No SPED enrollment",
      TRUE ~ "Included"
    )
  )

# ==================== Excel Workbook with all outputs ====================

wb <- createWorkbook()

# --- Add summaries ---
addWorksheet(wb, "unweighted_summary")
writeData(wb, "unweighted_summary",
          plot_df_unweighted %>% 
            mutate(across(c(mean_rate, ci_low, ci_high, median_rate),
                          ~ percent(.x, accuracy = 0.001))))

addWorksheet(wb, "weighted_summary")
writeData(wb, "weighted_summary",
          plot_df_weighted %>% 
            mutate(across(c(weighted_rate, unweighted_rate, median_rate, ci_low, ci_high),
                          ~ percent(.x, accuracy = 0.001))))

# --- School-level data (what each school looks like in the final sample) ---
addWorksheet(wb, "school_level")
writeData(wb, "school_level",
          v6_clean %>%
            transmute(
              school_code, year,
              black_share    = percent(black_share, accuracy = 0.1),
              black_quartile = as.character(black_prop_q_label),
              sped_rate      = percent(sped_rate, accuracy = 0.1),
              sped_enrollment = sped_den,
              school_type
            ))

# --- Enrollment distribution by quartile ---
addWorksheet(wb, "enrollment_distribution")
writeData(wb, "enrollment_distribution", enrollment_summary)

# --- Exclusion diagnostics (why rows were dropped) ---
addWorksheet(wb, "exclusions")
writeData(wb, "exclusions",
          excluded_schools %>% 
            count(quartile_status) %>%
            arrange(desc(n)))

# --- Raw quartile distribution (before filtering) ---
addWorksheet(wb, "quartile_distribution")
writeData(wb, "quartile_distribution",
          v6_features %>%
            filter(is_traditional) %>%
            count(black_prop_q_label, name = "n"))

# --- Optionally: Save key plots as images and embed links ---
# Export plots as PNG
ggsave(here("outputs","22_sped_rate_by_black_quartile_unweighted.png"),
       p_unweighted, width = 8, height = 5.2, dpi = 300)
ggsave(here("outputs","22_sped_rate_by_black_quartile_weighted.png"),
       p_weighted, width = 8, height = 5.2, dpi = 300)

# Save workbook
saveWorkbook(wb, here("outputs","22_sped_rate_by_black_quartile_FULL.xlsx"), overwrite = TRUE)

message("Done. PNGs + Excel saved to 'outputs/'.")
