# analysis/10_eda_hotspots_and_trends.R
# Exploratory scan for: (1) where disparities are largest, (2) where rates changed the most
# across levels/locales + reason categories (no graphs, tables only).

suppressPackageStartupMessages({
  library(here); library(arrow); library(dplyr); library(tidyr)
  library(stringr); library(readr); library(scales)
})

v5 <- arrow::read_parquet(here("data-stage","susp_v5.parquet"))

# ---- guards ------------------------------------------------------------------
need <- c("academic_year","reporting_category","total_suspensions",
          "cumulative_enrollment","locale_simple","level_strict3")
miss <- setdiff(need, names(v5))
if (length(miss)) stop("Missing columns in v5: ", paste(miss, collapse=", "))

years <- v5 %>% filter(reporting_category=="TA") %>%
  distinct(academic_year) %>% arrange(academic_year) %>% pull()
if (!length(years)) stop("No TA rows to anchor year order.")
first_year <- min(years); last_year <- max(years)

# ---- helpers -----------------------------------------------------------------
race_label <- function(code) dplyr::recode(
  code,
  RB="Black/African American", RW="White", RH="Hispanic/Latino", RL="Hispanic/Latino",
  RI="American Indian/Alaska Native", RA="Asian", RF="Filipino",
  RP="Pacific Islander", RT="Two or More Races", TA="All Students",
  .default = NA_character_
)
allowed_codes <- c("TA","RB","RW","RH","RL","RI","RA","RF","RP","RT")

safe_rate <- function(susp, enroll) ifelse(enroll>0, susp/enroll, NA_real_)

# Reason columns already in v5 as proportions of total suspensions
reason_cols <- names(v5)[grepl("^prop_susp_", names(v5))]
reason_map <- c(
  prop_susp_violent_injury     = "Violent (Injury)",
  prop_susp_violent_no_injury  = "Violent (No Injury)",
  prop_susp_weapons_possession = "Weapons",
  prop_susp_illicit_drug       = "Illicit Drug",
  prop_susp_defiance_only      = "Willful Defiance",
  prop_susp_other_reasons      = "Other"
)

# ---- 1) BASE AGGREGATES: rates by Level × Locale × Race × Year --------------
base <- v5 %>%
  filter(reporting_category %in% allowed_codes) %>%
  mutate(race = race_label(reporting_category)) %>%
  # keep All Students and named races; drop RD/NA
  filter(!is.na(race))

# TA denominator & total susp by setting-year (level × locale)
ta_by_set_year <- base %>%
  filter(race=="All Students") %>%
  group_by(level_strict3, locale_simple, academic_year) %>%
  summarise(
    total_susp_TA = sum(total_suspensions, na.rm = TRUE),
    enroll_TA     = sum(cumulative_enrollment, na.rm = TRUE),
    rate_TA       = safe_rate(total_susp_TA, enroll_TA),
    .groups="drop"
  )

# Race-specific pooled rates by setting-year
race_rates <- base %>%
  group_by(level_strict3, locale_simple, academic_year, race) %>%
  summarise(
    susp   = sum(total_suspensions, na.rm = TRUE),
    enroll = sum(cumulative_enrollment, na.rm = TRUE),
    rate   = safe_rate(susp, enroll),
    .groups="drop"
  ) %>%
  left_join(ta_by_set_year, by = c("level_strict3","locale_simple","academic_year")) %>%
  mutate(disparity_ratio_vs_all = ifelse(!is.na(rate_TA) & rate_TA>0, rate / rate_TA, NA_real_))

# ---- 2) WHERE ARE DISPARITIES LARGEST? ---------------------------------------
# (a) Within each setting-year, spread across races (max-min, excl. All Students)
spread_by_year <- race_rates %>%
  filter(race != "All Students") %>%
  group_by(level_strict3, locale_simple, academic_year) %>%
  summarise(
    max_rate = max(rate, na.rm = TRUE),
    min_rate = min(rate, na.rm = TRUE),
    spread_abs = ifelse(is.finite(max_rate-min_rate), max_rate-min_rate, NA_real_),
    spread_ratio = ifelse(is.finite(max_rate/min_rate) & min_rate>0, max_rate/min_rate, NA_real_),
    .groups="drop"
  )

# Rank settings by average spread across years
spread_rank <- spread_by_year %>%
  group_by(level_strict3, locale_simple) %>%
  summarise(
    years_n     = sum(!is.na(spread_abs)),
    avg_spread  = mean(spread_abs, na.rm = TRUE),
    avg_ratio   = mean(spread_ratio, na.rm = TRUE),
    latest_spread = spread_abs[which.max(academic_year)],  # last chronologically
    .groups="drop"
  ) %>%
  arrange(desc(avg_spread))

# (b) Which race deviates most from All Students (avg disparity ratio)
disp_vs_all_rank <- race_rates %>%
  filter(race != "All Students") %>%
  group_by(level_strict3, locale_simple, race) %>%
  summarise(
    years_n = sum(!is.na(disparity_ratio_vs_all)),
    avg_ratio_vs_all = mean(disparity_ratio_vs_all, na.rm = TRUE),
    latest_ratio_vs_all = disparity_ratio_vs_all[which.max(academic_year)],
    .groups="drop"
  ) %>%
  arrange(desc(avg_ratio_vs_all))

# ---- 3) WHICH SETTINGS CHANGED MOST OVER TIME? -------------------------------
# (a) Total suspension rate (All Students) change earliest->latest
total_changes <- ta_by_set_year %>%
  group_by(level_strict3, locale_simple) %>%
  arrange(academic_year, .by_group = TRUE) %>%
  summarise(
    start_year = first(academic_year), end_year = last(academic_year),
    start_rate = first(rate_TA),       end_rate = last(rate_TA),
    abs_change = end_rate - start_rate,
    pct_change = ifelse(is.finite(start_rate) & start_rate>0,
                        (end_rate - start_rate)/start_rate, NA_real_),
    .groups="drop"
  ) %>%
  mutate(
    direction = case_when(
      is.na(abs_change) ~ NA_character_,
      abs_change > 0 ~ "Increase",
      abs_change < 0 ~ "Decrease",
      TRUE ~ "No change"
    )
  ) %>%
  arrange(desc(abs(abs_change)))

# (b) Reason-specific rates: compute reason counts from shares, then rate by denom=TA enrollment
# compute reason_count = prop * total_suspensions at row level, sum over races
reason_long <- base %>%
  filter(race != "All Students") %>%
  select(level_strict3, locale_simple, academic_year, total_suspensions, all_of(reason_cols)) %>%
  pivot_longer(all_of(reason_cols), names_to = "reason_key", values_to = "prop") %>%
  mutate(reason_count = ifelse(!is.na(prop) & !is.na(total_suspensions),
                               prop * total_suspensions, 0))

reason_by_set_year <- reason_long %>%
  group_by(level_strict3, locale_simple, academic_year, reason_key) %>%
  summarise(total_reason = sum(reason_count, na.rm = TRUE), .groups="drop") %>%
  left_join(ta_by_set_year, by = c("level_strict3","locale_simple","academic_year")) %>%
  mutate(
    reason_rate = safe_rate(total_reason, enroll_TA),
    reason = recode(reason_key, !!!reason_map)
  )

reason_changes <- reason_by_set_year %>%
  group_by(level_strict3, locale_simple, reason) %>%
  arrange(academic_year, .by_group = TRUE) %>%
  summarise(
    start_year = first(academic_year), end_year = last(academic_year),
    start_rate = first(reason_rate),   end_rate = last(reason_rate),
    abs_change = end_rate - start_rate,
    pct_change = ifelse(is.finite(start_rate) & start_rate>0,
                        (end_rate - start_rate)/start_rate, NA_real_),
    .groups="drop"
  ) %>%
  mutate(direction = case_when(
    is.na(abs_change) ~ NA_character_,
    abs_change > 0 ~ "Increase",
    abs_change < 0 ~ "Decrease",
    TRUE ~ "No change"
  )) %>%
  arrange(desc(abs(abs_change)))

# ---- 4) CONSOLE SNAPSHOTS ----------------------------------------------------
cat("\n=== A) Settings with largest average racial spread (rate max - min) ===\n")
print(
  spread_rank %>%
    mutate(avg_spread_pct = percent(avg_spread, 0.01),
           latest_spread_pct = percent(latest_spread, 0.01)) %>%
    select(level_strict3, locale_simple, years_n, avg_spread_pct, latest_spread_pct) %>%
    head(15),
  n = 15
)

cat("\n=== B) Races deviating most from All Students (avg ratio > 1 == higher) ===\n")
print(
  disp_vs_all_rank %>%
    mutate(avg_ratio_vs_all = round(avg_ratio_vs_all, 2),
           latest_ratio_vs_all = round(latest_ratio_vs_all, 2)) %>%
    head(20),
  n = 20
)

cat("\n=== C) Biggest changes in TOTAL suspension rate (All Students) ===\n")
print(
  total_changes %>%
    mutate(
      start = percent(start_rate, 0.01),
      end   = percent(end_rate, 0.01),
      abs   = percent(abs_change, 0.01),
      pct   = percent(pct_change, 0.1)
    ) %>%
    select(level_strict3, locale_simple, start_year, end_year, start, end, abs, pct, direction) %>%
    head(20),
  n = 20
)

cat("\n=== D) Biggest changes in REASON-SPECIFIC rates (per student) ===\n")
print(
  reason_changes %>%
    mutate(
      start = percent(start_rate, 0.01),
      end   = percent(end_rate, 0.01),
      abs   = percent(abs_change, 0.01),
      pct   = percent(pct_change, 0.1)
    ) %>%
    select(level_strict3, locale_simple, reason, start_year, end_year, start, end, abs, pct, direction) %>%
    head(30),
  n = 30
)

cat("\n(i) Note: spreads/ratios aggregate over all reported races (excl. 'All Students').\n",
    "(ii) Reason rates use TA enrollment as the denominator and reason counts reconstructed from row-level shares.\n",
    "(iii) Year coverage is ", first_year, "–", last_year, " (skipping missing state years, if any).\n", sep="")

# ---- 5) WRITE CSVs -----------------------------------------------------------
outdir <- here("outputs"); dir.create(outdir, showWarnings = FALSE)

readr::write_csv(spread_rank,
                 file.path(outdir, "EDA_spread_by_level_locale_rank.csv"))
readr::write_csv(disp_vs_all_rank,
                 file.path(outdir, "EDA_disparity_vs_AllStudents_by_race_rank.csv"))
readr::write_csv(total_changes,
                 file.path(outdir, "EDA_total_rate_changes_by_level_locale.csv"))
readr::write_csv(reason_changes,
                 file.path(outdir, "EDA_reason_rate_changes_by_level_locale.csv"))

cat("\n✓ CSVs written to ", outdir, "\n", sep = "")