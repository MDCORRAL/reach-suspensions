# analysis/04_rates_by_size_quartile_and_race.R
suppressPackageStartupMessages({
  library(here); library(arrow); library(dplyr); library(tidyr)
  library(stringr); library(ggplot2); library(scales); library(ggrepel); library(readr)
})

source(here::here("R", "utils_keys_filters.R"))
# ======= knobs =======
LABEL_EVERY   <- 1    # label every 1 year (all points). Try 2 for every other year.
LABEL_SIZE_Q  <- 2  # label size in View A (facet by race)
LABEL_SIZE_R  <- 2  # label size in View B (facet by quartile)
NUDGE_X       <- 0.22 # push labels slightly to the right
HALO_ALPHA    <- 0.2 # label box fill transparency (0=clear, 1=opaque)

# Optionally subset races for clarity (set to NULL to include all)
RACE_SET <- NULL  # e.g., c("All Students","Hispanic/Latino","Black/African American","White","Asian")

# ======= data load & guards =======
v6 <- read_parquet(here("data-stage","susp_v6_long.parquet"))

need <- c("subgroup","academic_year","total_suspensions","cumulative_enrollment","enroll_q_label")
miss <- setdiff(need, names(v6))
if (length(miss)) stop("Missing columns in v6: ", paste(miss, collapse=", "),
                       "\nRe-run 03_feature_size_quartiles_TA.R and downstream.")

# Year order
year_levels <- v6 %>% filter(category_type == "Race/Ethnicity", subgroup == "All Students") %>%
  distinct(academic_year) %>% arrange(academic_year) %>% pull(academic_year)

# Enrollment quartiles of interest
q_keep <- c("Q1 (Smallest)","Q4 (Largest)")

# Color/shape (Okabe–Ito palette for accessibility)
pal_quart <- c("Q1 (Smallest)"="#0072B2", "Q4 (Largest)"="#D55E00")
shape_quart <- c("Q1 (Smallest)"=16, "Q4 (Largest)"=17)  # circle vs triangle

# Race code map provided by canon_race_label() helper (RD -> "Not Reported");
# "Not Reported" is filtered out of plots.

# ======= aggregate: pooled rates by year × quartile × race =======
# Total (All Students) from TA
df_total <- v6 %>%
  filter(category_type == "Race/Ethnicity", subgroup == "All Students", !is.na(enroll_q_label), enroll_q_label %in% q_keep) %>%
  group_by(academic_year, enroll_q_label) %>%
  summarise(
    susp   = sum(total_suspensions, na.rm=TRUE),
    enroll = sum(cumulative_enrollment, na.rm=TRUE),
    .groups="drop"
  ) %>%
  mutate(
    rate = if_else(enroll>0, susp/enroll, NA_real_),
    race = "All Students"
  )

# Races
df_race <- v6 %>%
  mutate(race = canon_race_label(subgroup)) %>%
  filter(
    race %in% setdiff(ALLOWED_RACES, "All Students"),
    !is.na(enroll_q_label), enroll_q_label %in% q_keep
  ) %>%
  group_by(academic_year, enroll_q_label, race) %>%
  summarise(
    susp   = sum(total_suspensions, na.rm=TRUE),
    enroll = sum(cumulative_enrollment, na.rm=TRUE),
    .groups="drop"
  ) %>%
  mutate(rate = if_else(enroll>0, susp/enroll, NA_real_))

# Combine (race + total)
df_all <- bind_rows(df_total, df_race) %>%
  mutate(
    year_fct = factor(academic_year, levels = year_levels),
    year_idx = as.integer(factor(academic_year, levels = year_levels))
  )

# Optional race subset
if (!is.null(RACE_SET)) df_all <- df_all %>% filter(race %in% RACE_SET)

# Order races (stable facets)
race_levels <- df_all %>% distinct(race) %>% arrange(race) %>% pull(race)
df_all <- df_all %>% mutate(race = factor(race, levels = race_levels))

# Helpers: label frames, thinned to every LABEL_EVERY year
labels_A <- df_all %>%
  filter(!is.na(rate), (year_idx - min(year_idx)) %% LABEL_EVERY == 0)
labels_B <- labels_A

# ======= View A: Facet by RACE (lines = Q1 vs Q4) =======
p_by_race_q14 <- ggplot(
  df_all,
  aes(x = year_fct, y = rate, color = enroll_q_label, shape = enroll_q_label, group = enroll_q_label)
) +
  geom_line(linewidth = 1.05) +
  geom_point(size = 2.1) +
  ggrepel::geom_label_repel(
    data = labels_A,
    aes(label = percent(rate, accuracy = 0.1)),
    size = LABEL_SIZE_Q, show.legend = FALSE,
    max.overlaps = Inf, direction = "y", nudge_x = NUDGE_X,
    box.padding = 0.16, point.padding = 0.16, min.segment.length = 0,
    label.size = 0, fill = alpha("white", HALO_ALPHA)
  ) +
  scale_color_manual(values = pal_quart, name = "School size quartile") +
  scale_shape_manual(values = shape_quart, name = "School size quartile") +
  scale_y_continuous(labels = percent_format(accuracy = 0.1), limits = c(0, NA),
                     expand = expansion(mult = c(0, 0.14))) +
  scale_x_discrete(expand = expansion(mult = c(0.02, 0.28))) +  # extra right margin for labels
  labs(
    title = "Suspension Rates by Race and School Size (Q1 Smallest vs Q4 Largest)",
    subtitle = "Pooled rate: total suspensions ÷ total enrollment within year × quartile × race",
    x = NULL, y = "Suspensions per student (%)"
  ) +
  facet_wrap(~ race, scales = "free_y") +
  theme_minimal(base_size = 12) +
  theme(
    axis.text.x = element_text(angle = 35, hjust = 1),
    legend.position = "bottom",
    plot.margin = margin(12, 28, 12, 12) # a bit of breathing room on the right
  )

# ======= View B: Facet by QUARTILE (lines = races) =======
race_palette <- setNames(scales::hue_pal()(length(race_levels)), race_levels)

p_by_quart_races <- ggplot(
  df_all,
  aes(x = year_fct, y = rate, color = race, group = race)
) +
  geom_line(linewidth = 1.05) +
  geom_point(size = 1.9) +
  ggrepel::geom_label_repel(
    data = labels_B,
    aes(label = percent(rate, accuracy = 0.1), color = race),
    size = LABEL_SIZE_R, show.legend = FALSE,
    max.overlaps = Inf, direction = "y", nudge_x = NUDGE_X,
    box.padding = 0.14, point.padding = 0.14, min.segment.length = 0,
    label.size = 0, fill = alpha("white", HALO_ALPHA)
  ) +
  scale_color_manual(values = race_palette, name = "Race/Ethnicity") +
  scale_y_continuous(labels = percent_format(accuracy = 0.1), limits = c(0, NA),
                     expand = expansion(mult = c(0, 0.14))) +
  scale_x_discrete(expand = expansion(mult = c(0.02, 0.28))) +
  labs(
    title = "Suspension Rates by School Size Quartile and Race",
    subtitle = "Pooled rate: total suspensions ÷ total enrollment within year × quartile × race",
    x = NULL, y = "Suspensions per student (%)"
  ) +
  facet_wrap(~ enroll_q_label, nrow = 1) +
  theme_minimal(base_size = 12) +
  theme(
    axis.text.x = element_text(angle = 35, hjust = 1),
    legend.position = "bottom",
    plot.margin = margin(12, 28, 12, 12)
  )

# ======= Summary table: average across years, plus Q4–Q1 gap =======
summary_tbl <- df_all %>%
  group_by(race, enroll_q_label) %>%
  summarise(avg_rate = mean(rate, na.rm = TRUE), .groups = "drop") %>%
  mutate(avg_rate_pct = percent(avg_rate, accuracy = 0.01)) %>%
  select(race, enroll_q_label, avg_rate_pct) %>%
  pivot_wider(names_from = enroll_q_label, values_from = avg_rate_pct) %>%
  left_join(
    df_all %>% group_by(race, enroll_q_label) %>% summarise(avg_rate = mean(rate, na.rm=TRUE), .groups="drop") %>%
      pivot_wider(names_from = enroll_q_label, values_from = avg_rate) %>%
      mutate(`Q4–Q1 gap` = scales::percent(`Q4 (Largest)` - `Q1 (Smallest)`, accuracy = 0.01)) %>%
      select(race, `Q4–Q1 gap`),
    by = "race"
  ) %>%
  arrange(race)

# ======= Print & Save =======
print(p_by_race_q14)
print(p_by_quart_races)

cat("\n=== Average suspension rate across years (by race & size quartile) ===\n")
print(summary_tbl, n = nrow(summary_tbl))

dir.create(here("outputs"), showWarnings = FALSE)
ggsave(here("outputs","rates_by_race_faceted_q1vQ4_labeled.png"), p_by_race_q14,
       width = 12, height = 8, dpi = 300, bg = "white")
ggsave(here("outputs","rates_by_quartile_faceted_races_labeled.png"), p_by_quart_races,
       width = 12, height = 7, dpi = 300, bg = "white")
write_csv(summary_tbl, here("outputs","summary_rates_by_race_quartile.csv"))
