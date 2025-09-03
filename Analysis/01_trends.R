# analysis/01_trends_rates.R
# analysis/01_trends_rates_labeled.R
suppressPackageStartupMessages({
  library(here); library(arrow); library(dplyr); library(tidyr)
  library(stringr); library(ggplot2); library(scales); library(ggrepel)
})

# ----- knobs you can tweak -----
LABEL_SIZE_TOTAL   <- 3.0
LABEL_SIZE_RACE    <- 2.6
LABEL_SIZE_REASON  <- 2.8
NUDGE_X_LABELS     <- 0.15  # push labels a hair to the right of points

# ============ Load ============
v5 <- read_parquet(here("data-stage", "susp_v5.parquet"))

# Year order from TA rows
year_levels <- v5 %>%
  filter(reporting_category == "TA") %>%
  distinct(academic_year) %>% arrange(academic_year) %>% pull(academic_year)

# ============ Race codes / labels ============
# Include RH (Hispanic/Latino) & RI (Am Indian/Alaska Native); exclude RD (not reported).
allowed_codes <- c("RB","RW","RH","RI","RA","RF","RP","RT","RL")  # RL alias folds into RH
race_label <- function(code) dplyr::recode(
  code,
  RB = "Black/African American",
  RW = "White",
  RH = "Hispanic/Latino",
  RL = "Hispanic/Latino",  # alias
  RI = "American Indian/Alaska Native",
  RA = "Asian",
  RF = "Filipino",
  RP = "Pacific Islander",
  RT = "Two or More Races",
  .default = NA_character_
)

# ============ Plot 1: Suspension RATES (All vs Race) ============
# Rate = total_suspensions / cumulative_enrollment
overall_rate_by_year <- v5 %>%
  filter(reporting_category == "TA") %>%
  group_by(academic_year) %>%
  summarise(
    susp   = sum(total_suspensions, na.rm = TRUE),
    enroll = sum(cumulative_enrollment, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(rate = if_else(enroll > 0, susp / enroll, NA_real_),
         race = "All Students",
         year_fct = factor(academic_year, levels = year_levels))

race_rate_by_year <- v5 %>%
  filter(reporting_category %in% allowed_codes) %>%
  mutate(race = race_label(reporting_category)) %>%
  filter(!is.na(race)) %>%
  group_by(academic_year, race) %>%
  summarise(
    susp   = sum(total_suspensions, na.rm = TRUE),
    enroll = sum(cumulative_enrollment, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(rate = if_else(enroll > 0, susp / enroll, NA_real_),
         year_fct = factor(academic_year, levels = year_levels))

# Palette: black for total + distinct hues for races
race_levels <- sort(unique(race_rate_by_year$race))
pal <- c("All Students" = "black",
         setNames(scales::hue_pal()(length(race_levels)), race_levels))

# Label frames (drop NAs so we don't label missing)
labels_total <- overall_rate_by_year %>% filter(!is.na(rate))
labels_race  <- race_rate_by_year   %>% filter(!is.na(rate))

p_rate_totals <- ggplot() +
  # Total line/points
  geom_line(
    data = overall_rate_by_year,
    aes(x = year_fct, y = rate, color = "All Students", group = 1),
    linewidth = 1.3
  ) +
  geom_point(
    data = overall_rate_by_year,
    aes(x = year_fct, y = rate, color = "All Students"),
    size = 2.2
  ) +
  # Labels: every year for total
  ggrepel::geom_text_repel(
    data = labels_total,
    aes(x = year_fct, y = rate, label = percent(rate, accuracy = 0.1), color = "All Students"),
    size = LABEL_SIZE_TOTAL, show.legend = FALSE,
    max.overlaps = Inf, direction = "y", nudge_x = NUDGE_X_LABELS,
    box.padding = 0.15, point.padding = 0.15, min.segment.length = 0
  ) +
  # Race lines/points
  geom_line(
    data = race_rate_by_year,
    aes(x = year_fct, y = rate, color = race, group = race),
    linewidth = 1
  ) +
  geom_point(
    data = race_rate_by_year,
    aes(x = year_fct, y = rate, color = race),
    size = 1.6
  ) +
  # Labels: every year for each race
  ggrepel::geom_text_repel(
    data = labels_race,
    aes(x = year_fct, y = rate, label = percent(rate, accuracy = 0.1), color = race),
    size = LABEL_SIZE_RACE, show.legend = FALSE,
    max.overlaps = Inf, direction = "y", nudge_x = NUDGE_X_LABELS,
    box.padding = 0.15, point.padding = 0.15, min.segment.length = 0
  ) +
  scale_color_manual(values = pal, breaks = c("All Students", race_levels)) +
  scale_y_continuous(labels = percent_format(accuracy = 0.1), limits = c(0, NA),
                     expand = expansion(mult = c(0, 0.15))) +
  scale_x_discrete(expand = expansion(mult = c(0.02, 0.25))) +
  labs(
    title = "Suspension Rates by Year: All Students and by Race/Ethnicity",
    subtitle = "Suspension events per enrolled student (annual rate)",
    x = NULL, y = "Suspensions per student (%)",
    color = NULL,
    caption = "Rate = total suspensions ÷ enrollment. RH=Hispanic/Latino; RI=American Indian/Alaska Native; RD excluded."
  ) +
  theme_minimal(base_size = 12) +
  theme(axis.text.x = element_text(angle = 35, hjust = 1))

# ============ Plot 2: Reason MIX (SHARE of suspensions) ============
# Share = suspensions in category ÷ total suspensions (TA) that year
ta_susp_by_year <- v5 %>%
  filter(reporting_category == "TA") %>%
  group_by(academic_year) %>%
  summarise(total_susp_all = sum(total_suspensions, na.rm = TRUE), .groups = "drop")

v5_race <- v5 %>% filter(reporting_category != "TA")
reason_cols <- names(v5_race)[grepl("^prop_susp_", names(v5_race))]

reason_share_by_year <- v5_race %>%
  select(academic_year, total_suspensions, all_of(reason_cols)) %>%
  pivot_longer(all_of(reason_cols), names_to = "reason", values_to = "prop") %>%
  mutate(reason_count = prop * total_suspensions) %>%
  group_by(academic_year, reason) %>%
  summarise(total_reason_susp = sum(reason_count, na.rm = TRUE), .groups = "drop") %>%
  left_join(ta_susp_by_year, by = "academic_year") %>%
  mutate(
    share = if_else(total_susp_all > 0, total_reason_susp / total_susp_all, NA_real_),
    reason_lab = recode(sub("^prop_susp_", "", reason),
                        "violent_injury"     = "Violent (Injury)",
                        "violent_no_injury"  = "Violent (No Injury)",
                        "weapons_possession" = "Weapons",
                        "illicit_drug"       = "Illicit Drug",
                        "defiance_only"      = "Willful Defiance",
                        "other_reasons"      = "Other",
                        .default = reason),
    year_fct = factor(academic_year, levels = year_levels)
  )

reason_colors <- c(
  "Willful Defiance"   = "#d62728",
  "Violent (Injury)"   = "#ff7f0e",
  "Violent (No Injury)"= "#2ca02c",
  "Weapons"            = "#1f77b4",
  "Illicit Drug"       = "#9467bd",
  "Other"              = "#8c564b"
)

reason_labels_all <- reason_share_by_year %>% filter(!is.na(share))

p_reason_share <- ggplot(reason_share_by_year,
                         aes(x = year_fct, y = share, color = reason_lab, group = reason_lab)) +
  geom_line(linewidth = 1.2, alpha = 0.9) +
  geom_point(size = 2) +
  # Labels: every year for each reason
  ggrepel::geom_text_repel(
    data = reason_labels_all,
    aes(label = percent(share, accuracy = 0.1)),
    size = LABEL_SIZE_REASON, show.legend = FALSE,
    max.overlaps = Inf, direction = "y", nudge_x = NUDGE_X_LABELS,
    box.padding = 0.15, point.padding = 0.15, min.segment.length = 0
  ) +
  scale_color_manual(values = reason_colors) +
  scale_y_continuous(labels = percent_format(accuracy = 0.1), limits = c(0, NA),
                     expand = expansion(mult = c(0, 0.15))) +
  scale_x_discrete(expand = expansion(mult = c(0.02, 0.25))) +
  labs(
    title = "Mix of Suspension Reasons Over Time",
    subtitle = "Share of all suspensions by category, each year",
    x = NULL, y = "Share of suspensions",
    color = "Reason",
    caption = "Share = suspensions in category ÷ total suspensions (TA) for that year."
  ) +
  theme_minimal(base_size = 12) +
  theme(axis.text.x = element_text(angle = 35, hjust = 1),
        legend.position = "none")

# ============ Print & Save ============
print(p_rate_totals)
print(p_reason_share)

dir.create(here("outputs"), showWarnings = FALSE)
ggsave(here("outputs", "rate_total_overall_and_race_labeled.png"), p_rate_totals,
       width = 12, height = 7, dpi = 300, bg = "white")
ggsave(here("outputs", "share_by_reason_over_time_labeled.png"),  p_reason_share,
       width = 12, height = 7, dpi = 300, bg = "white")
