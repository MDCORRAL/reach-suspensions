# analysis/01_trends.R  (use the same for ..._labeled.R)
suppressPackageStartupMessages({
  library(here); library(arrow); library(dplyr); library(tidyr)
  library(stringr); library(ggplot2); library(scales); library(ggrepel)
})

# Pin the project root using the REAL path (note the capital A in Analysis)
try(here::i_am("Analysis/01_trends.R"), silent = TRUE)

cat("Project root -> ", here::here(), "\n")

# Load utils; this must exist at <project>/R/utils_keys_filters.R
utils_path <- here::here("R", "utils_keys_filters.R")
cat("Loading utils from -> ", utils_path, "\n")
stopifnot(file.exists(utils_path))
source(utils_path)

# 0) Keys & filters
source(here::here("R","utils_keys_filters.R"))

# ----- knobs you can tweak -----
LABEL_SIZE_TOTAL   <- 3.0
LABEL_SIZE_RACE    <- 2.6
LABEL_SIZE_REASON  <- 2.8
NUDGE_X_LABELS     <- 0.15

# ============ Load ============
v6_raw <- read_parquet(here("data-stage", "susp_v6_long.parquet")) |> build_keys()

# Make this robust whether or not aggregate_level exists in v6
v6 <- {
  if ("aggregate_level" %in% names(v6_raw)) {
    v6_raw |> filter_campus_only()
  } else {
    # fall back: drop special 0000000/0000001 if present
    v6_raw |> filter(!school_code %in% SPECIAL_SCHOOL_CODES)
  }
}

# sanity: TA should be unique per campus-year (the long table itself is not)
ta_dups <- v6 |> filter(category_type == "Race/Ethnicity", subgroup == "All Students") |>
  count(cds_school, academic_year, name = "n") |> filter(n > 1)
if (nrow(ta_dups) > 0) {
  stop("TA not unique per campus-year in v6. Examples:\n",
       paste(utils::capture.output(print(head(ta_dups, 10))), collapse = "\n"))
}

# Year order from TA rows
year_levels <- v6 |>
  filter(category_type == "Race/Ethnicity", subgroup == "All Students") |>
  distinct(academic_year) |>
  arrange(academic_year) |>
  pull(academic_year)

# ============ Plot 1: Suspension RATES (All vs Race) ============
# Statewide rate = sum(suspensions) / sum(enrollment) from campus-only rows
overall_rate_by_year <- v6 |>
  filter(category_type == "Race/Ethnicity", subgroup == "All Students") |>
  group_by(academic_year) |>
  summarise(
    susp   = sum(total_suspensions, na.rm = TRUE),
    enroll = sum(cumulative_enrollment, na.rm = TRUE),
    .groups = "drop"
  ) |>
  mutate(
    rate = if_else(enroll > 0, susp / enroll, NA_real_),
    race = "All Students",
    year_fct = factor(academic_year, levels = year_levels)
  )

race_rate_by_year <- v6 |>
  mutate(race = canon_race_label(subgroup)) |>
  filter(race %in% ALLOWED_RACES) |>
  group_by(academic_year, race) |>
  summarise(
    susp   = sum(total_suspensions, na.rm = TRUE),
    enroll = sum(cumulative_enrollment, na.rm = TRUE),
    .groups = "drop"
  ) |>
  mutate(
    rate = if_else(enroll > 0, susp / enroll, NA_real_),
    year_fct = factor(academic_year, levels = year_levels)
  )

race_levels <- sort(unique(race_rate_by_year$race))
pal <- c("All Students" = "black",
         setNames(scales::hue_pal()(length(race_levels)), race_levels))

labels_total <- overall_rate_by_year |> filter(!is.na(rate))
labels_race  <- race_rate_by_year   |> filter(!is.na(rate))

p_rate_totals <- ggplot() +
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
  ggrepel::geom_text_repel(
    data = labels_total,
    aes(x = year_fct, y = rate, label = percent(rate, accuracy = 0.1), color = "All Students"),
    size = LABEL_SIZE_TOTAL, show.legend = FALSE,
    max.overlaps = Inf, direction = "y", nudge_x = NUDGE_X_LABELS,
    box.padding = 0.15, point.padding = 0.15, min.segment.length = 0
  ) +
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
    caption = "Rate = total suspensions ÷ enrollment. RH=Hispanic/Latino; RI=American Indian/Alaska Native. 'Not Reported' omitted."

  ) +
  theme_minimal(base_size = 12) +
  theme(axis.text.x = element_text(angle = 35, hjust = 1))

# ============ Plot 2: Reason MIX (SHARE of suspensions) ============
# If prop columns are absent, skip gracefully.
has_prop_cols <- any(grepl("^prop_susp_", names(v6)))
if (has_prop_cols) {
  ta_susp_by_year <- v6 |>
    filter(category_type == "Race/Ethnicity", subgroup == "All Students") |>
    group_by(academic_year) |>
    summarise(total_susp_all = sum(total_suspensions, na.rm = TRUE), .groups = "drop")
  
  v6_race <- v6 |> filter(subgroup != "All Students")
  reason_cols <- names(v6_race)[grepl("^prop_susp_", names(v6_race))]
  
  reason_share_by_year <- v6_race |>
    select(academic_year, total_suspensions, all_of(reason_cols)) |>
    pivot_longer(all_of(reason_cols), names_to = "reason", values_to = "prop") |>
    mutate(
      reason = sub("^prop_susp_", "", reason),
      reason_count = prop * total_suspensions
    ) |>
    group_by(academic_year, reason) |>
    summarise(total_reason_susp = sum(reason_count, na.rm = TRUE), .groups = "drop") |>
    left_join(ta_susp_by_year, by = "academic_year") |>
    add_reason_label() |>
    mutate(
      share = if_else(total_susp_all > 0, total_reason_susp / total_susp_all, NA_real_),
      year_fct = factor(academic_year, levels = year_levels)
    )
  
  reason_colors <- pal_reason
  
  reason_labels_all <- reason_share_by_year |> filter(!is.na(share))
  
  p_reason_share <- ggplot(reason_share_by_year,
                           aes(x = year_fct, y = share, color = reason_lab, group = reason_lab)) +
    geom_line(linewidth = 1.2, alpha = 0.9) +
    geom_point(size = 2) +
    ggrepel::geom_text_repel(
      data = reason_labels_all,
      aes(label = percent(share, accuracy = 0.1)),
      size = LABEL_SIZE_REASON, show.legend = FALSE,
      max.overlaps = Inf, direction = "y", nudge_x = NUDGE_X_LABELS,
      box.padding = 0.15, point.padding = 0.15, min.segment.length = 0
    ) +
    scale_color_manual(values = reason_colors, breaks = names(reason_colors)) +
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
} else {
  message("No columns matching '^prop_susp_' found. Skipping Plot 2 (reason mix).")
  p_reason_share <- NULL
}

# ============ Print & Save ============
print(p_rate_totals)
if (!is.null(p_reason_share)) print(p_reason_share)

dir.create(here("outputs"), showWarnings = FALSE)
ggsave(here("outputs", "01_rate_total_overall_and_race_labeled.png"), p_rate_totals,
       width = 12, height = 7, dpi = 300, bg = "white")
if (!is.null(p_reason_share)) {
  ggsave(here("outputs", "01_share_by_reason_over_time_labeled.png"),  p_reason_share,
         width = 12, height = 7, dpi = 300, bg = "white")
}
# End of file