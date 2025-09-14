# Analysis/09_rates_by_level_and_by_level_locale.R
# One graph per school level (Elementary/Middle/High School), and one per (level × locale),
# showing all years and all race/ethnicity lines with readable labels.

# --- 1) Setup -----------------------------------------------------------------
suppressPackageStartupMessages({
  library(here); library(arrow); library(dplyr); library(tidyr)
  library(stringr); library(ggplot2); library(scales); library(ggrepel)
})

try(here::i_am("Analysis/09_rates_by_level_and_by_level_locale.R"), silent = TRUE)

source(here::here("R", "utils_keys_filters.R"))

# --- 2) Config ----------------------------------------------------------------
INCLUDE_UNKNOWN_LOCALE <- FALSE   # set TRUE to also render "Unknown" locale images

# Styling knobs (kept subtle & readable)
POINT_ALL   <- 2.2
POINT_RACE  <- 1.9
LINE_ALL    <- 1.25
LINE_RACE   <- 1.00
LABEL_SIZE  <- 2.6
IMG_WIDTH   <- 12.5
IMG_HEIGHT  <- 7.0
IMG_DPI     <- 300

set.seed(42)

# --- 3) Load & guards ---------------------------------------------------------
v6_path <- here::here("data-stage","susp_v6_long.parquet")
if (!file.exists(v6_path)) stop("Data file not found: ", v6_path)
v6 <- arrow::read_parquet(v6_path)

need <- c("subgroup","academic_year","locale_simple","school_level",
          "total_suspensions","cumulative_enrollment")
miss <- setdiff(need, names(v6))
if (length(miss)) stop("Missing columns: ", paste(miss, collapse=", "))

# Academic year order driven by TA
year_levels <- v6 %>%
  filter(category_type == "Race/Ethnicity", subgroup == "All Students") %>%
  distinct(academic_year) %>% arrange(academic_year) %>% pull(academic_year)
if (!length(year_levels)) stop("No TA rows to establish academic year order.")

# Levels to include
# canonical grade levels
LEVELS <- setdiff(LEVEL_LABELS, c("Other", "Alternative"))

# --- 4) Race labels & allowed codes ------------------------------------------
# provided via canon_race_label() helper

# --- 5) Prep data -------------------------------------------------------------
# Base long with race and year factor
base <- v6 %>%
  filter(school_level %in% LEVELS) %>%
  mutate(
    race = canon_race_label(subgroup),
    year_fct = factor(academic_year, levels = year_levels),
    school_level = factor(school_level, levels = LEVELS)
  ) %>%
  filter(race %in% ALLOWED_RACES)

# A) Aggregated across locales -> by LEVEL × RACE × YEAR
df_level <- base %>%
  group_by(school_level, race, academic_year, year_fct) %>%
  summarise(
    susp   = sum(total_suspensions, na.rm = TRUE),
    enroll = sum(cumulative_enrollment, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    rate      = if_else(enroll > 0, susp / enroll, NA_real_),
    label_txt = percent(rate, accuracy = 0.1)
  )

# B) Split by locale -> by LEVEL × LOCALE × RACE × YEAR
# Locales to render
loc_levels <- if (INCLUDE_UNKNOWN_LOCALE) locale_levels else locale_levels[locale_levels != "Unknown"]

df_level_loc <- base %>%
  group_by(school_level, locale_simple, race, academic_year, year_fct) %>%
  summarise(
    susp   = sum(total_suspensions, na.rm = TRUE),
    enroll = sum(cumulative_enrollment, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    rate      = if_else(enroll > 0, susp / enroll, NA_real_),
    label_txt = percent(rate, accuracy = 0.1),
    locale_simple = factor(locale_simple, levels = loc_levels)
  )

# Global, consistent race palette across all images
all_races <- df_level %>% distinct(race) %>% arrange(race) %>% pull(race)
pal_race  <- setNames(scales::hue_pal()(length(all_races)), all_races)
if ("All Students" %in% names(pal_race)) pal_race["All Students"] <- "#000000"

# --- 6) Plot helpers ----------------------------------------------------------
plot_one_level <- function(level_name) {
  dat <- df_level %>% filter(school_level == level_name)
  if (!nrow(dat)) return(NULL)
  
  # End-of-series labels (race names on final year)
  end_labels <- dat %>%
    group_by(race) %>%
    filter(academic_year == max(academic_year, na.rm = TRUE)) %>%
    ungroup()
  
  ggplot(dat, aes(x = year_fct, y = rate, color = race, group = race)) +
    # All Students highlighted
    geom_line(data = subset(dat, race == "All Students"),
              linewidth = LINE_ALL) +
    geom_point(data = subset(dat, race == "All Students"),
               size = POINT_ALL) +
    # Other races
    geom_line(data = subset(dat, race != "All Students"),
              linewidth = LINE_RACE) +
    geom_point(data = subset(dat, race != "All Students"),
               size = POINT_RACE) +
    # Numeric labels on every point (subtle)
    ggrepel::geom_text_repel(
      data = dat %>% filter(!is.na(rate)),
      aes(label = round(rate * 100, 1)),
      color = "grey30",
      size = LABEL_SIZE - 0.1,
      segment.alpha = 0,
      point.padding = 0.08,
      box.padding = 0.08,
      min.segment.length = Inf,
      show.legend = FALSE
    ) +
    # Race name labels at the end of lines
    ggrepel::geom_text_repel(
      data = end_labels,
      aes(label = race, color = race),
      fontface = "bold",
      size = LABEL_SIZE + 0.6,
      nudge_x = 0.6,
      segment.color = 'grey50',
      segment.alpha = 0.8,
      box.padding = 0.5,
      point.padding = 0.35,
      min.segment.length = 0,
      show.legend = FALSE
    ) +
    scale_color_manual(values = pal_race) +
    scale_y_continuous(labels = percent_format(accuracy = 0.1), limits = c(0, NA),
                       expand = expansion(mult = c(0.05, 0.15))) +
    scale_x_discrete(expand = expansion(mult = c(0.05, 0.60))) +
    labs(
      title = paste0("Suspension Rates — ", level_name),
      subtitle = "All races and All Students, all years (All Students in bold black).",
      x = NULL, y = "Suspension Rate (%)"
    ) +
    theme_minimal(base_size = 12) +
    theme(
      legend.position = "none",
      axis.text.x = element_text(angle = 35, hjust = 1),
      plot.title = element_text(face = "bold", size = 16),
      plot.subtitle = element_text(size = 11),
      plot.margin = margin(12, 12, 12, 12)
    )
}

plot_one_level_locale <- function(level_name, loc_name) {
  dat <- df_level_loc %>% filter(school_level == level_name, locale_simple == loc_name)
  if (!nrow(dat)) return(NULL)
  
  end_labels <- dat %>%
    group_by(race) %>%
    filter(academic_year == max(academic_year, na.rm = TRUE)) %>%
    ungroup()
  
  ggplot(dat, aes(x = year_fct, y = rate, color = race, group = race)) +
    geom_line(data = subset(dat, race == "All Students"),
              linewidth = LINE_ALL) +
    geom_point(data = subset(dat, race == "All Students"),
               size = POINT_ALL) +
    geom_line(data = subset(dat, race != "All Students"),
              linewidth = LINE_RACE) +
    geom_point(data = subset(dat, race != "All Students"),
               size = POINT_RACE) +
    ggrepel::geom_text_repel(
      data = dat %>% filter(!is.na(rate)),
      aes(label = round(rate * 100, 1)),
      color = "grey30",
      size = LABEL_SIZE - 0.1,
      segment.alpha = 0,
      point.padding = 0.08,
      box.padding = 0.08,
      min.segment.length = Inf,
      show.legend = FALSE
    ) +
    ggrepel::geom_text_repel(
      data = end_labels,
      aes(label = race, color = race),
      fontface = "bold",
      size = LABEL_SIZE + 0.6,
      nudge_x = 0.6,
      segment.color = 'grey50',
      segment.alpha = 0.8,
      box.padding = 0.5,
      point.padding = 0.35,
      min.segment.length = 0,
      show.legend = FALSE
    ) +
    scale_color_manual(values = pal_race) +
    scale_y_continuous(labels = percent_format(accuracy = 0.1), limits = c(0, NA),
                       expand = expansion(mult = c(0.05, 0.15))) +
    scale_x_discrete(expand = expansion(mult = c(0.05, 0.60))) +
    labs(
      title = paste0("Suspension Rates — ", level_name, " • ", loc_name),
      subtitle = "All races and All Students, all years (All Students in bold black).",
      x = NULL, y = "Suspension Rate (%)"
    ) +
    theme_minimal(base_size = 12) +
    theme(
      legend.position = "none",
      axis.text.x = element_text(angle = 35, hjust = 1),
      plot.title = element_text(face = "bold", size = 16),
      plot.subtitle = element_text(size = 11),
      plot.margin = margin(12, 12, 12, 12)
    )
}

# --- 7) Render & save ---------------------------------------------------------
outdir <- here::here("outputs"); dir.create(outdir, showWarnings = FALSE)

# Set A: one image per LEVEL (all locales pooled)
for (lev in LEVELS) {
  p <- plot_one_level(lev)
  if (is.null(p)) { message("Skipping level: ", lev, " (no data)."); next }
  print(p)
  fname <- sprintf("09_level_%s_all_years_all_races.png",
                   stringr::str_replace_all(lev, "[^A-Za-z0-9]+", "_"))
  ggsave(file.path(outdir, fname), p,
         width = IMG_WIDTH, height = IMG_HEIGHT, dpi = IMG_DPI, bg = "white")
}

# Set B: one image per (LEVEL × LOCALE)
for (lev in LEVELS) {
  for (loc in loc_levels) {
    p <- plot_one_level_locale(lev, loc)
    if (is.null(p)) { message("Skipping ", lev, " × ", loc, " (no data)."); next }
    print(p)
    fname <- sprintf("09_level_%s_%s_all_years_all_races.png",
                     stringr::str_replace_all(lev, "[^A-Za-z0-9]+", "_"),
                     stringr::str_replace_all(loc, "[^A-Za-z0-9]+", "_"))
    ggsave(file.path(outdir, fname), p,
           width = IMG_WIDTH, height = IMG_HEIGHT, dpi = IMG_DPI, bg = "white")
  }
}

message("✓ Saved images to: ", outdir)