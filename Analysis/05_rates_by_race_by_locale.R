# analysis/05_rates_by_race_by_locale_cleaner.R
suppressPackageStartupMessages({
  library(here); library(arrow); library(dplyr); library(tidyr)
  library(stringr); library(ggplot2); library(scales); library(ggrepel); library(readr)
})

# ======= knobs =======
KEEP_CORE_RACES_ONLY <- FALSE   # TRUE keeps a focused set to reduce clutter
CORE_RACES <- c("All Students","Black/African American","Hispanic/Latino","White","Asian")
INCLUDE_UNKNOWN      <- FALSE  # set TRUE to include "Unknown" in Rural/Town panel

LABEL_SIZE           <- 2.6    # label size for per-year labels
NUDGE_X              <- 0.10   # small right nudge so labels don't sit directly on points
SEGMENT_ALPHA        <- 0.4    # connector line transparency
POINT_SIZE_ALL       <- 2.0    # point size for All Students
POINT_SIZE_RACE      <- 1.7    # point size for race lines
LINE_SIZE_ALL        <- 1.2    # thickness for All Students line
LINE_SIZE_RACE       <- 1.0    # thickness for race lines

set.seed(42)  # reproducible label placement

# ======= load & guards =======
v5 <- read_parquet(here("data-stage","susp_v5.parquet"))
need <- c("reporting_category","academic_year","locale_simple",
          "total_suspensions","cumulative_enrollment")
miss <- setdiff(need, names(v5))
if (length(miss)) stop("Missing columns in v5: ", paste(miss, collapse=", "),
                       "\nRe-run 02_feature_locale_simple.R and downstream.")

# Year order
year_levels <- v5 %>%
  filter(reporting_category == "TA") %>%
  distinct(academic_year) %>% arrange(academic_year) %>% pull(academic_year)

# Locale groups (two outputs)
LOCALES_A <- c("City","Suburban")
LOCALES_B <- if (INCLUDE_UNKNOWN) c("Rural","Town","Unknown") else c("Rural","Town")

# Race mapping (RD excluded; RL aliases to RH)
race_label <- function(code) dplyr::recode(
  code,
  RB = "Black/African American",
  RW = "White",
  RH = "Hispanic/Latino",
  RL = "Hispanic/Latino",
  RI = "American Indian/Alaska Native",
  RA = "Asian",
  RF = "Filipino",
  RP = "Pacific Islander",
  RT = "Two or More Races",
  TA = "All Students",
  .default = NA_character_
)

# ======= pooled rates by year × locale × race =======
# All Students (TA)
df_total <- v5 %>%
  filter(reporting_category == "TA") %>%
  group_by(academic_year, locale_simple) %>%
  summarise(
    susp   = sum(total_suspensions, na.rm = TRUE),
    enroll = sum(cumulative_enrollment, na.rm = TRUE),
    .groups="drop"
  ) %>%
  mutate(rate = if_else(enroll > 0, susp/enroll, NA_real_),
         race = "All Students")

# Races
allowed_race_codes <- c("RB","RW","RH","RL","RI","RA","RF","RP","RT")
df_race <- v5 %>%
  filter(reporting_category %in% allowed_race_codes) %>%
  mutate(race = race_label(reporting_category)) %>%
  filter(!is.na(race)) %>%
  group_by(academic_year, locale_simple, race) %>%
  summarise(
    susp   = sum(total_suspensions, na.rm = TRUE),
    enroll = sum(cumulative_enrollment, na.rm = TRUE),
    .groups="drop"
  ) %>%
  mutate(rate = if_else(enroll > 0, susp/enroll, NA_real_))

# Combine
df_all <- bind_rows(df_total, df_race) %>%
  mutate(
    year_fct = factor(academic_year, levels = year_levels),
    locale_simple = as.character(locale_simple)
  )

# Optional: keep a clean, core set of races
if (KEEP_CORE_RACES_ONLY) {
  df_all <- df_all %>% filter(race %in% CORE_RACES)
}

# Stable race order for legend
race_levels <- df_all %>% distinct(race) %>% arrange(race) %>% pull(race)
df_all <- df_all %>% mutate(race = factor(race, levels = race_levels))

# Colors (Okabe–Ito-ish; All Students = black)
pal_race <- setNames(hue_pal()(length(race_levels)), race_levels)
if ("All Students" %in% names(pal_race)) pal_race["All Students"] <- "#000000"

# ------- helper: one plot for a given locale set, with ALL-YEAR labels -------
plot_by_locale_set <- function(data, locales, title_suffix) {
  dat <- data %>%
    filter(locale_simple %in% locales) %>%
    mutate(locale_simple = factor(locale_simple, levels = locales))
  
  # Label *all* points (every year) per (locale, race)
  labels_df <- dat %>% filter(!is.na(rate))
  
  ggplot(dat, aes(x = year_fct, y = rate, color = race, group = race)) +
    # bold All Students underlayer
    geom_line(
      data = dat %>% filter(race == "All Students"),
      color = "black", linewidth = LINE_SIZE_ALL
    ) +
    geom_point(
      data = dat %>% filter(race == "All Students"),
      color = "black", size = POINT_SIZE_ALL
    ) +
    # race lines/points
    geom_line(linewidth = LINE_SIZE_RACE) +
    geom_point(size = POINT_SIZE_RACE) +
    # label every year (no halo)
    ggrepel::geom_text_repel(
      data = labels_df,
      aes(label = percent(rate, accuracy = 0.1)),
      size = LABEL_SIZE, show.legend = FALSE,
      direction = "y", nudge_x = NUDGE_X,
      box.padding = 0.12, point.padding = 0.12, min.segment.length = 0,
      segment.alpha = SEGMENT_ALPHA, segment.size = 0.25
    ) +
    scale_color_manual(values = pal_race, name = "Race/Ethnicity") +
    scale_y_continuous(labels = percent_format(accuracy = 0.1), limits = c(0, NA),
                       expand = expansion(mult = c(0, 0.12))) +
    scale_x_discrete(expand = expansion(mult = c(0.02, 0.26))) +
    labs(
      title = paste0("Suspension Rates by Race • ", title_suffix),
      subtitle = "Pooled rate within year × locale × race (All Students in bold black). Labels shown for every year.",
      x = NULL, y = "Suspensions per student (%)"
    ) +
    facet_wrap(~ locale_simple, nrow = 1, scales = "free_y") +
    theme_minimal(base_size = 12) +
    theme(
      axis.text.x = element_text(angle = 35, hjust = 1),
      legend.position = "bottom",
      plot.margin = margin(12, 28, 12, 12)
    )
}

# ------- build the two clean panels -------
p_city_suburb <- plot_by_locale_set(df_all, LOCALES_A, "City + Suburban")
p_rural_town  <- plot_by_locale_set(df_all, LOCALES_B, paste(LOCALES_B, collapse = " + "))

# ------- print & save -------
print(p_city_suburb)
print(p_rural_town)

dir.create(here("outputs"), showWarnings = FALSE)
ggsave(here("outputs","rates_by_race_city_suburban_all_labels.png"), p_city_suburb,
       width = 12, height = 5.8, dpi = 300, bg = "white")
ggsave(here("outputs","rates_by_race_rural_town_all_labels.png"), p_rural_town,
       width = 12, height = 5.8, dpi = 300, bg = "white")
