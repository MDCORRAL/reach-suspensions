# analysis/05a_rates_by_race_by_locale.R
# Suspension rates by race, faceted by locale (two images max).

# --- 1) Setup -----------------------------------------------------------------
suppressPackageStartupMessages({
  library(here); library(arrow); library(dplyr); library(tidyr)
  library(stringr); library(ggplot2); library(scales); library(ggrepel)
})

source(here::here("R", "utils_keys_filters.R"))

# --- 2) Config ----------------------------------------------------------------
INCLUDE_UNKNOWN <- FALSE   # include "Unknown" locale?
LABEL_EVERY     <- 1       # label every year (2 = every other year, etc.)
LABEL_SIZE      <- 2.6
NUDGE_X         <- 0.10
SEGMENT_ALPHA   <- 0.40
POINT_ALL       <- 2.0
POINT_RACE      <- 1.7
LINE_ALL        <- 1.15
LINE_RACE       <- 0.95
IMG_WIDTH       <- 12.5
IMG_HEIGHT      <- 5.8
IMG_DPI         <- 300

set.seed(42)

# --- 3) Load & prepare --------------------------------------------------------
message("Loading data…")
v5_path <- here::here("data-stage","susp_v6_long.parquet")
if (!file.exists(v5_path)) stop("Data file not found: ", v5_path)
v5 <- arrow::read_parquet(v5_path)

need <- c("subgroup","academic_year","locale_simple",
          "total_suspensions","cumulative_enrollment")
miss <- setdiff(need, names(v5))
if (length(miss)) stop("Missing columns: ", paste(miss, collapse=", "))

year_levels <- v5 %>%
  filter(category_type == "Race/Ethnicity", subgroup == "All Students") %>%
  distinct(academic_year) %>% arrange(academic_year) %>% pull(academic_year)

# All Students
df_total <- v5 %>%
  filter(category_type == "Race/Ethnicity", subgroup == "All Students") %>%
  group_by(academic_year, locale_simple) %>%
  summarise(susp=sum(total_suspensions, na.rm=TRUE),
            enroll=sum(cumulative_enrollment, na.rm=TRUE), .groups="drop") %>%
  mutate(rate=if_else(enroll>0, susp/enroll, NA_real_), race="All Students")

# Race-specific
df_race <- v5 %>%
  filter(subgroup %in% c("Black/African American","White","Hispanic/Latino","Hispanic/Latino","American Indian/Alaska Native","Asian","Filipino","Native Hawaiian/Pacific Islander","Two or More Races")) %>%
  mutate(race=canon_race_label(subgroup)) %>%
  filter(!is.na(race)) %>%
  group_by(academic_year, locale_simple, race) %>%
  summarise(susp=sum(total_suspensions, na.rm=TRUE),
            enroll=sum(cumulative_enrollment, na.rm=TRUE), .groups="drop") %>%
  mutate(rate=if_else(enroll>0, susp/enroll, NA_real_))

df_all <- bind_rows(df_total, df_race) %>%
  mutate(year_fct=factor(academic_year, levels=year_levels),
         label_txt=percent(rate, accuracy=0.1))

# Locale sets (two images)
LOCALES_A <- locale_levels[1:2]
LOCALES_B <- locale_levels[!(locale_levels %in% LOCALES_A)]
if (!INCLUDE_UNKNOWN) LOCALES_B <- LOCALES_B[LOCALES_B != tail(locale_levels, 1)]

# Race palette (All Students = black)
race_levels <- df_all %>% distinct(race) %>% arrange(race) %>% pull(race)
pal_race <- setNames(hue_pal()(length(race_levels)), race_levels)
if ("All Students" %in% names(pal_race)) pal_race["All Students"] <- "#000000"

# --- 4) Plot function ---------------------------------------------------------
plot_by_locale_set <- function(locales, title_suffix) {
  dat <- df_all %>%
    filter(locale_simple %in% locales) %>%
    mutate(locale_simple = factor(locale_simple, levels = locales),
           is_label = ((as.integer(year_fct)-1L) %% LABEL_EVERY) == 0)
  if (!nrow(dat)) return(NULL)
  labels_df <- dat %>% filter(!is.na(rate) & is_label)
  
  ggplot(dat, aes(x=year_fct, y=rate, color=race, group=race)) +
    geom_line(data=dat %>% filter(race=="All Students"),
              color="black", linewidth=LINE_ALL) +
    geom_point(data=dat %>% filter(race=="All Students"),
               color="black", size=POINT_ALL) +
    geom_line(linewidth=LINE_RACE) +
    geom_point(size=POINT_RACE) +
    ggrepel::geom_text_repel(
      data = labels_df,
      aes(label = label_txt),
      size = LABEL_SIZE, show.legend = FALSE,
      direction = "y", nudge_x = NUDGE_X,
      box.padding = 0.12, point.padding = 0.12,
      min.segment.length = 0, segment.alpha = SEGMENT_ALPHA, segment.size = 0.25
    ) +
    scale_color_manual(values = pal_race, name = "Race/Ethnicity") +
    scale_y_continuous(labels = percent_format(accuracy = 0.1), limits = c(0, NA),
                       expand = expansion(mult = c(0, 0.12))) +
    scale_x_discrete(expand = expansion(mult = c(0.02, 0.26))) +
    labs(
      title = paste0("Suspension Rates by Race • ", title_suffix),
      subtitle = "Pooled rate within year × locale × race (All Students in bold black)",
      x = NULL, y = "Suspensions per student (%)"
    ) +
    facet_wrap(~ locale_simple, nrow = 1, scales = "free_y") +
    theme_minimal(base_size = 12) +
    theme(axis.text.x = element_text(angle = 35, hjust = 1),
          legend.position = "bottom",
          plot.margin = margin(12, 28, 12, 12))
}

# --- 5) Render & save ---------------------------------------------------------
outdir <- here::here("outputs"); dir.create(outdir, showWarnings = FALSE)

p_city_suburb <- plot_by_locale_set(LOCALES_A, "City + Suburban")
p_rural_town  <- plot_by_locale_set(LOCALES_B, paste(LOCALES_B, collapse=" + "))

if (!is.null(p_city_suburb)) {
  print(p_city_suburb)
  ggsave(file.path(outdir, "A_rates_by_race_city_suburban.png"),
         p_city_suburb, width=IMG_WIDTH, height=IMG_HEIGHT, dpi=IMG_DPI, bg="white")
}
if (!is.null(p_rural_town)) {
  print(p_rural_town)
  ggsave(file.path(outdir, "A_rates_by_race_rural_town.png"),
         p_rural_town, width=IMG_WIDTH, height=IMG_HEIGHT, dpi=IMG_DPI, bg="white")
}
message("✓ Saved images to: ", outdir)