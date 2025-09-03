# analysis/05a_rates_by_race_by_locale_TWO.R
suppressPackageStartupMessages({
  library(here); library(arrow); library(dplyr); library(tidyr)
  library(stringr); library(ggplot2); library(scales); library(ggrepel)
})

# ===== knobs =====
INCLUDE_UNKNOWN   <- FALSE     # include "Unknown" locale?
LABEL_EVERY       <- 1         # 1 = label every year; 2 = every other year, etc.
LABEL_SIZE        <- 2.4
NUDGE_X           <- 0.10
SEGMENT_ALPHA     <- 0.4
POINT_SIZE_ALL    <- 2.0
POINT_SIZE_RACE   <- 1.6
LINE_SIZE_ALL     <- 1.1
LINE_SIZE_RACE    <- 0.95

set.seed(42)

# ===== load & prep =====
v5 <- arrow::read_parquet(here::here("data-stage","susp_v5.parquet"))

need <- c("reporting_category","academic_year","locale_simple",
          "total_suspensions","cumulative_enrollment")
miss <- setdiff(need, names(v5))
if (length(miss)) stop("Missing columns in v5: ", paste(miss, collapse=", "))

year_levels <- v5 %>%
  filter(reporting_category == "TA") %>%
  distinct(academic_year) %>% arrange(academic_year) %>% pull(academic_year)

# map race codes (RD excluded, RL -> RH)
race_label <- function(code) dplyr::recode(
  code,
  RB = "Black/African American", RW = "White",
  RH = "Hispanic/Latino", RL = "Hispanic/Latino",
  RI = "American Indian/Alaska Native", RA = "Asian",
  RF = "Filipino", RP = "Pacific Islander",
  RT = "Two or More Races", TA = "All Students",
  .default = NA_character_
)

# All Students (TA)
df_total <- v5 %>%
  filter(reporting_category == "TA") %>%
  group_by(academic_year, locale_simple) %>%
  summarise(susp = sum(total_suspensions, na.rm = TRUE),
            enroll = sum(cumulative_enrollment, na.rm = TRUE), .groups="drop") %>%
  mutate(rate = if_else(enroll > 0, susp/enroll, NA_real_), race = "All Students")

# Race-specific
df_race <- v5 %>%
  filter(reporting_category %in% c("RB","RW","RH","RL","RI","RA","RF","RP","RT")) %>%
  mutate(race = race_label(reporting_category)) %>%
  filter(!is.na(race)) %>%
  group_by(academic_year, locale_simple, race) %>%
  summarise(susp = sum(total_suspensions, na.rm = TRUE),
            enroll = sum(cumulative_enrollment, na.rm = TRUE), .groups="drop") %>%
  mutate(rate = if_else(enroll > 0, susp/enroll, NA_real_))

df_all <- bind_rows(df_total, df_race) %>%
  mutate(
    year_fct   = factor(academic_year, levels = year_levels),
    year_idx   = as.integer(factor(academic_year, levels = year_levels)),
    label_txt  = percent(rate, accuracy = 0.1)
  )

# locales for the two images
LOCALES_A <- c("City","Suburban")
LOCALES_B <- if (INCLUDE_UNKNOWN) c("Rural","Town","Unknown") else c("Rural","Town")

# palette for races (All Students = black)
race_levels <- df_all %>% distinct(race) %>% arrange(race) %>% pull(race)
pal_race <- setNames(scales::hue_pal()(length(race_levels)), race_levels)
if ("All Students" %in% names(pal_race)) pal_race["All Students"] <- "#000000"

labels_mask <- (df_all$year_idx - min(df_all$year_idx, na.rm = TRUE)) %% LABEL_EVERY == 0

plot_by_locale_set <- function(locales, title_suffix) {
  dat <- df_all %>%
    dplyr::filter(locale_simple %in% locales) %>%
    dplyr::mutate(locale_simple = factor(locale_simple, levels = locales),
                  # compute label flag *inside* the subset
                  is_label = ((as.integer(year_fct) - 1L) %% LABEL_EVERY) == 0)
  
  labels_df <- dat %>% dplyr::filter(!is.na(rate) & is_label)
  
  ggplot(dat, aes(x = year_fct, y = rate, color = race, group = race)) +
    geom_line(data = dat %>% dplyr::filter(race == "All Students"),
              color = "black", size = LINE_SIZE_ALL) +
    geom_point(data = dat %>% dplyr::filter(race == "All Students"),
               color = "black", size = POINT_SIZE_ALL) +
    geom_line(size = LINE_SIZE_RACE) +
    geom_point(size = POINT_SIZE_RACE) +
    ggrepel::geom_text_repel(
      data = labels_df,
      aes(label = label_txt),
      size = LABEL_SIZE, show.legend = FALSE,
      direction = "y", nudge_x = NUDGE_X,
      box.padding = 0.12, point.padding = 0.12,
      min.segment.length = 0, segment.alpha = SEGMENT_ALPHA, segment.size = 0.25
    ) +
    scale_color_manual(values = pal_race, name = "Race/Ethnicity") +
    scale_y_continuous(labels = scales::percent_format(accuracy = 0.1), limits = c(0, NA),
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
# ===== make & save plots =====

p_city_suburb <- plot_by_locale_set(LOCALES_A, "City + Suburban")
p_rural_town  <- plot_by_locale_set(LOCALES_B, paste(LOCALES_B, collapse = " + "))

print(p_city_suburb); print(p_rural_town)

dir.create(here::here("outputs"), showWarnings = FALSE)
ggsave(here::here("outputs","A_rates_by_race_city_suburban.png"), p_city_suburb,
       width = 12, height = 5.8, dpi = 300, bg = "white")
ggsave(here::here("outputs","A_rates_by_race_rural_town.png"), p_rural_town,
       width = 12, height = 5.8, dpi = 300, bg = "white")
