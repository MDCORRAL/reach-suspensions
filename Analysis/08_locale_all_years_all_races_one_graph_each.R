# analysis/08_locale_all_years_all_races_one_graph_each.R
# One graph per locale: all years, all races, with final, polished data labels.

# --- 1) Setup -----------------------------------------------------------------
suppressPackageStartupMessages({
  library(here); library(arrow); library(dplyr); library(tidyr)
  library(stringr); library(ggplot2); library(scales); library(ggrepel)
})

# --- 2) Config ----------------------------------------------------------------
POINT_ALL         <- 2.2
POINT_RACE        <- 1.9
LINE_ALL          <- 1.25
LINE_RACE         <- 1.00
IMG_WIDTH         <- 12.5
IMG_HEIGHT        <- 7.0
IMG_DPI           <- 300

set.seed(42)

# --- 3) Load & guards ---------------------------------------------------------
v5_path <- here::here("data-stage","susp_v5.parquet")
if (!file.exists(v5_path)) stop("Data file not found: ", v5_path)
v5 <- arrow::read_parquet(v5_path)

need <- c("reporting_category","academic_year","locale_simple",
          "total_suspensions","cumulative_enrollment")
miss <- setdiff(need, names(v5))
if (length(miss)) stop("Missing columns: ", paste(miss, collapse=", "))

# Year order driven by TA
year_levels <- v5 %>%
  filter(reporting_category == "TA") %>%
  distinct(academic_year) %>% arrange(academic_year) %>% pull(academic_year)
if (!length(year_levels)) stop("No TA rows to establish academic year order.")

# --- 4) Race labels and Data Prep ---------------------------------------------
race_label <- function(code) dplyr::recode(
  code,
  RB="Black/African American",
  RW="White",
  RH="Hispanic/Latino",
  RL="Hispanic/Latino",           # alias
  RI="American Indian/Alaska Native",
  RA="Asian",
  RF="Filipino",
  RP="Pacific Islander",
  RT="Two or More Races",
  TA="All Students",
  .default = NA_character_
)

# Keep TA + known races; drop RD (Not Reported)
allowed_codes <- c("TA","RB","RW","RH","RL","RI","RA","RF","RP","RT")

df <- v5 %>%
  filter(reporting_category %in% allowed_codes) %>%
  mutate(
    race = race_label(reporting_category),
    year_fct = factor(academic_year, levels = year_levels)
  ) %>%
  group_by(locale_simple, race, academic_year, year_fct) %>%
  summarise(
    susp   = sum(total_suspensions, na.rm = TRUE),
    enroll = sum(cumulative_enrollment, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    rate      = if_else(enroll > 0, susp / enroll, NA_real_),
    label_txt = percent(rate, accuracy = 0.1)
  )

# Locales to render (1 image per)
locale_levels <- c("City","Suburban","Town","Rural")

# --- 5) Plot helper (Sharpened with Corrected Label Size) ----------------------
plot_one_locale <- function(loc_name) {
  dat <- df %>% filter(locale_simple == loc_name)
  if (!nrow(dat)) return(NULL)
  
  # Stable race order; color map with All Students = black
  race_levels <- dat %>% distinct(race) %>% arrange(race) %>% pull(race)
  pal_race <- setNames(hue_pal()(length(race_levels)), race_levels)
  if ("All Students" %in% names(pal_race)) pal_race["All Students"] <- "#000000"
  
  # Data for numeric labels on every point
  labels_all_points <- dat %>% filter(!is.na(rate))
  
  # Data for the direct race labels at the end of the series
  end_labels <- dat %>%
    group_by(race) %>%
    filter(academic_year == max(academic_year, na.rm = TRUE)) %>%
    ungroup()
  
  ggplot(dat, aes(x = year_fct, y = rate, color = race, group = race)) +
    # All Students highlighted
    geom_line(data = . %>% filter(race == "All Students"), linewidth = LINE_ALL) +
    geom_point(data = . %>% filter(race == "All Students"), size = POINT_ALL) +
    # Other races
    geom_line(data = . %>% filter(race != "All Students"), linewidth = LINE_RACE) +
    geom_point(data = . %>% filter(race != "All Students"), size = POINT_RACE) +
    
    # 1. Add small, subtle numeric labels to EVERY point
    ggrepel::geom_text_repel(
      data = labels_all_points,
      aes(label = round(rate * 100, 1)),
      color = "grey30",
      size = 2.5,
      segment.alpha = 0,
      point.padding = 0.1,
      box.padding = 0.1,
      min.segment.length = Inf,
      show.legend = FALSE
    ) +
    
    # 2. Add direct, color-matched RACE labels at the end of each line
    ggrepel::geom_text_repel(
      data = end_labels,
      aes(label = race, color = race),
      fontface = "bold",
      size = 3.4,                      # <-- ADJUSTED FONT SIZE
      nudge_x = 0.6,
      segment.color = 'grey50',
      segment.alpha = 0.8,
      box.padding = 0.5,
      point.padding = 0.4,
      min.segment.length = 0,
      show.legend = FALSE
    ) +
    
    scale_color_manual(values = pal_race) +
    scale_y_continuous(labels = percent_format(accuracy = 1), limits = c(0, NA),
                       expand = expansion(mult = c(0.05, 0.15))) +
    scale_x_discrete(expand = expansion(mult = c(0.05, 0.60))) + 
    labs(
      title = paste0("Suspension Rates — ", loc_name),
      subtitle = "Suspension rate per 100 students. 'All Students' shown in bold black.",
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

# --- 6) Render & save ---------------------------------------------------------
outdir <- here::here("outputs"); dir.create(outdir, showWarnings = FALSE)

for (loc in locale_levels) {
  p <- plot_one_locale(loc)
  if (is.null(p)) { message("Skipping ", loc, " (no data)."); next }
  print(p)
  fname <- sprintf("08_locale_%s_all_years_all_races.png",
                   stringr::str_replace_all(loc, "[^A-Za-z0-9]+", "_"))
  ggsave(file.path(outdir, fname), p,
         width = IMG_WIDTH, height = IMG_HEIGHT, dpi = IMG_DPI, bg = "white")
}

message("✓ Saved one image per locale to: ", outdir)