# analysis/07_rates_trad_vs_other_by_race_by_locale.R
# Suspension rates by race, comparing Traditional vs All other, split by locale.

# --- 1) Setup -----------------------------------------------------------------
suppressPackageStartupMessages({
  library(here); library(arrow); library(dplyr); library(tidyr)
  library(stringr); library(ggplot2); library(scales); library(ggrepel)
})

# --- 2) Config ----------------------------------------------------------------
INCLUDE_UNKNOWN       <- FALSE  # include "Unknown" locale?
MAX_FACETS_PER_IMAGE  <- 2      # ≤ 2 race facets per image
LABEL_EVERY           <- 1      # 1 = every year; 2 = every other year, etc.
LABEL_SIZE            <- 2.6
NUDGE_X               <- 0.10
SEGMENT_ALPHA         <- 0.40
POINT_SIZE            <- 1.8
LINE_SIZE             <- 1.05
IMG_WIDTH             <- 12.5
IMG_HEIGHT            <- 6.0
IMG_DPI               <- 300
DROP_ALL_STUDENTS     <- TRUE   # drop TA when faceting by race
KEEP_CORE_RACES_ONLY  <- FALSE  # set TRUE to reduce clutter
CORE_RACES <- c("Black/African American","Hispanic/Latino","White","Asian")

set.seed(42)

# --- 3) Load & guards ---------------------------------------------------------
message("Loading data…")
v5_path <- here::here("data-stage","susp_v5.parquet")
if (!file.exists(v5_path)) stop("Data file not found: ", v5_path)
v5 <- arrow::read_parquet(v5_path)

need <- c("reporting_category","academic_year","locale_simple","level_strict3",
          "school_type","total_suspensions","cumulative_enrollment")
miss <- setdiff(need, names(v5))
if (length(miss)) stop("Missing required columns: ", paste(miss, collapse=", "))

year_levels <- v5 %>%
  dplyr::filter(reporting_category == "TA") %>%
  dplyr::distinct(academic_year) %>% dplyr::arrange(academic_year) %>% dplyr::pull(academic_year)
if (!length(year_levels)) stop("No TA rows to establish academic year order.")

# --- 4) School group & caption note ------------------------------------------
v5 <- v5 %>%
  mutate(
    school_group = dplyr::case_when(
      level_strict3 %in% c("Elementary","Middle","High") ~ "Traditional",
      TRUE                                               ~ "All other"
    )
  )

# Build a short hint of what “All other” includes
alt_examples <- v5 %>%
  filter(level_strict3 == "Alternative") %>%
  distinct(school_type) %>% pull() %>% tolower()
alt_hint <- c("continuation","community day","juvenile court","alternative")
alt_found <- alt_hint[alt_hint %in% unique(unlist(str_split(alt_examples, "\\W+")))]
alt_found_pretty <- if (length(alt_found)) paste(alt_found, collapse=", ") else "alternative settings"
all_other_note <- paste0("All other = Alternative (e.g., ", alt_found_pretty, 
                         ") + schools with Other/Unknown grade spans.")

# --- 5) Race labels -----------------------------------------------------------
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

# --- 6) Aggregate to pooled rates by year × locale × race × group ------------
allowed_race_codes <- c("RB","RW","RH","RL","RI","RA","RF","RP","RT","TA")

df_all <- v5 %>%
  mutate(race = race_label(reporting_category)) %>%
  filter(!is.na(race), reporting_category %in% allowed_race_codes) %>%
  group_by(academic_year, locale_simple, school_group, race) %>%
  summarise(
    susp   = sum(total_suspensions, na.rm = TRUE),
    enroll = sum(cumulative_enrollment, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    rate      = if_else(enroll > 0, susp/enroll, NA_real_),
    year_fct  = factor(academic_year, levels = year_levels),
    label_txt = percent(rate, accuracy = 0.1)
  )

if (DROP_ALL_STUDENTS) df_all <- df_all %>% filter(race != "All Students")
if (KEEP_CORE_RACES_ONLY) df_all <- df_all %>% filter(race %in% CORE_RACES)
if (!nrow(df_all)) stop("No data available after filtering.")

# Locales to render
locale_levels <- c("City","Suburban","Town","Rural","Unknown")
if (!INCLUDE_UNKNOWN) locale_levels <- setdiff(locale_levels, "Unknown")

# --- 7) Plot function: one locale, race-faceted (≤2 panels) -------------------
plot_locale_chunk <- function(dat_locale, races, loc_name, i, n_total) {
  dat <- dat_locale %>%
    filter(race %in% races) %>%
    mutate(
      race = factor(race, levels = races),
      is_label = ((as.integer(year_fct) - 1L) %% LABEL_EVERY) == 0
    )
  if (!nrow(dat)) return(NULL)
  labels_df <- dat %>% filter(!is.na(rate) & is_label)
  
  ggplot(dat, aes(x = year_fct, y = rate, color = school_group, group = school_group)) +
    geom_line(linewidth = LINE_SIZE) +
    geom_point(size = POINT_SIZE) +
    ggrepel::geom_text_repel(
      data = labels_df,
      aes(label = label_txt, color = school_group),
      size = LABEL_SIZE, show.legend = FALSE,
      direction = "y", nudge_x = NUDGE_X,
      box.padding = 0.16, point.padding = 0.16,
      min.segment.length = 0, segment.alpha = SEGMENT_ALPHA, segment.size = 0.25
    ) +
    scale_color_manual(values = c("Traditional"="#1f77b4", "All other"="#d62728"),
                       name = "School group") +
    scale_y_continuous(labels = percent_format(accuracy = 0.1), limits = c(0, NA),
                       expand = expansion(mult = c(0.05, 0.15))) +
    scale_x_discrete(expand = expansion(mult = c(0.05, 0.15))) +
    labs(
      title = paste0("Suspension Rates — ", loc_name, 
                     " (Set ", i, " of ", n_total, ")"),
      subtitle = "Suspension rate per 100 students; faceted by race/ethnicity",
      caption = all_other_note,
      x = NULL, y = "Suspension Rate (%)"
    ) +
    facet_wrap(~ race, ncol = MAX_FACETS_PER_IMAGE, scales = "free_y") +
    theme_minimal(base_size = 12) +
    theme(
      axis.text.x = element_text(angle = 35, hjust = 1),
      legend.position = "bottom",
      plot.title = element_text(face = "bold", size = 16),
      strip.text = element_text(face = "bold", size = 12),
      plot.margin = margin(12, 12, 12, 12)
    )
}

# --- 8) Render & save: loop over locales; chunk races per locale --------------
outdir <- here::here("outputs"); dir.create(outdir, showWarnings = FALSE)

for (loc in locale_levels) {
  dat_loc <- df_all %>% filter(locale_simple == loc)
  if (!nrow(dat_loc)) { message("Skipping ", loc, " (no data)."); next }
  
  # Order races by avg rate within this locale, then chunk into pairs
  race_order <- dat_loc %>%
    group_by(race) %>%
    summarise(avg_rate = mean(rate, na.rm = TRUE), .groups = "drop") %>%
    arrange(desc(avg_rate)) %>% pull(race)
  
  chunk_id <- ceiling(seq_along(race_order) / MAX_FACETS_PER_IMAGE)
  race_chunks <- split(race_order, chunk_id)
  message("Locale ", loc, ": ", length(race_chunks), " image(s).")
  
  for (i in seq_along(race_chunks)) {
    races_i <- race_chunks[[i]]
    p <- plot_locale_chunk(dat_loc, races_i, loc, i, length(race_chunks))
    if (is.null(p)) next
    print(p)
    fname <- sprintf("C_rates_trad_vs_other_by_race_%s_set_%02d.png",
                     stringr::str_replace_all(loc, "[^A-Za-z0-9]+", "_"), i)
    ggsave(file.path(outdir, fname), p,
           width = IMG_WIDTH, height = IMG_HEIGHT, dpi = IMG_DPI, bg = "white")
  }
}

message("✓ Done. Images saved to: ", outdir)