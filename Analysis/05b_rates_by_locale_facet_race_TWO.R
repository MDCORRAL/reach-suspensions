# Analysis/05b_rates_by_locale_facet_race_TWO.R
# Suspension rates by locale, faceted by race (<=2 facets per image).

# --- 1) Setup -----------------------------------------------------------------
suppressPackageStartupMessages({
  library(here); library(arrow); library(dplyr); library(tidyr)
  library(stringr); library(ggplot2); library(scales); library(ggrepel)
})

try(here::i_am("Analysis/05b_rates_by_locale_facet_race_TWO.R"), silent = TRUE)

source(here::here("R", "utils_keys_filters.R"))

# --- 2) Config ----------------------------------------------------------------
MAX_FACETS_PER_IMAGE  <- 2
DROP_ALL_STUDENTS     <- TRUE
INCLUDE_UNKNOWN       <- FALSE
LABEL_EVERY           <- 1
LABEL_SIZE            <- 2.6
NUDGE_X               <- 0.10
SEGMENT_ALPHA         <- 0.40
POINT_SIZE            <- 1.8
LINE_SIZE             <- 1.05
IMG_WIDTH             <- 12.5
IMG_HEIGHT            <- 6.0
IMG_DPI               <- 300

set.seed(42)

# --- 3) Load & prepare --------------------------------------------------------
message("Loading and preparing data…")
v6_path <- here::here("data-stage","susp_v6_long.parquet")
if (!file.exists(v6_path)) stop("Data file not found: ", v6_path)
v6 <- arrow::read_parquet(v6_path)

need <- c("subgroup","academic_year","locale_simple",
          "total_suspensions","cumulative_enrollment")
miss <- setdiff(need, names(v6))
if (length(miss)) stop("Missing required columns: ", paste(miss, collapse=", "))

year_levels <- v6 %>%
  filter(category_type == "Race/Ethnicity", subgroup == "All Students") %>%
  distinct(academic_year) %>% arrange(academic_year) %>% pull(academic_year)

df_all <- v6 %>%
  mutate(race = canon_race_label(subgroup)) %>%
  filter(race %in% ALLOWED_RACES) %>%  # drop Not Reported
  group_by(academic_year, locale_simple, race) %>%
  summarise(susp=sum(total_suspensions, na.rm=TRUE),
            enroll=sum(cumulative_enrollment, na.rm=TRUE), .groups="drop") %>%
  mutate(rate = if_else(enroll>0, susp/enroll, NA_real_),
         year_fct = factor(academic_year, levels=year_levels),
         label_txt = percent(rate, accuracy=0.1))

if (DROP_ALL_STUDENTS) df_all <- df_all %>% filter(race != "All Students")
if (!nrow(df_all)) stop("After filtering, no data to plot.")

# Order races by avg rate; split into chunks of <=2
race_order <- df_all %>%
  group_by(race) %>%
  summarise(avg_rate = mean(rate, na.rm=TRUE), .groups="drop") %>%
  arrange(desc(avg_rate)) %>% pull(race)
chunk_id <- ceiling(seq_along(race_order) / MAX_FACETS_PER_IMAGE)
race_chunks <- split(race_order, chunk_id)
message("Races split into ", length(race_chunks), " image(s).")

# Locale palette and ordering
loc_levels <- if (INCLUDE_UNKNOWN) locale_levels else locale_levels[locale_levels != "Unknown"]
pal_locale_use <- pal_locale[loc_levels]

# --- 4) Plot function ---------------------------------------------------------
plot_race_chunk <- function(races, i, n_total) {
  dat <- df_all %>%
    filter(race %in% races) %>%
    mutate(
      race = factor(race, levels=races),
      locale_simple = factor(locale_simple, levels=loc_levels),
      is_label = ((as.integer(year_fct)-1L) %% LABEL_EVERY) == 0
    )
  if (!nrow(dat)) return(NULL)
  labels_df <- dat %>% filter(!is.na(rate) & is_label)
  
  ggplot(dat, aes(x=year_fct, y=rate, color=locale_simple, group=locale_simple)) +
    geom_line(linewidth=LINE_SIZE) +
    geom_point(size=POINT_SIZE) +
    ggrepel::geom_text_repel(
      data = labels_df,
      aes(label = label_txt),
      size = LABEL_SIZE, show.legend = FALSE,
      direction = "y", nudge_x = NUDGE_X,
      box.padding = 0.16, point.padding = 0.16,
      min.segment.length = 0, segment.alpha = SEGMENT_ALPHA, segment.size = 0.25
    ) +
    scale_color_manual(values = pal_locale_use, name = "School Locale") +
    scale_y_continuous(labels = percent_format(accuracy = 0.1), limits = c(0, NA),
                       expand = expansion(mult = c(0.05, 0.15))) +
    scale_x_discrete(expand = expansion(mult = c(0.05, 0.15))) +
    labs(
      title = paste0("Suspension Rates by Locale (Set ", i, " of ", n_total, ")"),
      subtitle = "Suspension rate per 100 students, faceted by race/ethnicity",
      x = NULL, y = "Suspension Rate (%)"
    ) +
    facet_wrap(~ race, ncol = MAX_FACETS_PER_IMAGE, scales = "free_y") +
    theme_minimal(base_size = 12) +
    theme(axis.text.x = element_text(angle = 35, hjust = 1),
          legend.position = "bottom",
          plot.title = element_text(face = "bold", size = 16),
          strip.text = element_text(face = "bold", size = 12),
          plot.margin = margin(12, 12, 12, 12))
}

# --- 5) Render & save ---------------------------------------------------------
outdir <- here::here("outputs"); dir.create(outdir, showWarnings = FALSE)
for (i in seq_along(race_chunks)) {
  races_in_chunk <- race_chunks[[i]]
  message(sprintf("Generating plot %d for: %s", i, paste(races_in_chunk, collapse=" & ")))
  p <- plot_race_chunk(races_in_chunk, i, length(race_chunks))
  if (is.null(p)) { warning("No data for chunk ", i); next }
  print(p)
  fname <- sprintf("B_rates_by_locale_facet_race_set_%02d.png", i)
  ggsave(file.path(outdir, fname), p, width=IMG_WIDTH, height=IMG_HEIGHT, dpi=IMG_DPI, bg="white")
}
message("✓ Saved images to: ", outdir)