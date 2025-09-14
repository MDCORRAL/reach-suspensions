# analysis/06_rates_by_race_traditional_vs_other.R
# Suspension rates by race for Traditional vs All other schools

# --- 1) Setup -----------------------------------------------------------------
suppressPackageStartupMessages({
  library(here); library(arrow); library(dplyr); library(tidyr)
  library(stringr); library(ggplot2); library(scales); library(ggrepel)
})

try(here::i_am("Analysis/06_rates_by_race_traditional_vs_other.R"), silent = TRUE)

source(here::here("R", "utils_keys_filters.R"))

# --- 2) Config ----------------------------------------------------------------
LABEL_EVERY           <- 1       # 1 = label every year; 2 = every other year
LABEL_SIZE            <- 2.6
NUDGE_X               <- 0.10
SEGMENT_ALPHA         <- 0.40
POINT_ALL             <- 2.0
POINT_RACE            <- 1.7
LINE_ALL              <- 1.15
LINE_RACE             <- 0.95

IMG_W_A               <- 12.5    # image A (facet = group)
IMG_H_A               <- 5.8
IMG_W_B               <- 12.5    # image B (facet = race, chunked)
IMG_H_B               <- 6.0
IMG_DPI               <- 300
MAX_FACETS_PER_IMAGE  <- 2       # ≤2 race facets per saved image

KEEP_ALL_STUDENTS_IN_A <- TRUE   # include All Students baseline in View A
DROP_ALL_STUDENTS_IN_B <- TRUE   # drop it in View B (race facets)

set.seed(42)

# --- 3) Load & guards ---------------------------------------------------------
message("Loading v6…")
v6_path <- here::here("data-stage","susp_v6_long.parquet")
if (!file.exists(v6_path)) stop("Data file not found: ", v6_path)
v6 <- arrow::read_parquet(v6_path)

need <- c("subgroup","academic_year","school_level",
          "school_type","total_suspensions","cumulative_enrollment")
miss <- setdiff(need, names(v6))
if (length(miss)) stop("Missing columns in v6: ", paste(miss, collapse=", "))

# academic year order
year_levels <- v6 %>%
  filter(category_type == "Race/Ethnicity", subgroup == "All Students") %>%
  distinct(academic_year) %>% arrange(academic_year) %>% pull(academic_year)
if (!length(year_levels)) stop("No TA rows to establish year order.")

# Race label map (RD -> "Not Reported"; RL -> RH) handled by canon_race_label();
# "Not Reported" is excluded from plots.

# --- 4) Derive school group (Traditional vs All other) ------------------------
# "Traditional" = Elementary/Middle/High (canonical levels)
# "All other"   = Alternative + Other grade spans (alt/continuation/comm day/juvenile court, atypical/unknown)
v6 <- v6 %>%
  mutate(
    school_group = dplyr::case_when(
      school_level %in% setdiff(LEVEL_LABELS, c("Other", "Alternative")) ~ "Traditional",
      TRUE                                                                ~ "All other"
    )
  )

# Plain-English description for captions/subtitles
alt_examples <- v6 %>%
  filter(school_level == "Alternative") %>%
  distinct(school_type) %>% pull() %>%
  tolower() %>% unique()

alt_hint <- c("continuation","community day","juvenile court","alternative")
alt_found <- alt_hint[alt_hint %in% unique(unlist(str_split(alt_examples, "\\W+")))]
alt_found_pretty <- if (length(alt_found)) paste(alt_found, collapse=", ") else "alternative settings"

all_other_note <- paste0(
  "All other = Alternative (e.g., ", alt_found_pretty,
  ") plus schools with Other grade spans."
)

# --- 5) Build pooled rates by year × school_group × race ----------------------
# All Students baseline
df_total <- v6 %>%
  filter(category_type == "Race/Ethnicity", subgroup == "All Students") %>%
  group_by(academic_year, school_group) %>%
  summarise(
    susp   = sum(total_suspensions, na.rm = TRUE),
    enroll = sum(cumulative_enrollment, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(rate = if_else(enroll > 0, susp/enroll, NA_real_),
         race = "All Students")

# Race-specific
df_race <- v6 %>%
  mutate(race = canon_race_label(subgroup)) %>%
  filter(race %in% setdiff(ALLOWED_RACES, "All Students")) %>%
  group_by(academic_year, school_group, race) %>%
  summarise(
    susp   = sum(total_suspensions, na.rm = TRUE),
    enroll = sum(cumulative_enrollment, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(rate = if_else(enroll > 0, susp/enroll, NA_real_))

df_all_A <- bind_rows(df_total, df_race) %>%
  mutate(
    year_fct  = factor(academic_year, levels = year_levels),
    label_txt = percent(rate, accuracy = 0.1),
    school_group = factor(school_group, levels = c("Traditional","All other"))
  )

if (!KEEP_ALL_STUDENTS_IN_A) df_all_A <- df_all_A %>% filter(race != "All Students")
if (!nrow(df_all_A)) stop("No data to plot for View A.")

# For View B (facet by race), we usually drop "All Students"
df_all_B <- df_all_A %>% filter(if (DROP_ALL_STUDENTS_IN_B) race != "All Students" else TRUE)
if (!nrow(df_all_B)) stop("No data to plot for View B.")

# --- 6) Palettes --------------------------------------------------------------
# Race palette (set All Students black if present)
race_levels <- df_all_A %>% distinct(race) %>% arrange(race) %>% pull(race)
pal_race <- setNames(hue_pal()(length(race_levels)), race_levels)
if ("All Students" %in% names(pal_race)) pal_race["All Students"] <- "#000000"

pal_group <- c("Traditional"="#1f77b4", "All other"="#d62728")

# --- 7) View A: facet = school_group; lines = races ---------------------------
plot_view_A <- function(dat) {
  dat <- dat %>%
    mutate(is_label = ((as.integer(year_fct) - 1L) %% LABEL_EVERY) == 0)
  labels_df <- dat %>% filter(!is.na(rate) & is_label)
  
  ggplot(dat, aes(x = year_fct, y = rate, color = race, group = race)) +
    # bolder All Students line
    geom_line(
      data = dat %>% filter(race == "All Students"),
      color = "black", linewidth = LINE_ALL
    ) +
    geom_point(
      data = dat %>% filter(race == "All Students"),
      color = "black", size = POINT_ALL
    ) +
    # other race lines
    geom_line(linewidth = LINE_RACE) +
    geom_point(size = POINT_RACE) +
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
      title = "Suspension Rates by Race • Traditional vs All other",
      subtitle = "Pooled rate within year × school group × race (All Students in bold black)",
      caption = all_other_note,
      x = NULL, y = "Suspensions per student (%)"
    ) +
    facet_wrap(~ school_group, nrow = 1, scales = "free_y") +
    theme_minimal(base_size = 12) +
    theme(axis.text.x = element_text(angle = 35, hjust = 1),
          legend.position = "bottom",
          plot.margin = margin(12, 28, 12, 12))
}

pA <- plot_view_A(df_all_A)

# --- 8) View B: facet = race (≤2 per image); lines = school_group ------------
# Order races by avg rate for nice grouping, then chunk
race_order <- df_all_B %>%
  group_by(race) %>% summarise(avg_rate = mean(rate, na.rm = TRUE), .groups = "drop") %>%
  arrange(desc(avg_rate)) %>% pull(race)
chunk_id <- ceiling(seq_along(race_order) / MAX_FACETS_PER_IMAGE)
race_chunks <- split(race_order, chunk_id)

plot_view_B_chunk <- function(races, i, n_total) {
  dat <- df_all_B %>%
    filter(race %in% races) %>%
    mutate(
      race = factor(race, levels = races),
      is_label = ((as.integer(year_fct) - 1L) %% LABEL_EVERY) == 0
    )
  if (!nrow(dat)) return(NULL)
  labels_df <- dat %>% filter(!is.na(rate) & is_label)
  
  ggplot(dat, aes(x = year_fct, y = rate, color = school_group, group = school_group)) +
    geom_line(linewidth = 1.05) +
    geom_point(size = 1.8) +
    ggrepel::geom_text_repel(
      data = labels_df,
      aes(label = label_txt, color = school_group),
      size = LABEL_SIZE, show.legend = FALSE,
      direction = "y", nudge_x = NUDGEX <- NUDGE_X,
      box.padding = 0.16, point.padding = 0.16,
      min.segment.length = 0, segment.alpha = SEGMENT_ALPHA, segment.size = 0.25
    ) +
    scale_color_manual(values = pal_group, name = "School group") +
    scale_y_continuous(labels = percent_format(accuracy = 0.1), limits = c(0, NA),
                       expand = expansion(mult = c(0.05, 0.15))) +
    scale_x_discrete(expand = expansion(mult = c(0.05, 0.15))) +
    labs(
      title = paste0("Suspension Rates • Traditional vs All other (Set ", i, " of ", n_total, ")"),
      subtitle = "Pooled rate within year × school group × race",
      caption = all_other_note,
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

# --- 9) Print & save ----------------------------------------------------------
outdir <- here::here("outputs"); dir.create(outdir, showWarnings = FALSE)

# View A (2 facets in one image)
print(pA)
ggsave(file.path(outdir, "rates_by_race_traditional_vs_other_facetGroup.png"),
       pA, width = IMG_W_A, height = IMG_H_A, dpi = IMG_DPI, bg = "white")

# View B (chunked pairs of races; ≤2 facets per image)
for (i in seq_along(race_chunks)) {
  races_i <- race_chunks[[i]]
  message(sprintf("Rendering race set %d: %s", i, paste(races_i, collapse = " & ")))
  pB <- plot_view_B_chunk(races_i, i, length(race_chunks))
  if (is.null(pB)) next
  print(pB)
  fname <- sprintf("rates_by_race_traditional_vs_other_facetRace_set_%02d.png", i)
  ggsave(file.path(outdir, fname), pB,
         width = IMG_W_B, height = IMG_H_B, dpi = IMG_DPI, bg = "white")
}

message("✓ Done. See: ", outdir)