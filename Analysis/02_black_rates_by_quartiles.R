# analysis/02_black_rates_by_quartiles.R
suppressPackageStartupMessages({
  library(here); library(arrow); library(dplyr); library(tidyr)
  library(ggplot2); library(scales); library(ggrepel); library(stringr)
})

# project root
here::here()

# sanity: these should both be TRUE if set up right
stopifnot(file.exists(here::here("Analysis", "01_trends.R")))
stopifnot(file.exists(here::here("R", "utils_keys_filters.R")))

# peek at what's in R/ (helper scripts, etc.)
list.files(here::here("R"))

# load utils; this must exist at <project>/R/utils_keys_filters.R

# ---------- Load ----------
source(here::here("R","utils_keys_filters.R"))

v5 <- read_parquet(here("data-stage","susp_v5.parquet")) %>%
  build_keys() %>%
  filter_campus_only()

# quick guards
need_cols <- c("reporting_category","academic_year","total_suspensions","cumulative_enrollment")
stopifnot(all(need_cols %in% names(v5)))

# quartile columns from file 04 (black & white prop quartiles)
has_black_q <- any(grepl("^black_prop_q", names(v5)))
has_white_q <- any(grepl("^white_prop_q", names(v5)))
if (!has_black_q) stop("Missing black_prop_q4/q_label in v5. Re-run R/04 and downstream.")
if (!has_white_q) stop("Missing white_prop_q4/q_label in v5. Re-run revised R/04 and downstream.")

# normalize label columns using shared helper
if (!"black_prop_q_label" %in% names(v5)) {
  v5 <- v5 %>% mutate(black_prop_q_label = get_quartile_label(black_prop_q4, "Black"))
}
if (!"white_prop_q_label" %in% names(v5)) {
  v5 <- v5 %>% mutate(white_prop_q_label = get_quartile_label(white_prop_q4, "White"))
}

# Year order from TA rows
year_levels <- v5 %>%
  filter(reporting_category == "TA") %>%
  distinct(academic_year) %>% arrange(academic_year) %>% pull(academic_year)

# Reason columns (built in R/06)
reason_cols <- names(v5)[grepl("^prop_susp_", names(v5))]
reason_nice <- function(nm) dplyr::recode(sub("^prop_susp_", "", nm),
                                          "violent_injury"     = "Violent (Injury)",
                                          "violent_no_injury"  = "Violent (No Injury)",
                                          "weapons_possession" = "Weapons",
                                          "illicit_drug"       = "Illicit Drug",
                                          "defiance_only"      = "Willful Defiance",
                                          "other_reasons"      = "Other",
                                          .default = nm
)

# ---------- Core aggregator: pooled rate for RB by a chosen quartile field ----------
# pooled (sum susp) / (sum enroll) within year x quartile — preferred for stability
agg_rb_by_quart <- function(v5, quart_var, quart_title) {
  # 1) TOTAL rate & counts (RB only)
  rb_totals <- v5 %>%
    filter(reporting_category == "RB", !is.na(.data[[quart_var]]), .data[[quart_var]] != "Unknown")%>%
    group_by(academic_year, .data[[quart_var]]) %>%
    summarise(
      susp   = sum(total_suspensions, na.rm = TRUE),
      enroll = sum(cumulative_enrollment, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    mutate(
      rate = dplyr::if_else(enroll > 0, susp / enroll, NA_real_),
      quart = .data[[quart_var]],
      year_fct = factor(academic_year, levels = year_levels)
    )
  
  # 2) Reason-specific RB rate = (prop * total_suspensions) / RB enrollment
  rb_reason <- v5 %>%
    filter(reporting_category == "RB", !is.na(.data[[quart_var]])) %>%
    select(academic_year, cumulative_enrollment, total_suspensions, .data[[quart_var]], all_of(reason_cols)) %>%
    pivot_longer(all_of(reason_cols), names_to = "reason", values_to = "prop") %>%
    mutate(reason_count = prop * total_suspensions) %>%
    group_by(academic_year, .data[[quart_var]], reason) %>%
    summarise(
      susp_reason = sum(reason_count, na.rm = TRUE),
      enroll      = sum(cumulative_enrollment, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    mutate(
      rate      = dplyr::if_else(enroll > 0, susp_reason / enroll, NA_real_),
      quart     = .data[[quart_var]],
      reason_lab = reason_nice(reason),
      year_fct  = factor(academic_year, levels = year_levels)
    )
  
  list(totals = rb_totals, reasons = rb_reason, quart_title = quart_title)
}

# Build both views:
blk_view   <- agg_rb_by_quart(v5, "black_prop_q_label", "School Black Enrollment Quartile")
wht_view   <- agg_rb_by_quart(v5, "white_prop_q_label", "School White Enrollment Quartile")

###sanity check#####
missing_quart <- blk_view$totals %>%
  tidyr::complete(academic_year = year_levels, quart, fill = list(rate = NA_real_)) %>%
  group_by(academic_year) %>%
  summarise(missing = sum(is.na(rate)), .groups = "drop")
if (any(missing_quart$missing > 0)) {
  message("Heads up: some year×quartile combos have no data (common if RB enrollment is zero).")
}
###################
# common colors for quartiles
quart_levels_blk <- get_quartile_label(1:4, "Black")
quart_levels_wht <- get_quartile_label(1:4, "White")
pal_quart <- setNames(scales::hue_pal()(4), quart_levels_blk)

# ---------- Plot helpers ----------

plot_rb_total_rate <- function(df, title, caption = NULL) {
  df2 <- df %>% filter(!is.na(rate))
  ggplot(df2, aes(x = year_fct, y = rate, color = quart, group = quart)) +
    geom_line(linewidth = 1.1) +
    geom_point(size = 1.9) +
    ggrepel::geom_text_repel(
      data = df2,
      aes(label = scales::percent(rate, accuracy = 0.1)),
      size = 2.7, show.legend = FALSE,
      max.overlaps = Inf, direction = "y", nudge_x = 0.12,
      box.padding = 0.12, point.padding = 0.12, min.segment.length = 0
    ) +
    scale_y_continuous(labels = scales::percent_format(accuracy = 0.1),
                       limits = c(0, NA), expand = expansion(mult = c(0, 0.15))) +
    scale_x_discrete(expand = expansion(mult = c(0.02, 0.25))) +
    scale_color_manual(values = pal_quart) +
    labs(
      title = title,
      subtitle = "Black student suspension rate (events per Black student)",
      x = NULL, y = "Suspensions per Black student (%)",
      color = "Quartile",
      caption = caption
    ) +
    theme_minimal(base_size = 12) +
    theme(axis.text.x = element_text(angle = 35, hjust = 1))
}

plot_rb_total_counts <- function(df, title) {
  df2 <- df %>% filter(!is.na(susp))
  ggplot(df2, aes(x = year_fct, y = susp, color = quart, group = quart)) +
    geom_line(linewidth = 1.1) +
    geom_point(size = 1.9) +
    ggrepel::geom_text_repel(
      data = df2,
      aes(label = scales::comma(susp)),
      size = 2.6, show.legend = FALSE,
      max.overlaps = Inf, direction = "y", nudge_x = 0.12,
      box.padding = 0.12, point.padding = 0.12, min.segment.length = 0
    ) +
    scale_y_continuous(labels = scales::comma,
                       expand = expansion(mult = c(0, 0.15))) +
    scale_x_discrete(expand = expansion(mult = c(0.02, 0.25))) +
    scale_color_manual(values = pal_quart) +
    labs(
      title = title,
      subtitle = "Total suspensions (events) for Black students",
      x = NULL, y = "Total suspensions (events)",
      color = "Quartile"
    ) +
    theme_minimal(base_size = 12) +
    theme(axis.text.x = element_text(angle = 35, hjust = 1))
}

plot_rb_reason_rates <- function(df, title) {
  df2 <- df %>% filter(!is.na(rate))
  ggplot(df2, aes(x = year_fct, y = rate, color = quart, group = quart)) +
    geom_line(linewidth = 0.95) +
    geom_point(size = 1.6) +
    ggrepel::geom_text_repel(
      data = df2,
      aes(label = scales::percent(rate, accuracy = 0.01)),
      size = 2.4, show.legend = FALSE,
      max.overlaps = Inf, direction = "y", nudge_x = 0.1,
      box.padding = 0.1, point.padding = 0.1, min.segment.length = 0
    ) +
    scale_y_continuous(labels = scales::percent_format(accuracy = 0.01),
                       limits = c(0, NA), expand = expansion(mult = c(0, 0.12))) +
    scale_x_discrete(expand = expansion(mult = c(0.02, 0.22))) +
    scale_color_manual(values = pal_quart) +
    labs(
      title = title,
      subtitle = "Reason-specific suspension rates (events per Black student)",
      x = NULL, y = "Rate (%)", color = "Quartile"
    ) +
    facet_wrap(~ reason_lab, scales = "free_y") +
    theme_minimal(base_size = 12) +
    theme(axis.text.x = element_text(angle = 35, hjust = 1),
          legend.position = "bottom")
}

# ---------- BLACK quartiles ----------
blk_totals <- blk_view$totals %>%
  mutate(quart = factor(quart, levels = quart_levels_blk))

blk_reasons <- blk_view$reasons %>%
  mutate(quart = factor(quart, levels = quart_levels_blk))

p_blk_rate   <- plot_rb_total_rate(blk_totals,
                                   title = "Black Students • Suspension Rate by Year and School Black-Enrollment Quartile",
                                   caption = "Quartiles are based on each school-year’s % Black enrollment (RB/TA)."
)
p_blk_counts <- plot_rb_total_counts(blk_totals,
                                     title = "Black Students • Total Suspensions by Year and School Black-Enrollment Quartile"
)
p_blk_reason <- plot_rb_reason_rates(blk_reasons,
                                     title = "Black Students • Reason-Specific Rates by Year and School Black-Enrollment Quartile"
)

# ---------- WHITE quartiles ----------
# reuse the same palette but re-level to white labels for clean legends
pal_quart <- setNames(scales::hue_pal()(4), quart_levels_wht)

wht_totals <- wht_view$totals %>%
  mutate(quart = factor(quart, levels = quart_levels_wht))
wht_reasons <- wht_view$reasons %>%
  mutate(quart = factor(quart, levels = quart_levels_wht))

p_wht_rate   <- plot_rb_total_rate(wht_totals,
                                   title = "Black Students • Suspension Rate by Year and School White-Enrollment Quartile",
                                   caption = "Quartiles are based on each school-year’s % White enrollment (RW/TA)."
)
p_wht_counts <- plot_rb_total_counts(wht_totals,
                                     title = "Black Students • Total Suspensions by Year and School White-Enrollment Quartile"
)
p_wht_reason <- plot_rb_reason_rates(wht_reasons,
                                     title = "Black Students • Reason-Specific Rates by Year and School White-Enrollment Quartile"
)

# ---------- Print & Save ----------
print(p_blk_rate);   print(p_blk_counts);   print(p_blk_reason)
print(p_wht_rate);   print(p_wht_counts);   print(p_wht_reason)

dir.create(here("outputs"), showWarnings = FALSE)
ggsave(here("outputs","02_rb_rate_by_black_quart.png"),   p_blk_rate,   width = 11, height = 6.5, dpi = 300, bg = "white")
ggsave(here("outputs","02_rb_counts_by_black_quart.png"), p_blk_counts, width = 11, height = 6.5, dpi = 300, bg = "white")
ggsave(here("outputs","02_rb_reason_by_black_quart.png"), p_blk_reason, width = 12, height = 7.5, dpi = 300, bg = "white")

ggsave(here("outputs","02_rb_rate_by_white_quart.png"),   p_wht_rate,   width = 11, height = 6.5, dpi = 300, bg = "white")
ggsave(here("outputs","02_rb_counts_by_white_quart.png"), p_wht_counts, width = 11, height = 6.5, dpi = 300, bg = "white")
ggsave(here("outputs","02_rb_reason_by_white_quart.png"), p_wht_reason, width = 12, height = 7.5, dpi = 300, bg = "white")


##sanity check
missing_quart <- blk_view$totals %>%
  tidyr::complete(academic_year = year_levels, quart, fill = list(rate = NA_real_)) %>%
  group_by(academic_year) %>%
  summarise(missing = sum(is.na(rate)), .groups = "drop")
if (any(missing_quart$missing > 0)) {
  message("Heads up: some year×quartile combos have no data (common if RB enrollment is zero).")
}

