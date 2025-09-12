# analysis/02b_black_reason_rates_and_shares_by_quartiles.R
suppressPackageStartupMessages({
  library(here); library(arrow); library(dplyr); library(tidyr)
  library(ggplot2); library(scales); library(ggrepel); library(stringr)
})

# ------------ knobs you can tweak -------------
LABEL_SIZE_RATE   <- 2.7
LABEL_SIZE_COUNT  <- 2.6
LABEL_SIZE_REASON <- 2.4
NUDGE_X           <- 0.12

# ------------ load & guards -------------------
suppressPackageStartupMessages({
  library(here); library(arrow); library(dplyr); library(tidyr)
  library(ggplot2); library(scales); library(ggrepel); library(stringr)
})

# use the shared key/filter helpers
source(here::here("R","utils_keys_filters.R"))

# ------------ load & guards -------------------
v5 <- read_parquet(here("data-stage","susp_v6_long.parquet")) %>%
  build_keys() %>%
  filter_campus_only()   # drops 0000000/0000001 and non-school rows

need_cols <- c(
  "subgroup","academic_year",
  "total_suspensions","cumulative_enrollment",
  "black_prop_q","black_prop_q_label","white_prop_q","white_prop_q_label"
)
missing <- setdiff(need_cols, names(v5))
if (length(missing)) stop("Missing in v5: ", paste(missing, collapse=", "))

# ensure readable labels exist via shared helper
if (!"black_prop_q_label" %in% names(v5))  v5 <- v5 %>% mutate(black_prop_q_label = get_quartile_label(black_prop_q, "Black"))
if (!"white_prop_q_label" %in% names(v5))  v5 <- v5 %>% mutate(white_prop_q_label = get_quartile_label(white_prop_q, "White"))

# year order (from TA rows with positive enrollment)
year_levels <- v5 %>%
  filter(category_type == "Race/Ethnicity", subgroup == "All Students", cumulative_enrollment > 0) %>%
  distinct(academic_year) %>% arrange(academic_year) %>% pull(academic_year)

# reason columns (skip reason plots gracefully if none found)
reason_cols <- names(v5)[grepl("^prop_susp_", names(v5))]
if (length(reason_cols) == 0) {
  message("No reason proportion columns found; reason plots will be skipped.")
}

# ------------ core aggregators for RB ---------
# pooled rates: (sum events) / (sum RB enrollment) within year × quartile
agg_rb_rates_and_counts <- function(v5, quart_var) {
  # total RB rate & counts
  totals <- v5 %>%
    filter(subgroup == "Black/African American", !is.na(.data[[quart_var]]), .data[[quart_var]] != "Unknown") %>%
    group_by(academic_year, .data[[quart_var]]) %>%
    summarise(
      susp   = sum(total_suspensions, na.rm = TRUE),
      enroll = sum(cumulative_enrollment, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    mutate(
      rate = if_else(enroll > 0, susp / enroll, NA_real_),
      quart   = .data[[quart_var]],
      year_fct = factor(academic_year, levels = year_levels)
    )
  
  # reason-specific RB rate
    reasons_rate <- v5 %>%
      filter(subgroup == "Black/African American", !is.na(.data[[quart_var]])) %>%
      select(academic_year, .data[[quart_var]], cumulative_enrollment, total_suspensions, all_of(reason_cols)) %>%
      pivot_longer(all_of(reason_cols), names_to = "reason", values_to = "prop") %>%
      mutate(
        reason = sub("^prop_susp_", "", reason),
        reason_events = prop * total_suspensions
      ) %>%
      group_by(academic_year, .data[[quart_var]], reason) %>%
      summarise(
        susp_reason = sum(reason_events, na.rm = TRUE),
        enroll      = sum(cumulative_enrollment, na.rm = TRUE),
        .groups = "drop"
      ) %>%
      add_reason_label() %>%
      mutate(
        rate      = if_else(enroll > 0, susp_reason / enroll, NA_real_),
        quart     = .data[[quart_var]],
        year_fct  = factor(academic_year, levels = year_levels)
      )
  
  list(totals = totals, reasons_rate = reasons_rate)
}

# shares: within year × quartile, fraction of RB suspensions by reason
agg_rb_reason_shares <- function(v5, quart_var) {
    rb_reason_share <- v5 %>%
      filter(subgroup == "Black/African American", !is.na(.data[[quart_var]])) %>%
      select(academic_year, .data[[quart_var]], total_suspensions, all_of(reason_cols)) %>%
      pivot_longer(all_of(reason_cols), names_to = "reason", values_to = "prop") %>%
      mutate(
        reason = sub("^prop_susp_", "", reason),
        reason_events = prop * total_suspensions
      ) %>%
      group_by(academic_year, .data[[quart_var]], reason) %>%
      summarise(total_reason_susp = sum(reason_events, na.rm = TRUE), .groups = "drop") %>%
      group_by(academic_year, .data[[quart_var]]) %>%
      mutate(total_rb_susp = sum(total_reason_susp, na.rm = TRUE)) %>%
      ungroup() %>%
      add_reason_label() %>%
      mutate(
        share     = if_else(total_rb_susp > 0, total_reason_susp / total_rb_susp, NA_real_),
        quart     = .data[[quart_var]],
        year_fct  = factor(academic_year, levels = year_levels)
      )
  rb_reason_share
}

# ------------ build both views --------------
blk_view  <- agg_rb_rates_and_counts(v5, "black_prop_q_label")
wht_view  <- agg_rb_rates_and_counts(v5, "white_prop_q_label")
blk_share <- agg_rb_reason_shares(v5, "black_prop_q_label")
wht_share <- agg_rb_reason_shares(v5, "white_prop_q_label")

# drop Unknown quartile from plots
keep_blk <- get_quartile_label(1:4, "Black")
keep_wht <- get_quartile_label(1:4, "White")

blk_totals      <- blk_view$totals       %>% filter(quart %in% keep_blk) %>% mutate(quart = factor(quart, levels = keep_blk))
blk_reasons_rt  <- blk_view$reasons_rate %>% filter(quart %in% keep_blk) %>% mutate(quart = factor(quart, levels = keep_blk))
blk_reasons_sh  <- blk_share             %>% filter(quart %in% keep_blk) %>% mutate(quart = factor(quart, levels = keep_blk))

wht_totals      <- wht_view$totals       %>% filter(quart %in% keep_wht) %>% mutate(quart = factor(quart, levels = keep_wht))
wht_reasons_rt  <- wht_view$reasons_rate %>% filter(quart %in% keep_wht) %>% mutate(quart = factor(quart, levels = keep_wht))
wht_reasons_sh  <- wht_share             %>% filter(quart %in% keep_wht) %>% mutate(quart = factor(quart, levels = keep_wht))

# palettes per-legend
pal_blk <- setNames(hue_pal()(4), keep_blk)
pal_wht <- setNames(hue_pal()(4), keep_wht)

# ------------ plotting helpers -------------
plot_rate_lines <- function(df, pal, title, subtitle, ylab, caption=NULL) {
  labs_df <- df %>% filter(!is.na(rate))
  ggplot(df, aes(x = year_fct, y = rate, color = quart, group = quart)) +
    geom_line(linewidth = 1.05) +
    geom_point(size = 1.8) +
    ggrepel::geom_text_repel(
      data = labs_df,
      aes(label = percent(rate, accuracy = 0.1)),
      size = LABEL_SIZE_RATE, show.legend = FALSE,
      max.overlaps = Inf, direction = "y", nudge_x = NUDGE_X,
      box.padding = 0.14, point.padding = 0.14, min.segment.length = 0
    ) +
    scale_color_manual(values = pal) +
    scale_y_continuous(labels = percent_format(accuracy = 0.1), limits = c(0, NA),
                       expand = expansion(mult = c(0, 0.14))) +
    scale_x_discrete(expand = expansion(mult = c(0.02, 0.24))) +
    labs(title = title, subtitle = subtitle, x = NULL, y = ylab, color = "Quartile", caption = caption) +
    theme_minimal(base_size = 12) +
    theme(axis.text.x = element_text(angle = 35, hjust = 1))
}

plot_count_lines <- function(df, pal, title, subtitle, ylab) {
  labs_df <- df %>% filter(!is.na(susp))
  ggplot(df, aes(x = year_fct, y = susp, color = quart, group = quart)) +
    geom_line(linewidth = 1.05) +
    geom_point(size = 1.8) +
    ggrepel::geom_text_repel(
      data = labs_df,
      aes(label = comma(susp)),
      size = LABEL_SIZE_COUNT, show.legend = FALSE,
      max.overlaps = Inf, direction = "y", nudge_x = NUDGE_X,
      box.padding = 0.14, point.padding = 0.14, min.segment.length = 0
    ) +
    scale_color_manual(values = pal) +
    scale_y_continuous(labels = comma, expand = expansion(mult = c(0, 0.14))) +
    scale_x_discrete(expand = expansion(mult = c(0.02, 0.24))) +
    labs(title = title, subtitle = subtitle, x = NULL, y = ylab, color = "Quartile") +
    theme_minimal(base_size = 12) +
    theme(axis.text.x = element_text(angle = 35, hjust = 1))
}

plot_reason_rates <- function(df, pal, title, subtitle) {
  labs_df <- df %>% filter(!is.na(rate))
  ggplot(df, aes(x = year_fct, y = rate, color = quart, group = quart)) +
    geom_line(linewidth = 0.95) +
    geom_point(size = 1.6) +
    ggrepel::geom_text_repel(
      data = labs_df,
      aes(label = percent(rate, accuracy = 0.01)),
      size = LABEL_SIZE_REASON, show.legend = FALSE,
      max.overlaps = Inf, direction = "y", nudge_x = 0.1,
      box.padding = 0.10, point.padding = 0.10, min.segment.length = 0
    ) +
    scale_color_manual(values = pal) +
    scale_y_continuous(labels = percent_format(accuracy = 0.01), limits = c(0, NA),
                       expand = expansion(mult = c(0, 0.12))) +
    scale_x_discrete(expand = expansion(mult = c(0.02, 0.22))) +
    labs(title = title, subtitle = subtitle, x = NULL, y = "Rate (%)", color = "Quartile") +
    facet_wrap(~ reason_lab, scales = "free_y") +
    theme_minimal(base_size = 12) +
    theme(axis.text.x = element_text(angle = 35, hjust = 1),
          legend.position = "bottom")
}

plot_reason_shares <- function(df, pal, title, subtitle) {
  labs_df <- df %>% filter(!is.na(share))
  ggplot(df, aes(x = year_fct, y = share, color = quart, group = quart)) +
    geom_line(linewidth = 0.95) +
    geom_point(size = 1.6) +
    ggrepel::geom_text_repel(
      data = labs_df,
      aes(label = percent(share, accuracy = 0.1)),
      size = LABEL_SIZE_REASON, show.legend = FALSE,
      max.overlaps = Inf, direction = "y", nudge_x = 0.1,
      box.padding = 0.10, point.padding = 0.10, min.segment.length = 0
    ) +
    scale_color_manual(values = pal) +
    scale_y_continuous(labels = percent_format(accuracy = 0.1), limits = c(0, NA),
                       expand = expansion(mult = c(0, 0.12))) +
    scale_x_discrete(expand = expansion(mult = c(0.02, 0.22))) +
    labs(title = title, subtitle = subtitle, x = NULL, y = "Share of Black suspensions", color = "Quartile") +
    facet_wrap(~ reason_lab, scales = "free_y") +
    theme_minimal(base_size = 12) +
    theme(axis.text.x = element_text(angle = 35, hjust = 1),
          legend.position = "bottom")
}

# ------------ BLACK-quartile plots ----------
p_blk_rate   <- plot_rate_lines(
  blk_totals, pal_blk,
  title    = "Black Students • Suspension Rate by Year & School Black-Enrollment Quartile",
  subtitle = "Suspension events per Black student (pooled rate)",
  ylab     = "Suspensions per Black student (%)",
  caption  = "Quartiles computed within year; denominator is Black enrollment."
)
p_blk_count  <- plot_count_lines(
  blk_totals, pal_blk,
  title    = "Black Students • Total Suspensions by Year & School Black-Enrollment Quartile",
  subtitle = "Total suspension events (RB rows pooled)",
  ylab     = "Events"
)
p_blk_r_rt   <- plot_reason_rates(
  blk_reasons_rt, pal_blk,
  title    = "Black Students • Reason-Specific Rates by Year & School Black-Enrollment Quartile",
  subtitle = "Events in reason ÷ Black enrollment"
)
p_blk_r_sh   <- plot_reason_shares(
  blk_reasons_sh, pal_blk,
  title    = "Black Students • Reason Mix by Year & School Black-Enrollment Quartile",
  subtitle = "Reason events ÷ all Black suspensions (within year × quartile)"
)

# ------------ WHITE-quartile plots ----------
p_wht_rate   <- plot_rate_lines(
  wht_totals, pal_wht,
  title    = "Black Students • Suspension Rate by Year & School White-Enrollment Quartile",
  subtitle = "Suspension events per Black student (pooled rate)",
  ylab     = "Suspensions per Black student (%)",
  caption  = "Quartiles computed within year; denominator is Black enrollment."
)
p_wht_count  <- plot_count_lines(
  wht_totals, pal_wht,
  title    = "Black Students • Total Suspensions by Year & School White-Enrollment Quartile",
  subtitle = "Total suspension events (RB rows pooled)",
  ylab     = "Events"
)
p_wht_r_rt   <- plot_reason_rates(
  wht_reasons_rt, pal_wht,
  title    = "Black Students • Reason-Specific Rates by Year & School White-Enrollment Quartile",
  subtitle = "Events in reason ÷ Black enrollment"
)
p_wht_r_sh   <- plot_reason_shares(
  wht_reasons_sh, pal_wht,
  title    = "Black Students • Reason Mix by Year & School White-Enrollment Quartile",
  subtitle = "Reason events ÷ all Black suspensions (within year × quartile)"
)

# ------------ print & save ------------------
print(p_blk_rate); print(p_blk_count); print(p_blk_r_rt); print(p_blk_r_sh)
print(p_wht_rate); print(p_wht_count); print(p_wht_r_rt); print(p_wht_r_sh)

dir.create(here("outputs"), showWarnings = FALSE)
ggsave(here("outputs","02b_rb_rate_by_black_quart.png"),   p_blk_rate,  width=11, height=6.5, dpi=300, bg="white")
ggsave(here("outputs","02b_rb_counts_by_black_quart.png"), p_blk_count, width=11, height=6.5, dpi=300, bg="white")
ggsave(here("outputs","02b_rb_reasonRATE_by_black_quart.png"), p_blk_r_rt, width=12, height=7.5, dpi=300, bg="white")
ggsave(here("outputs","02b_rb_reasonSHARE_by_black_quart.png"), p_blk_r_sh, width=12, height=7.5, dpi=300, bg="white")

ggsave(here("outputs","02b_rb_rate_by_white_quart.png"),   p_wht_rate,  width=11, height=6.5, dpi=300, bg="white")
ggsave(here("outputs","02b_rb_counts_by_white_quart.png"), p_wht_count, width=11, height=6.5, dpi=300, bg="white")
ggsave(here("outputs","02b_rb_reasonRATE_by_white_quart.png"), p_wht_r_rt, width=12, height=7.5, dpi=300, bg="white")
ggsave(here("outputs","02b_rb_reasonSHARE_by_white_quart.png"), p_wht_r_sh, width=12, height=7.5, dpi=300, bg="white")
