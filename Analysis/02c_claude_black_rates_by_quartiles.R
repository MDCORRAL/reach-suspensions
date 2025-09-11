# R/02c_analysis_black_student_rates_final.R
# Final analysis of Black student suspension rates by school racial composition with improved data labels.

# --- 1) Setup -----------------------------------------------------------------
suppressPackageStartupMessages({
  library(here); library(arrow); library(dplyr); library(ggplot2)
  library(tidyr); library(scales); library(patchwork); library(ggrepel)
})

# keys + campus filter (handles 0000000/0000001)
source(here::here("R","utils_keys_filters.R"))

theme_set(theme_minimal(base_size = 12))

# Palettes (keep your originals)
black_quartile_colors <- c(
  "Q1 (Lowest % Black)" = "#FEE5D9", "Q2" = "#FCAE91",
  "Q3" = "#FB6A4A", "Q4 (Highest % Black)" = "#CB181D"
)
white_quartile_colors <- c(
  "Q1 (Lowest % White)" = "#EFF3FF", "Q2" = "#BDD7E7",
  "Q3" = "#6BAED6", "Q4 (Highest % White)" = "#08519C"
)

# --- 2) Load + guards ---------------------------------------------------------
message("Loading data...")
v5 <- arrow::read_parquet(here::here("data-stage","susp_v5.parquet")) %>%
  build_keys() %>%            # adds cds_district, cds_school (canonical keys)
  filter_campus_only()        # drops fake schools + special codes

need_cols <- c("reporting_category","academic_year",
               "total_suspensions","cumulative_enrollment",
               "black_prop_q4","white_prop_q4")
missing <- setdiff(need_cols, names(v5))
if (length(missing)) stop("Missing in v5: ", paste(missing, collapse=", "))

# make sure readable quartile labels exist
lbl_q <- function(q4, who) dplyr::case_when(
  is.na(q4) ~ "Unknown",
  q4 == 1L ~ paste0("Q1 (Lowest % ", who, ")"),
  q4 == 2L ~ "Q2",
  q4 == 3L ~ "Q3",
  q4 == 4L ~ paste0("Q4 (Highest % ", who, ")")
)
if (!"black_prop_q_label" %in% names(v5)) v5 <- v5 %>% mutate(black_prop_q_label = lbl_q(black_prop_q4, "Black"))
if (!"white_prop_q_label" %in% names(v5)) v5 <- v5 %>% mutate(white_prop_q_label = lbl_q(white_prop_q4, "White"))

# order x-axis by TA years that actually have enrollment
year_levels <- v5 %>%
  filter(reporting_category == "TA", cumulative_enrollment > 0) %>%
  distinct(academic_year) %>% arrange(academic_year) %>% pull(academic_year)

# restrict to Black rows for all calculations
black_students_data <- v5 %>% filter(reporting_category == "RB")

# Reason columns: prefer *_count columns if present; otherwise derive from proportions
has_count_cols <- all(c(
  "suspension_count_defiance_only",
  "suspension_count_violent_incident_injury",
  "suspension_count_violent_incident_no_injury",
  "suspension_count_weapons_possession",
  "suspension_count_illicit_drug_related",
  "suspension_count_other_reasons"
) %in% names(v5))

prop_cols <- grep("^prop_susp_", names(v5), value = TRUE)

nice_reason <- function(x) recode(x,
                                  "Defiance"                = "Defiance",
                                  "Violent Injury"          = "Violent (Injury)",
                                  "Violent No Injury"       = "Violent (No Injury)",
                                  "Weapons"                 = "Weapons",
                                  "Drugs"                   = "Illicit Drug",
                                  "Other"                   = "Other",
                                  .default = x
)

# --- 3) Total Rate Plot helper -----------------------------------------------
create_total_rate_plot <- function(data, group_var, colors, title_suffix, legend_title) {
  gsym <- rlang::ensym(group_var)
  
  plot_data <- data %>%
    filter(!is.na(!!gsym), !!gsym != "Unknown") %>%
    group_by(academic_year, !!gsym) %>%
    summarise(
      total_suspensions = sum(total_suspensions, na.rm = TRUE),
      total_enrollment  = sum(cumulative_enrollment, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    mutate(
      suspension_rate = if_else(total_enrollment > 0, total_suspensions / total_enrollment, NA_real_),
      year_fct = factor(academic_year, levels = year_levels)
    )
  
  df2 <- plot_data %>% filter(!is.na(suspension_rate))
  ggplot(df2, aes(x = year_fct, y = suspension_rate, color = !!gsym, group = !!gsym)) +
    geom_line(linewidth = 1.2) +
    geom_point(size = 2.5) +
    ggrepel::geom_text_repel(
      aes(label = scales::percent(suspension_rate, accuracy = 0.1)),
      color = "black", size = 3.2, segment.color = "grey60",
      box.padding = 0.4, max.overlaps = Inf, nudge_x = 0.12
    ) +
    scale_color_manual(values = colors, name = legend_title) +
    scale_y_continuous(labels = percent_format(accuracy = 0.1),
                       expand = expansion(mult = c(0.05, 0.18))) +
    labs(
      title = paste("Black Student Suspension Rate by", title_suffix),
      subtitle = "Suspension events per Black student (pooled rate)",
      x = "Academic Year", y = "Suspensions per Black student (%)"
    ) +
    theme(legend.position = "bottom",
          plot.title = element_text(face = "bold", size = 14),
          axis.text.x = element_text(angle = 45, hjust = 1))
}

# --- 4) Reason-specific Rate Plot helper --------------------------------------
create_category_rate_plot <- function(data, group_var, colors, title_suffix, legend_title) {
  gsym <- rlang::ensym(group_var)
  
  if (has_count_cols) {
    # Use provided *_count columns
    plot_data <- data %>%
      filter(!is.na(!!gsym)) %>%
      group_by(academic_year, !!gsym) %>%
      summarise(
        Defiance          = sum(suspension_count_defiance_only, na.rm = TRUE),
        `Violent Injury`  = sum(suspension_count_violent_incident_injury, na.rm = TRUE),
        `Violent No Injury` = sum(suspension_count_violent_incident_no_injury, na.rm = TRUE),
        Weapons           = sum(suspension_count_weapons_possession, na.rm = TRUE),
        Drugs             = sum(suspension_count_illicit_drug_related, na.rm = TRUE),
        Other             = sum(suspension_count_other_reasons, na.rm = TRUE),
        total_enrollment  = sum(cumulative_enrollment, na.rm = TRUE),
        .groups = "drop"
      ) %>%
      pivot_longer(
        cols = c(Defiance, `Violent Injury`, `Violent No Injury`, Weapons, Drugs, Other),
        names_to = "category", values_to = "suspension_count"
      ) %>%
      mutate(reason_rate = if_else(total_enrollment > 0, suspension_count / total_enrollment, NA_real_),
             reason_lab = nice_reason(category),
             year_fct = factor(academic_year, levels = year_levels))
  } else if (length(prop_cols) > 0) {
    # Derive counts from proportions × total_suspensions
    plot_data <- data %>%
      filter(!is.na(!!gsym)) %>%
      select(academic_year, !!gsym, total_suspensions, cumulative_enrollment, all_of(prop_cols)) %>%
      pivot_longer(all_of(prop_cols), names_to = "prop_name", values_to = "prop") %>%
      mutate(reason = recode(sub("^prop_susp_", "", prop_name),
                             "defiance_only"        = "Defiance",
                             "violent_injury"       = "Violent Injury",
                             "violent_no_injury"    = "Violent No Injury",
                             "weapons_possession"   = "Weapons",
                             "illicit_drug"         = "Drugs",
                             "other_reasons"        = "Other",
                             .default = prop_name),
             reason_count = prop * total_suspensions) %>%
      group_by(academic_year, !!gsym, reason) %>%
      summarise(
        suspension_count = sum(reason_count, na.rm = TRUE),
        total_enrollment = sum(cumulative_enrollment, na.rm = TRUE),
        .groups = "drop"
      ) %>%
      mutate(reason_rate = if_else(total_enrollment > 0, suspension_count / total_enrollment, NA_real_),
             reason_lab  = nice_reason(reason),
             year_fct    = factor(academic_year, levels = year_levels))
  } else {
    stop("No reason data available: neither *_count nor prop_susp_* columns exist in v5.")
  }
  
  df2 <- plot_data %>% filter(!is.na(reason_rate))
  ggplot(df2, aes(x = year_fct, y = reason_rate, color = !!gsym, group = !!gsym)) +
    geom_line(linewidth = 0.9) +
    geom_point(size = 1.8) +
    ggrepel::geom_text_repel(
      aes(label = scales::percent(reason_rate, accuracy = 0.1)),
      color = "black", size = 2.8, segment.color = "grey60",
      box.padding = 0.35, max.overlaps = Inf, nudge_x = 0.1
    ) +
    facet_wrap(~ reason_lab, scales = "free_y", ncol = 3) +
    scale_color_manual(values = colors, name = legend_title) +
    scale_y_continuous(labels = percent_format(accuracy = 0.1),
                       expand = expansion(mult = c(0, 0.15))) +
    labs(
      title = paste("Black Student Suspension Rates by Category and", title_suffix),
      subtitle = "Events in reason per Black student (pooled rate)",
      x = "Academic Year", y = "Rate (%)"
    ) +
    theme(legend.position = "bottom",
          plot.title = element_text(face = "bold", size = 14),
          axis.text.x = element_text(angle = 45, hjust = 1),
          strip.text = element_text(face = "bold"))
}

# --- 5) Generate Plots --------------------------------------------------------
message("Generating plots...")

# by Black-enrollment quartiles
p1_total_black <- create_total_rate_plot(
  black_students_data, "black_prop_q_label", black_quartile_colors,
  "School Black Student Proportion", "School % Black Students"
)
p2_categories_black <- create_category_rate_plot(
  black_students_data, "black_prop_q_label", black_quartile_colors,
  "School Black Student Proportion", "School % Black Students"
)

# by White-enrollment quartiles
p3_total_white <- create_total_rate_plot(
  black_students_data, "white_prop_q_label", white_quartile_colors,
  "School White Student Proportion", "School % White Students"
)
p4_categories_white <- create_category_rate_plot(
  black_students_data, "white_prop_q_label", white_quartile_colors,
  "School White Student Proportion", "School % White Students"
)
# Combine plots into a single layout 

# ---- optional: drop "Unknown" quartiles from all plots ----------------------
drop_unknown <- function(p) p + scale_color_discrete(drop = TRUE)
p1_total_black    <- p1_total_black    |> drop_unknown()
p2_categories_black <- p2_categories_black |> drop_unknown()
p3_total_white    <- p3_total_white    |> drop_unknown()
p4_categories_white <- p4_categories_white |> drop_unknown()

# ---- combine with patchwork --------------------------------------------------
# top row = total rates; bottom row = reason rates
top_row    <- p1_total_black | p3_total_white
bottom_row <- p2_categories_black | p4_categories_white

final_grid <- (top_row / bottom_row) +
  plot_layout(
    heights = c(1, 2),
    guides = "collect"
  ) +
  plot_annotation(
    title = "Black Student Suspension Rates by School Racial Composition",
    subtitle = "Campus-only data; special codes removed; pooled rates (sum of events ÷ sum of enrollment)",
    theme = theme(
      plot.title = element_text(face = "bold", size = 15),
      legend.position = "bottom"
    )
  )

# ---- save -------------------------------------------------------------------
dir.create(here::here("outputs"), showWarnings = FALSE)

ggsave(here::here("outputs","02c_black_by_blackQuart_total.png"),
       p1_total_black, width = 10, height = 7, dpi = 300, bg = "white")

ggsave(here::here("outputs","02c_black_by_blackQuart_reasons.png"),
       p2_categories_black, width = 12, height = 8, dpi = 300, bg = "white")

ggsave(here::here("outputs","02c_black_by_whiteQuart_total.png"),
       p3_total_white, width = 10, height = 7, dpi = 300, bg = "white")

ggsave(here::here("outputs","02c_black_by_whiteQuart_reasons.png"),
       p4_categories_white, width = 12, height = 8, dpi = 300, bg = "white")

ggsave(here::here("outputs","02c_black_rates_all_panels.png"),
       final_grid, width = 16, height = 14, dpi = 300, bg = "white")