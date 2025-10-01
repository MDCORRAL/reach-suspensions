# graph_scripts/09_nonrace_demographic_trends.R
# Generate UCLA brand-aligned statewide suspension trend graphics for
# non-race demographic categories (e.g., disability, socioeconomic,
# migrant, foster, homeless, and English learner groups).

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(forcats)
  library(ggplot2)
  library(ggrepel)
  library(glue)
  library(here)
  library(janitor)
  library(purrr)
  library(readr)
  library(scales)
  library(stringr)
})

try(here::i_am("graph_scripts/09_nonrace_demographic_trends.R"), silent = TRUE)

# -----------------------------------------------------------------------------
# Configuration ----------------------------------------------------------------
# -----------------------------------------------------------------------------

source(here("R", "utils_keys_filters.R"))

DATA_STAGE <- here("data-stage")
OTH_LONG <- file.path(DATA_STAGE, "oth_long.parquet")

stopifnot(file.exists(OTH_LONG))

OUTPUT_DIR <- here("outputs", "graphs", "nonrace_demographics")
dir.create(OUTPUT_DIR, recursive = TRUE, showWarnings = FALSE)

# UCLA brand palette -----------------------------------------------------------
ucla_colors <- c(
  "Darkest Blue" = "#003B5C",
  "Darker Blue" = "#005587",
  "UCLA Blue" = "#2774AE",
  "Lighter Blue" = "#8BB8E8",
  "UCLA Gold" = "#FFD100",
  "Darker Gold" = "#FFC72C",
  "Darkest Gold" = "#FFB81C",
  "Purple" = "#8A69D4",
  "Green" = "#00FF87",
  "Magenta" = "#FF00A5",
  "Cyan" = "#00FFFF"
)

all_students_color <- "#C8102E"

safe_div <- function(num, den) ifelse(is.na(den) | den == 0, NA_real_, num / den)

ucla_theme <- function(base_size = 12, base_family = NULL) {
  ggplot2::theme_minimal(base_size = base_size, base_family = base_family) +
    ggplot2::theme(
      plot.title = element_text(
        face = "bold", size = base_size + 6, hjust = 0,
        color = ucla_colors[["Darkest Blue"]]
      ),
      plot.subtitle = element_text(
        size = base_size + 1, margin = margin(b = 10),
        color = ucla_colors[["Darkest Blue"]]
      ),
      plot.caption = element_text(size = base_size - 1, color = "#5A5A5A", hjust = 0),
      axis.title = element_text(color = ucla_colors[["Darkest Blue"]], face = "bold"),
      axis.text = element_text(color = ucla_colors[["Darkest Blue"]]),
      panel.grid.major = element_line(color = "#DFE2E5", linewidth = 0.4),
      panel.grid.minor = element_blank(),
      legend.title = element_text(face = "bold", color = ucla_colors[["Darkest Blue"]]),
      legend.text = element_text(color = ucla_colors[["Darkest Blue"]]),
      legend.position = "bottom",
      plot.background = element_rect(fill = "white", color = NA),
      panel.background = element_rect(fill = "white", color = NA),
      strip.text = element_text(face = "bold", color = ucla_colors[["Darkest Blue"]])
    )
}

# -----------------------------------------------------------------------------
# Load and prepare data --------------------------------------------------------
# -----------------------------------------------------------------------------

oth_long <- read_parquet(OTH_LONG) %>%
  clean_names() %>%
  build_keys() %>%
  mutate(
    academic_year = as.character(academic_year),

    subgroup = str_squish(as.character(subgroup)),
    category_type = str_squish(as.character(category_type)),
    cumulative_enrollment = readr::parse_number(as.character(cumulative_enrollment)),
    total_suspensions = readr::parse_number(as.character(total_suspensions))
  ) %>%
  filter(
    !is.na(academic_year),
    !is.na(school_code),
    !school_code %in% SPECIAL_SCHOOL_CODES,
    !is.na(category_type),
    !is.na(subgroup)
  )

analytic_data <- oth_long

year_levels <- sort(unique(analytic_data$academic_year))

calc_summary <- function(df) {
  df %>%
    group_by(category_type, academic_year, subgroup) %>%
    summarise(
      n_schools = n_distinct(school_code),
      total_enrollment = sum(cumulative_enrollment, na.rm = TRUE),
      total_suspensions = sum(total_suspensions, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    mutate(
      pooled_rate = safe_div(total_suspensions, total_enrollment),
      pooled_rate_pct = percent(pooled_rate, accuracy = 0.1)
    )
}

summary_by_demo <- calc_summary(analytic_data)

all_students_summary <- summary_by_demo %>%
  filter(category_type == "Total", subgroup == "All Students") %>%
  transmute(
    academic_year,
    subgroup,
    n_schools,
    total_enrollment,
    total_suspensions,
    pooled_rate,
    pooled_rate_pct
  )

category_subgroups <- list(
  "Special Education" = c("Students with Disabilities"),
  "Socioeconomic" = c("Socioeconomically Disadvantaged"),
  "English Learner" = c("English Learner"),
  "Foster" = c("Foster Youth"),
  "Homeless" = c("Homeless"),
  "Migrant" = c("Migrant"),
  "Sex" = c("Female", "Male", "Non-Binary", "Not Reported", "Missing Gender")
)

category_palettes <- list(
  "Special Education" = c(
    "Students with Disabilities" = ucla_colors[["UCLA Blue"]],
    "All Students" = all_students_color
  ),
  "Socioeconomic" = c(
    "Socioeconomically Disadvantaged" = ucla_colors[["Darker Blue"]],
    "All Students" = all_students_color
  ),
  "English Learner" = c(
    "English Learner" = ucla_colors[["UCLA Blue"]],
    "All Students" = all_students_color
  ),
  "Foster" = c(
    "Foster Youth" = ucla_colors[["Purple"]],
    "All Students" = all_students_color
  ),
  "Homeless" = c(
    "Homeless" = ucla_colors[["Green"]],
    "All Students" = all_students_color
  ),
  "Migrant" = c(
    "Migrant" = ucla_colors[["Magenta"]],
    "All Students" = all_students_color
  ),
  "Sex" = c(
    "Female" = ucla_colors[["UCLA Blue"]],
    "Male" = ucla_colors[["Darker Gold"]],
    "Non-Binary" = ucla_colors[["Purple"]],
    "Not Reported" = ucla_colors[["Magenta"]],
    "Missing Gender" = ucla_colors[["Cyan"]],
    "All Students" = all_students_color
  )
)

build_category_data <- function(cat) {
  subgroups <- category_subgroups[[cat]]
  cat_data <- summary_by_demo %>%
    filter(category_type == cat, subgroup %in% subgroups) %>%
    select(academic_year, subgroup, n_schools, total_enrollment, total_suspensions, pooled_rate, pooled_rate_pct)

  ref <- all_students_summary %>%
    mutate(category_type = cat)

  bind_rows(
    cat_data %>% mutate(category_type = cat),
    ref
  ) %>%
    mutate(
      academic_year = factor(academic_year, levels = year_levels),
      subgroup = forcats::fct_relevel(subgroup, c(subgroups, "All Students"))
    )
}

line_styles_for <- function(palette) {
  styles <- setNames(rep("solid", length(palette)), names(palette))
  if ("All Students" %in% names(styles)) styles[["All Students"]] <- "longdash"
  styles
}

line_widths_for <- function(palette) {
  widths <- setNames(rep(1.1, length(palette)), names(palette))
  if ("All Students" %in% names(widths)) widths[["All Students"]] <- 1.4
  widths
}

plot_category_trends <- function(cat) {
  palette <- category_palettes[[cat]]
  data <- build_category_data(cat)
  if (nrow(data) == 0) {
    warning("No data available for category: ", cat)
    return(NULL)
  }

  ggplot(data, aes(x = academic_year, y = pooled_rate, color = subgroup, group = subgroup)) +
    geom_line(aes(linetype = subgroup, linewidth = subgroup)) +
    geom_point(size = 2.5) +
    geom_label_repel(
      aes(label = pooled_rate_pct),
      size = 3,
      label.size = 0,
      fill = scales::alpha("white", 0.9),
      label.padding = grid::unit(0.15, "lines"),
      point.padding = grid::unit(0.25, "lines"),
      label.r = grid::unit(0.1, "lines"),
      segment.alpha = 0.4,
      show.legend = FALSE,
      max.overlaps = Inf
    ) +
    scale_color_manual(values = palette, drop = FALSE) +
    scale_linetype_manual(values = line_styles_for(palette), drop = FALSE) +
    scale_linewidth_manual(values = line_widths_for(palette), drop = FALSE) +
    scale_y_continuous(labels = percent_format(accuracy = 0.1), expand = expansion(mult = c(0.05, 0.1))) +
    labs(
      title = glue("Statewide Suspension Rates by Year — {cat}"),
      subtitle = "Enrollment-weighted suspension rates for traditional public schools (campus-level totals aggregated statewide).",
      x = "Academic Year",
      y = "Pooled suspension rate",
      color = cat,
      caption = "Source: California Department of Education CALPADS suspension data processed through the REACH staging pipeline (susp_v6_features.parquet & oth_long.parquet). Demographic categories canonicalized in R/01b_ingest_demographics.R; rates equal total suspensions divided by cumulative enrollment."
    ) +
    guides(linetype = guide_none(), linewidth = guide_none()) +
    ucla_theme()
}

plots <- imap(category_palettes, function(palette, cat) {
  plt <- plot_category_trends(cat)
  if (is.null(plt)) {
    return(NULL)
  }
  file_stub <- str_replace_all(str_to_lower(cat), "[^a-z0-9]+", "_")
  out_path <- file.path(OUTPUT_DIR, glue("suspension_rates_{file_stub}.png"))
  ggsave(out_path, plt, width = 11, height = 6.5, dpi = 320)
  message("Saved ", out_path)
  plt
})

invisible(plots)
