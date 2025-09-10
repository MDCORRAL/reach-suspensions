# 17_tail_concentration_by_level.R
# Tail concentration metrics by level (elementary, middle, high)

suppressPackageStartupMessages({
  library(dplyr); library(stringr); library(forcats); library(janitor)
  library(arrow); library(here); library(ggplot2); library(openxlsx)
  library(scales); library(tidyr); library(purrr); library(readr)
  library(ineq)
})

DATA_STAGE <- here("data-stage")
INPUT_PATH <- file.path(DATA_STAGE, "susp_v5.parquet")
V6F_PARQ   <- file.path(DATA_STAGE, "susp_v6_features.parquet")

RUN_TAG <- format(Sys.time(), "%Y%m%d_%H%M")
OUT_DIR <- here("outputs", paste0("tail_concentration_by_level_", RUN_TAG))
if (!dir.exists(OUT_DIR)) dir.create(OUT_DIR, recursive = TRUE)

cols <- list(
  school_id   = "school_code",
  school_name = "school_name",
  year        = "academic_year",
  setting     = "school_type",
  enrollment  = "cumulative_enrollment",
  total_susp  = "total_suspensions",
  undup_susp  = "unduplicated_count_of_students_suspended_total"
)

MEASURE <- "total_susp"
TOP_PCT <- c(0.05, 0.10, 0.20)

safe_div <- function(n, d) ifelse(is.na(d) | d == 0, NA_real_, n / d)

map_setting <- function(x) {
  x_clean <- str_to_lower(str_trim(as.character(x)))
  case_when(
    is.na(x) ~ NA_character_,
    str_detect(x_clean, "traditional") ~ "Traditional",
    TRUE ~ "Non-traditional"
  )
}

extract_year <- function(x) {
  year_match <- str_extract(x, "^\\d{4}")
  as.integer(year_match)
}

pareto_shares <- function(df, top_ps = TOP_PCT) {
  n_schools <- n_distinct(df$school_id)
  if (n_schools == 0) return(tibble())

  df_sorted <- df %>%
    group_by(school_id, school_name, enrollment) %>%
    summarise(measure = sum(measure, na.rm = TRUE), .groups = "drop") %>%
    arrange(desc(measure)) %>%
    mutate(
      rank = row_number(),
      cum_measure = cumsum(measure),
      total_measure = sum(measure, na.rm = TRUE),
      share_cum = safe_div(cum_measure, total_measure)
    )

  map_dfr(top_ps, function(p) {
    cutoff_n <- max(1, floor(p * n_schools))
    top_share <- df_sorted %>%
      slice(1:cutoff_n) %>%
      summarise(
        top_schools = cutoff_n,
        total_schools = n_schools,
        top_share = safe_div(sum(measure, na.rm = TRUE),
                             df_sorted$total_measure[1])
      ) %>%
      mutate(top_pct = p)
    top_share
  })
}

lorenz_gini <- function(df) {
  x <- df %>%
    group_by(school_id) %>%
    summarise(measure = sum(measure, na.rm = TRUE), .groups = "drop") %>%
    pull(measure)
  x[is.na(x)] <- 0
  if (length(x) == 0 || sum(x) == 0) {
    return(list(curve = tibble(p = 0, L = 0), gini = NA_real_))
  }
  L <- ineq::Lc(x)
  g <- ineq::Gini(x)
  list(curve = tibble(p = L$p, L = L$L), gini = g)
}

message("Loading suspension data from: ", INPUT_PATH)
dat0 <- read_parquet(INPUT_PATH) %>% clean_names()

dat <- dat0 %>%
  filter(str_to_lower(reporting_category) %in% c("total", "all students", "ta")) %>%
  rename(
    school_id   = !!sym(cols$school_id),
    year        = !!sym(cols$year),
    enrollment  = !!sym(cols$enrollment),
    total_susp  = !!sym(cols$total_susp),
    undup_susp  = !!sym(cols$undup_susp)
  ) %>%
  mutate(
    year_num    = extract_year(year),
    enrollment  = as.numeric(enrollment),
    total_susp  = as.numeric(total_susp),
    undup_susp  = as.numeric(undup_susp),
    measure     = if (MEASURE == "undup_susp") undup_susp else total_susp,
    school_name = if ("school_name" %in% names(dat0)) school_name else school_id
  ) %>%
  filter(
    !is.na(year_num), !is.na(enrollment), !is.na(measure),
    enrollment > 0, measure >= 0
  )

if (file.exists(V6F_PARQ)) {
  message("Loading school features...")
  v6_features <- read_parquet(V6F_PARQ) %>% clean_names()
  v6_features <- v6_features %>%
    mutate(year_char = as.character(year)) %>%
    select(school_code, year_char, is_traditional, school_type,
           school_level_final, level_strict3)

  dat <- dat %>%
    left_join(v6_features,
              by = c("school_id" = "school_code", "year" = "year_char")) %>%
    mutate(
      setting = map_setting(school_type),
      level = coalesce(school_level_final, level_strict3)
    )
} else {
  dat <- dat %>% mutate(setting = NA_character_, level = NA_character_)
}

ps_y_ls <- dat %>%
  filter(!is.na(level), !is.na(setting)) %>%
  group_by(year_num, level, setting) %>%
  group_modify(~pareto_shares(.x, TOP_PCT)) %>%
  ungroup() %>%
  mutate(measure_type = MEASURE)

ps_y_ls_lines <- ps_y_ls %>%
  mutate(
    top_label = paste0("Top ", scales::percent(top_pct)),
    share_pct = scales::percent(top_share, accuracy = 0.1)
  ) %>%
  select(year_num, level, setting, top_label, share_pct,
         top_schools, total_schools, measure_type)

write_csv(ps_y_ls, file.path(OUT_DIR,
                             "pareto_shares_by_year_level_setting_raw.csv"))
write_csv(ps_y_ls_lines, file.path(OUT_DIR,
                                   "pareto_shares_by_year_level_setting_slide_ready.csv"))

lg_y_ls <- dat %>%
  filter(!is.na(level), !is.na(setting)) %>%
  group_by(year_num, level, setting) %>%
  group_modify(~{
    lg <- lorenz_gini(.x)
    lg$curve %>% mutate(year_num = unique(.y$year_num),
                        level = unique(.y$level),
                        setting = unique(.y$setting),
                        gini = lg$gini)
  }) %>%
  ungroup()

write_csv(lg_y_ls, file.path(OUT_DIR,
                             "lorenz_points_by_year_level_setting_with_gini.csv"))

latest_year <- max(ps_y_ls$year_num, na.rm = TRUE)
for (lvl in unique(ps_y_ls$level)) {
  df_plot <- dat %>% filter(year_num == latest_year, level == lvl, !is.na(setting))
  if (nrow(df_plot) == 0) next
  lg <- lorenz_gini(df_plot)
  p_lorenz <- ggplot(lg$curve, aes(p, L)) +
    geom_line(color = "steelblue", size = 1) +
    geom_abline(slope = 1, intercept = 0, linetype = "dashed") +
    labs(
      title = paste0("Lorenz Curve (", latest_year, " - ", lvl, ")"),
      x = "Cumulative share of schools",
      y = "Cumulative share of suspensions",
      caption = paste0("Gini = ", scales::percent(lg$gini, accuracy = 0.1))
    ) +
    theme_minimal(base_size = 12) +
    theme(panel.grid.minor = element_blank(),
          plot.title = element_text(face = "bold"))
  ggsave(file.path(OUT_DIR,
                   paste0("lorenz_", latest_year, "_", lvl, ".png")),
         p_lorenz, width = 8, height = 6, dpi = 300)
}

message("Outputs written to: ", OUT_DIR)
