# graph_scripts/05_unequal_burden.R
# Line graph showing concentration of suspensions among a small share of schools.

suppressPackageStartupMessages({
  library(dplyr)
  library(ggplot2)
  library(glue)
  library(here)
  library(scales)
})

source(here::here("graph_scripts", "graph_utils.R"))

calc_top_share <- function(df, label, top_frac = 0.10) {
  df %>%
    dplyr::group_by(academic_year) %>%
    dplyr::group_modify(~{
      school_totals <- .x %>%
        dplyr::group_by(school_code) %>%
        dplyr::summarise(total_susp = sum(total_suspensions, na.rm = TRUE), .groups = "drop") %>%
        dplyr::filter(!is.na(total_susp)) %>%
        dplyr::arrange(dplyr::desc(total_susp))
      n_schools <- nrow(school_totals)
      if (n_schools == 0) {
        return(tibble::tibble())
      }
      top_n <- max(1, floor(top_frac * n_schools))
      total_sum <- sum(school_totals$total_susp, na.rm = TRUE)
      top_sum <- sum(school_totals$total_susp[seq_len(top_n)], na.rm = TRUE)
      tibble::tibble(
        academic_year = unique(.x$academic_year),
        setting = label,
        top_share = safe_div(top_sum, total_sum),
        top_schools = top_n,
        total_schools = n_schools
      )
    }) %>%
    dplyr::ungroup()
}

joined <- load_joined_data()

susp_base <- joined %>%
  dplyr::filter(
    is_traditional,
    subgroup %in% c("All Students", "Total"),
    !is.na(total_suspensions),
    total_suspensions >= 0
  ) %>%
  dplyr::mutate(academic_year = as.character(academic_year))

series_all <- calc_top_share(susp_base, "All Traditional Schools")
series_elem <- calc_top_share(
  susp_base %>% dplyr::filter(school_level == "Elementary"),
  "Elementary Traditional Schools"
)

series <- dplyr::bind_rows(series_all, series_elem)

year_levels <- series$academic_year %>% unique() %>% sort()

series <- series %>%
  dplyr::mutate(
    academic_year = factor(academic_year, levels = year_levels),
    setting = factor(setting, levels = names(setting_palette))
  ) %>%
  dplyr::filter(!is.na(top_share))

latest_year <- latest_year_available(series$academic_year)
label_data <- series %>%
  dplyr::filter(as.character(academic_year) == latest_year) %>%
  dplyr::mutate(label = scales::percent(top_share, accuracy = 0.1))

plot_concentration <- ggplot(series,
                             aes(x = academic_year, y = top_share, color = setting, group = setting)) +
  geom_line(linewidth = 1.05) +
  geom_point(size = 2.6) +
  geom_text(data = label_data,
            aes(label = label),
            position = position_nudge(x = 0.18),
            hjust = 0,
            size = 3,
            show.legend = FALSE) +
  scale_color_manual(values = setting_palette, name = NULL) +
  scale_x_discrete(expand = expansion(mult = c(0.02, 0.22))) +
  scale_y_continuous(labels = scales::percent_format(accuracy = 0.1),
                     expand = expansion(mult = c(0, 0.08))) +
  coord_cartesian(clip = "off") +
  labs(
    title = "Suspensions Cluster in a Small Share of Schools",
    subtitle = "Share of total suspensions attributed to the top 10% of schools (by suspensions)",
    x = NULL,
    y = "Share of suspensions",
    caption = "Source: California statewide suspension data (susp_v5 + v6 features)"
  ) +
  theme_reach()

out_path <- file.path(OUTPUT_DIR, "unequal_burden_top10.png")

ggsave(out_path, plot_concentration, width = 11.5, height = 7, dpi = 320)

fmt_percent <- function(x) if (!is.na(x)) scales::percent(x, accuracy = 0.1) else "N/A"

first_year <- if (length(year_levels) > 0) year_levels[1] else NA_character_
latest_all <- label_data %>% dplyr::filter(setting == "All Traditional Schools")
latest_elem <- label_data %>% dplyr::filter(setting == "Elementary Traditional Schools")
first_all <- series %>% dplyr::filter(setting == "All Traditional Schools", as.character(academic_year) == first_year)
first_elem <- series %>% dplyr::filter(setting == "Elementary Traditional Schools", as.character(academic_year) == first_year)

pull_value <- function(df, col) {
  if (nrow(df) == 0) {
    return(NA)
  }
  val <- df[[col]]
  if (length(val) == 0) NA else val[1]
}

share_all_now   <- pull_value(latest_all, "top_share")
share_elem_now  <- pull_value(latest_elem, "top_share")
share_all_first <- pull_value(first_all, "top_share")
share_elem_first<- pull_value(first_elem, "top_share")
top_school_count<- pull_value(latest_all, "top_schools")
total_school_ct <- pull_value(latest_all, "total_schools")

top_school_phrase <- if (!is.na(top_school_count) && !is.na(total_school_ct)) {
  glue("{top_school_count} campuses out of {total_school_ct}")
} else {
  "a small set of campuses"
}

concentration_text <- glue(
  "The top decile of traditional schools accounted for {fmt_percent(share_all_now)} of statewide suspensions in {latest_year}, meaning {top_school_phrase} carry that share. ",
  "Elementary campuses show similar concentration: the top decile captured {fmt_percent(share_elem_now)} of suspensions in {latest_year}, compared with {fmt_percent(share_elem_first)} in {first_year}. ",
  "Statewide, the share shifted from {fmt_percent(share_all_first)} in {first_year}, underscoring how a small cluster of schools continues to shoulder most suspensions."
)

write_description(concentration_text, "unequal_burden_top10.txt")

message("Saved plot to ", out_path)
