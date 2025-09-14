suppressPackageStartupMessages({
  library(dplyr); library(stringr); library(forcats); library(janitor)
  library(arrow); library(here); library(ggplot2); library(openxlsx)
  library(scales); library(tidyr)
})

source(here::here("R","utils_keys_filters.R"))
source(here::here("R","demographic_labels.R"))

# ──────────────────────────────── Config / Paths ──────────────────────────────
DATA_STAGE <- here("data-stage")                   # portable path
V6F_PARQ   <- file.path(DATA_STAGE, "susp_v6_features.parquet")  # keys + flags
V6L_PARQ   <- file.path(DATA_STAGE, "susp_v6_long.parquet")      # tidy metrics (total_suspensions, cumulative_enrollment, suspension_rate_percent_total)

dir.create(here("outputs"), showWarnings = FALSE)
OUT_IMG  <- here("outputs", "rates_pooled_by_year_blackquartile_subgroup.png")
OUT_XLSX <- here("outputs", "rates_pooled_by_year_blackquartile_subgroup.xlsx")

# ────────────────────────────────── Helpers ───────────────────────────────────
safe_div <- function(n, d) ifelse(is.na(d) | d == 0, NA_real_, n / d)

norm_black_q <- NULL  # no longer needed; quartiles stored numerically

# ───────────────────────────── Load v6_features ───────────────────────────────
stopifnot(file.exists(V6F_PARQ))
v6_features <- read_parquet(V6F_PARQ) %>% clean_names() %>%
  transmute(
    school_code   = as.character(school_code),
    academic_year = as.character(academic_year),
    black_prop_q  = as.integer(black_prop_q),
    is_traditional = !is.na(is_traditional) & is_traditional
  )

# Pad school_code for safe joins
padw <- max(nchar(v6_features$school_code), na.rm = TRUE)
v6_features <- v6_features %>% mutate(school_code = stringr::str_pad(school_code, padw, "left", "0"))

# ───── Preferred source for subgroup totals (v6_long long metrics) ─────
stopifnot(file.exists(V6L_PARQ))
long_counts <- read_parquet(V6L_PARQ) %>% clean_names() %>%
  transmute(
    school_code   = stringr::str_pad(as.character(school_code), padw, "left", "0"),
    academic_year = as.character(academic_year),
    subgroup      = dplyr::coalesce(canon_demo_label(subgroup),
                                    canon_race_label(subgroup)),
    total_suspensions      = as.numeric(total_suspensions),
    cumulative_enrollment  = as.numeric(cumulative_enrollment)
  ) %>%
  filter(!is.na(subgroup), subgroup != "Sex")

# ───────────────────────── Join keys + filter to traditional ──────────────────
analytic <- long_counts %>%
  inner_join(v6_features, by = c("school_code","academic_year")) %>%
  filter(is_traditional, !is.na(black_prop_q), !is.na(total_suspensions), !is.na(cumulative_enrollment)) %>%
  mutate(black_prop_q_label = factor(paste0("Q", black_prop_q),
                                     levels = paste0("Q", 1:4)))

if (nrow(analytic) == 0) stop("No rows after join/filter. Check keys or inputs.")

# ───────────── Summarize (POOLED) by year × black quartile × subgroup ────────
sum_by <- analytic %>%
  group_by(academic_year, black_prop_q_label, subgroup) %>%
  summarise(
    n_schools    = dplyr::n(),
    total_susp   = sum(total_suspensions, na.rm = TRUE),
    total_enroll = sum(cumulative_enrollment, na.rm = TRUE),
    pooled_rate  = safe_div(total_susp, total_enroll),
    .groups = "drop"
  ) %>%
  arrange(subgroup, academic_year, black_prop_q_label)

# ─────────────────────────── Plot (styled like example) ───────────────────────
# palette close to your example
quartile_cols <- c("Q1"="#F8766D","Q2"="#7CAE00","Q3"="#00BFC4","Q4"="#C77CFF")

sum_by <- sum_by %>%
  mutate(
    academic_year      = forcats::fct_inorder(academic_year),
    black_prop_q_label = forcats::fct_relevel(black_prop_q_label,
                                              "Q1","Q2","Q3","Q4"),
    label_txt = percent(pooled_rate, accuracy = 0.1)
  )

use_ggrepel <- requireNamespace("ggrepel", quietly = TRUE)

base_plot <- ggplot(sum_by,
                    aes(x = academic_year, y = pooled_rate,
                        group = black_prop_q_label, color = black_prop_q_label)) +
  geom_line(linewidth = 1.2) +
  geom_point(size = 2.8) +
  scale_color_manual(values = quartile_cols,
                     breaks = c("Q1","Q2","Q3","Q4"),
                     labels = get_quartile_label(1:4, "Black")) +
  scale_y_continuous(labels = percent_format(accuracy = 0.1),
                     expand = expansion(mult = c(0.05, 0.15))) +
  labs(
    title = "Suspension Rates by Year & School Black-Enrollment Quartile",
    subtitle = "Pooled rates (Σ suspensions ÷ Σ enrollment) • Traditional schools only",
    x = NULL, y = "Rate", color = "Quartile"
  ) +
  theme_minimal(base_size = 13) +
  theme(
    panel.background   = element_rect(fill = "white", color = NA),
    plot.background    = element_rect(fill = "white", color = NA),
    panel.grid.major   = element_line(linewidth = 0.3, color = "#D8D8D8"),
    panel.grid.minor   = element_blank(),
    axis.text.x        = element_text(angle = 45, hjust = 1),
    legend.position    = "right",
    plot.title         = element_text(face = "bold", size = 16)
  )

if (use_ggrepel) {
  base_plot <- base_plot +
    ggrepel::geom_text_repel(aes(label = label_txt),
                             size = 3, max.overlaps = 10, box.padding = 0.25,
                             point.padding = 0.2, segment.alpha = 0.3)
} else {
  base_plot <- base_plot + geom_text(aes(label = label_txt), vjust = -0.6, size = 3, show.legend = FALSE)
}

p <- base_plot + facet_wrap(~ subgroup, scales = "free_y")
ggsave(OUT_IMG, p, width = 12, height = 8, dpi = 300)

# ───────────────── Excel (match Script 23 tabs; pooled rates) ─────────────────
# Ensure n_schools exists for wide_N (derive from analytic if missing)
if (!"n_schools" %in% names(sum_by)) {
  n_by <- analytic %>%
    dplyr::group_by(academic_year, black_prop_q_label, subgroup) %>%
    dplyr::summarise(n_schools = dplyr::n_distinct(school_code), .groups = "drop")
  sum_by <- sum_by %>% dplyr::left_join(n_by,
                                        by = c("academic_year","black_prop_q_label","subgroup"))
}

wb <- openxlsx::createWorkbook()

# 1) Tidy sheet — SAME NAME as Script 23
openxlsx::addWorksheet(wb, "tidy_by_year_q_subgroup")
tidy_out <- sum_by %>%
  dplyr::mutate(
    dplyr::across(c(academic_year, black_prop_q_label, subgroup), as.character),
    pooled_rate = scales::percent(pooled_rate, accuracy = 0.1),
    dplyr::across(dplyr::any_of(c("ci_low","ci_high")),
                  ~ if (is.numeric(.x)) scales::percent(.x, accuracy = 0.1) else .x)
  ) %>% dplyr::arrange(subgroup, academic_year, black_prop_q_label)
openxlsx::writeData(wb, "tidy_by_year_q_subgroup", tidy_out)

# 2) Wide rates — SAME NAME as Script 23 (but using pooled_rate)
openxlsx::addWorksheet(wb, "wide_rates")
wide_rates <- sum_by %>%
  dplyr::select(academic_year, black_prop_q_label, subgroup, pooled_rate) %>%
  dplyr::mutate(
    dplyr::across(c(academic_year, black_prop_q_label), as.character),
    pooled_rate = scales::percent(pooled_rate, accuracy = 0.1)
  ) %>%
  tidyr::pivot_wider(names_from = subgroup, values_from = pooled_rate) %>%
  dplyr::arrange(academic_year, black_prop_q_label)
openxlsx::writeData(wb, "wide_rates", wide_rates)

# 3) Wide N — SAME NAME as Script 23
openxlsx::addWorksheet(wb, "wide_N")
wide_n <- sum_by %>%
  dplyr::select(academic_year, black_prop_q_label, subgroup, n_schools) %>%
  dplyr::mutate(dplyr::across(c(academic_year, black_prop_q_label), as.character)) %>%
  tidyr::pivot_wider(names_from = subgroup, values_from = n_schools) %>%
  dplyr::arrange(academic_year, black_prop_q_label)
openxlsx::writeData(wb, "wide_N", wide_n)

openxlsx::saveWorkbook(wb, OUT_XLSX, overwrite = TRUE)
# ──────────────────────────────── Done / Messages ─────────────────────────────

message(
  "Done:\n- Plot: ", OUT_IMG,
  "\n- Excel: ", OUT_XLSX,
  "\nSources: ", if (file.exists(V6L_PARQ)) "v6_long (total_suspensions/cumulative_enrollment) + v6_features" else "v5 (total_suspensions/cumulative_enrollment) + v6_features",
  "\nMethod: POOLED rates (sum susp / sum enr)."
)
