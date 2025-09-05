suppressPackageStartupMessages({
  library(dplyr); library(stringr); library(forcats); library(janitor)
  library(arrow); library(here); library(ggplot2); library(openxlsx)
  library(scales); library(tidyr)
})

# ──────────────────────────────── Config / Paths ──────────────────────────────
DATA_STAGE <- here("data-stage")                   # portable path
V6F_PARQ   <- file.path(DATA_STAGE, "susp_v6_features.parquet")  # keys + flags
V6L_PARQ   <- file.path(DATA_STAGE, "susp_v6_long.parquet")      # tidy metrics (num, den, rate)
V5_PARQ    <- file.path(DATA_STAGE, "susp_v5.parquet")           # fallback raw source

dir.create(here("outputs"), showWarnings = FALSE)
OUT_IMG  <- here("outputs", "rates_pooled_by_year_blackquartile_subgroup.png")
OUT_XLSX <- here("outputs", "rates_pooled_by_year_blackquartile_subgroup.xlsx")

# ────────────────────────────────── Helpers ───────────────────────────────────
safe_div <- function(n, d) ifelse(is.na(d) | d == 0, NA_real_, n / d)

norm_black_q <- function(x) {                      # returns factor Q1..Q4
  x <- as.character(x) |> stringr::str_trim()
  qn <- suppressWarnings(as.integer(stringr::str_extract(x, "[1-4]")))
  forcats::fct_relevel(factor(paste0("Q", qn)), "Q1","Q2","Q3","Q4")
}

canon_label <- function(x) {                       # canonicalize subgroup labels
  xl <- stringr::str_to_lower(x)
  dplyr::case_when(
    str_detect(xl, "\\b(total|all)\\b")                          ~ "Total",
    str_detect(xl, "black|african")                               ~ "Black/African American",
    str_detect(xl, "white")                                       ~ "White",
    str_detect(xl, "hispanic|latino")                             ~ "Hispanic/Latino",
    str_detect(xl, "\\basian\\b")                                 ~ "Asian",
    str_detect(xl, "filipino")                                    ~ "Filipino",
    str_detect(xl, "american indian|alaska")                      ~ "American Indian/Alaska Native",
    str_detect(xl, "pacific islander|hawaiian")                   ~ "Native Hawaiian/Pacific Islander",
    str_detect(xl, "two or more|multiracial|two or more races")   ~ "Two or More Races",
    str_detect(xl, "students? with disabilities|special\\s*education") ~ "Students with Disabilities",
    TRUE ~ NA_character_
  )
}

# ───────────────────────────── Load v6_features ───────────────────────────────
stopifnot(file.exists(V6F_PARQ))
v6_features <- read_parquet(V6F_PARQ) %>% clean_names() %>%
  transmute(
    school_code = as.character(school_code),
    year        = as.character(year),
    black_q     = norm_black_q(black_q),
    is_traditional = !is.na(is_traditional) & is_traditional
  )

# Pad school_code for safe joins
padw <- max(nchar(v6_features$school_code), na.rm = TRUE)
v6_features <- v6_features %>% mutate(school_code = stringr::str_pad(school_code, padw, "left", "0"))

# ─────────────── Preferred source for subgroup NUM/DEN (v6_long) ─────────────
get_long_with_counts <- function() {
  if (file.exists(V6L_PARQ)) {
    # v6_long is expected to already have num/den
    read_parquet(V6L_PARQ) %>% clean_names() %>%
      transmute(
        school_code = stringr::str_pad(as.character(school_code), padw, "left", "0"),
        year        = as.character(year),
        subgroup    = canon_label(subgroup),
        num         = as.numeric(num),
        den         = as.numeric(den)
      ) %>%
      filter(!is.na(subgroup))
  } else {
    # Fallback: derive counts from v5
    stopifnot(file.exists(V5_PARQ))
    v5 <- read_parquet(V5_PARQ) %>% clean_names()
    
    subg_desc <- if ("reporting_category_description" %in% names(v5))
      "reporting_category_description" else "reporting_category"
    num_col   <- if ("unduplicated_count_of_students_suspended_total" %in% names(v5))
      "unduplicated_count_of_students_suspended_total" else "total_suspensions"
    den_col   <- "cumulative_enrollment"
    if ("aggregate_level" %in% names(v5)) {
      v5 <- v5 %>% filter(stringr::str_to_lower(aggregate_level) %in% c("school","sch"))
    }
    
    v5 %>%
      transmute(
        school_code = stringr::str_pad(as.character(school_code), padw, "left", "0"),
        year        = as.character(academic_year),
        subgroup    = canon_label(.data[[subg_desc]]),
        num         = as.numeric(.data[[num_col]]),
        den         = as.numeric(.data[[den_col]])
      ) %>%
      filter(!is.na(subgroup))
  }
}

long_counts <- get_long_with_counts()

# ───────────────────────── Join keys + filter to traditional ──────────────────
analytic <- long_counts %>%
  inner_join(v6_features, by = c("school_code","year")) %>%
  filter(is_traditional, !is.na(black_q), !is.na(num), !is.na(den))

if (nrow(analytic) == 0) stop("No rows after join/filter. Check keys or inputs.")

# ───────────── Summarize (POOLED) by year × black quartile × subgroup ────────
sum_by <- analytic %>%
  group_by(year, black_q, subgroup) %>%
  summarise(
    n_schools    = dplyr::n(),
    total_susp   = sum(num, na.rm = TRUE),
    total_enroll = sum(den, na.rm = TRUE),
    pooled_rate  = safe_div(total_susp, total_enroll),
    .groups = "drop"
  ) %>%
  arrange(subgroup, year, black_q)

# ─────────────────────────── Plot (styled like example) ───────────────────────
# palette close to your example
quartile_cols <- c("Q1"="#F8766D","Q2"="#7CAE00","Q3"="#00BFC4","Q4"="#C77CFF")

sum_by <- sum_by %>%
  mutate(
    year = forcats::fct_inorder(year),
    black_q = forcats::fct_relevel(black_q, "Q1","Q2","Q3","Q4"),
    label_txt = percent(pooled_rate, accuracy = 0.1)
  )

use_ggrepel <- requireNamespace("ggrepel", quietly = TRUE)

base_plot <- ggplot(sum_by, aes(x = year, y = pooled_rate, group = black_q, color = black_q)) +
  geom_line(linewidth = 1.2) +
  geom_point(size = 2.8) +
  scale_color_manual(values = quartile_cols,
                     breaks = c("Q1","Q2","Q3","Q4"),
                     labels = c("Q1 (Lowest % Black)","Q2","Q3","Q4 (Highest % Black)")) +
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

# ───────────────────────────── Excel (tidy + wide) ────────────────────────────
wb <- createWorkbook()

# Tidy pooled summary
addWorksheet(wb, "tidy_pooled_by_year_q_subg")
writeData(
  wb, "tidy_pooled_by_year_q_subg",
  sum_by %>%
    mutate(
      pooled_rate = percent(pooled_rate, accuracy = 0.1)
    ) %>%
    arrange(subgroup, year, black_q)
)

# Wide table: rows = year × black_q, columns = subgroups (pooled rate %)
wide_rates <- sum_by %>%
  select(year, black_q, subgroup, pooled_rate) %>%
  mutate(pooled_rate = percent(pooled_rate, accuracy = 0.1)) %>%
  pivot_wider(names_from = subgroup, values_from = pooled_rate) %>%
  arrange(year, black_q)

addWorksheet(wb, "wide_pooled_rates")
writeData(wb, "wide_pooled_rates", wide_rates)

saveWorkbook(wb, OUT_XLSX, overwrite = TRUE)

message(
  "Done:\n- Plot: ", OUT_IMG,
  "\n- Excel: ", OUT_XLSX,
  "\nSources: ", if (file.exists(V6L_PARQ)) "v6_long (num/den) + v6_features" else "v5 (num/den) + v6_features",
  "\nMethod: POOLED rates (sum susp / sum enr)."
)
