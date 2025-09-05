suppressPackageStartupMessages({
  library(dplyr); library(stringr); library(forcats); library(janitor)
  library(arrow); library(here); library(ggplot2); library(openxlsx)
  library(scales); library(tidyr)
})

# ───────────────────────────── Config / Paths ─────────────────────────────
DATA_STAGE <- here("data-stage")
V6F_PARQ   <- file.path(DATA_STAGE, "susp_v6_features.parquet")  # keys + is_traditional (+ black_q in v6)
V6L_PARQ   <- file.path(DATA_STAGE, "susp_v6_long.parquet")      # tidy metrics (num, den, rate)
V5_PARQ    <- file.path(DATA_STAGE, "susp_v5.parquet")           # to fetch white/black quartiles if needed

dir.create(here("outputs"), showWarnings = FALSE)
OUT_XLSX <- here("outputs", "white_vs_black_quartiles_total_vs_swd.xlsx")
OUT_IMG_WHITE <- here("outputs", "rates_pooled_white_quartiles_total_vs_swd.png")
OUT_IMG_BLACK <- here("outputs", "rates_pooled_black_quartiles_total_vs_swd.png")
OUT_IMG_Q4    <- here("outputs", "rates_pooled_Q4_white_vs_Q4_black_total_vs_swd.png")

# ───────────────────────────── Helpers ───────────────────────────────────
safe_div <- function(n, d) ifelse(is.na(d) | d == 0, NA_real_, n / d)

norm_quartile <- function(x) {                      # "Q1"/"1"/" q2 " -> ordered factor Q1..Q4
  x <- as.character(x) |> str_trim()
  qn <- suppressWarnings(as.integer(str_extract(x, "[1-4]")))
  forcats::fct_relevel(factor(paste0("Q", qn)), "Q1","Q2","Q3","Q4")
}

canon_label <- function(x) {                        # map to our subgroup set
  xl <- str_to_lower(x)
  dplyr::case_when(
    str_detect(xl, "\\b(total|all)\\b")                          ~ "Total",
    str_detect(xl, "students? with disabilities|special\\s*education") ~ "Students with Disabilities",
    TRUE ~ NA_character_
  )
}

# ───────────────────────── Load features & quartiles ─────────────────────
stopifnot(file.exists(V6F_PARQ), file.exists(V5_PARQ))
v6_features <- read_parquet(V6F_PARQ) %>% clean_names() %>%
  transmute(
    school_code = as.character(school_code),
    year        = as.character(year),
    is_traditional = !is.na(is_traditional) & is_traditional,
    # keep black_q if already present; we’ll normalize after join
    black_q_raw = as.character(if ("black_q" %in% names(.)) black_q else NA)
  )

# Pull white/black quartiles from v5 (authoritative)
v5 <- read_parquet(V5_PARQ) %>% clean_names() %>%
  transmute(
    school_code = as.character(school_code),
    year        = as.character(academic_year),
    white_q_raw = as.character(white_prop_q_label),
    black_q_raw_v5 = as.character(black_prop_q_label)
  ) %>%
  distinct()

# Merge quartiles onto v6_features; prefer v6 black if set, else v5
keys <- v6_features %>%
  left_join(v5, by = c("school_code","year")) %>%
  mutate(
    black_q = ifelse(!is.na(black_q_raw), black_q_raw, black_q_raw_v5),
    black_q = norm_quartile(black_q),
    white_q = norm_quartile(white_q_raw)
  ) %>%
  select(school_code, year, is_traditional, black_q, white_q)

# Pad codes for robust joins
padw <- max(nchar(keys$school_code), na.rm = TRUE)
keys <- keys %>% mutate(school_code = stringr::str_pad(school_code, padw, "left", "0"))

# ─────────────── Preferred source for subgroup NUM/DEN (v6_long) ───────────
get_long_counts <- function() {
  if (file.exists(V6L_PARQ)) {
    read_parquet(V6L_PARQ) %>% clean_names() %>%
      transmute(
        school_code = str_pad(as.character(school_code), padw, "left", "0"),
        year        = as.character(year),
        subgroup    = canon_label(subgroup),
        num         = as.numeric(num),
        den         = as.numeric(den)
      ) %>% filter(!is.na(subgroup))
  } else {
    # Fallback from v5
    subg_desc <- if ("reporting_category_description" %in% names(v5)) "reporting_category_description" else "reporting_category"
    num_col   <- if ("unduplicated_count_of_students_suspended_total" %in% names(read_parquet(V5_PARQ) %>% clean_names()))
      "unduplicated_count_of_students_suspended_total" else "total_suspensions"
    den_col   <- "cumulative_enrollment"
    
    raw <- read_parquet(V5_PARQ) %>% clean_names()
    if ("aggregate_level" %in% names(raw)) {
      raw <- raw %>% filter(str_to_lower(aggregate_level) %in% c("school","sch"))
    }
    
    raw %>%
      transmute(
        school_code = str_pad(as.character(school_code), padw, "left", "0"),
        year        = as.character(academic_year),
        subgroup    = canon_label(.data[[subg_desc]]),
        num         = as.numeric(.data[[num_col]]),
        den         = as.numeric(.data[[den_col]])
      ) %>%
      filter(!is.na(subgroup))
  }
}

long_counts_all <- get_long_counts()

# Keep only Total & SWD; join keys; filter Traditional
analytic <- long_counts_all %>%
  filter(subgroup %in% c("Total","Students with Disabilities")) %>%
  inner_join(keys, by = c("school_code","year")) %>%
  filter(is_traditional, !is.na(white_q), !is.na(black_q), !is.na(num), !is.na(den))

stopifnot(nrow(analytic) > 0)

# ───────────── Summaries (POOLED) by year × quartile × series ─────────────
sum_white <- analytic %>%
  group_by(year, white_q, subgroup) %>%
  summarise(
    n_schools    = n(),
    total_susp   = sum(num, na.rm = TRUE),
    total_enroll = sum(den, na.rm = TRUE),
    pooled_rate  = safe_div(total_susp, total_enroll),
    .groups = "drop"
  ) %>%
  mutate(
    year = fct_inorder(year),
    white_q = fct_relevel(white_q, "Q1","Q2","Q3","Q4")
  )

sum_black <- analytic %>%
  group_by(year, black_q, subgroup) %>%
  summarise(
    n_schools    = n(),
    total_susp   = sum(num, na.rm = TRUE),
    total_enroll = sum(den, na.rm = TRUE),
    pooled_rate  = safe_div(total_susp, total_enroll),
    .groups = "drop"
  ) %>%
  mutate(
    year = fct_inorder(year),
    black_q = fct_relevel(black_q, "Q1","Q2","Q3","Q4")
  )

# Focused Q4 vs Q4 (highest quartile) comparison by year (Total vs SWD)
sum_q4_white <- sum_white %>% filter(white_q == "Q4") %>% mutate(group = "White Q4")
sum_q4_black <- sum_black %>% filter(black_q == "Q4") %>% mutate(group = "Black Q4")
sum_q4_compare <- bind_rows(
  sum_q4_white %>% select(year, subgroup, pooled_rate, group),
  sum_q4_black %>% select(year, subgroup, pooled_rate, group)
) %>%
  arrange(group, year, subgroup)

# Find the maximum pooled rate across both sets
y_max <- max(c(sum_white$pooled_rate, sum_black$pooled_rate), na.rm = TRUE)

# Add ~5–10% headroom
y_lim <- c(0, y_max * 1.1)

# ─────────────────────────── Plot settings ─────────────────────────────────
quartile_cols <- c("Q1"="#F8766D","Q2"="#7CAE00","Q3"="#00BFC4","Q4"="#C77CFF")

base_theme <- theme_minimal(base_size = 13) +
  theme(
    panel.background = element_rect(fill = "white", color = NA),
    plot.background  = element_rect(fill = "white", color = NA),
    panel.grid.major = element_line(linewidth = 0.3, color = "#D8D8D8"),
    panel.grid.minor = element_blank(),
    axis.text.x      = element_text(angle = 45, hjust = 1),
    legend.position  = "right",
    plot.title       = element_text(face = "bold", size = 16)
  )

# Lines: color = quartile, linetype = subgroup (Total solid, SWD dashed)
# White quartiles plot
plot_white <- ggplot(
  sum_white %>% mutate(series = recode(subgroup,
                                       "Total"="Total (All Students)",
                                       "Students with Disabilities"="Students with Disabilities"),
                       label_txt = percent(pooled_rate, 0.1)),
  aes(x = year, y = pooled_rate,
      color = white_q, linetype = series, group = interaction(white_q, series))) +
  geom_line(linewidth = 1.0) +
  geom_point(shape = 21, size = 2.6, stroke = 0.8, fill = "white") +
  geom_text_repel(aes(label = label_txt),
                  size = 3, box.padding = 0.25, point.padding = 0.2,
                  max.overlaps = 30, segment.color = "grey70", segment.size = 0.3) +
  scale_color_manual(values = quartile_cols,
                     breaks = c("Q1","Q2","Q3","Q4"),
                     labels = c("Q1 (Lowest % White)","Q2","Q3","Q4 (Highest % White)")) +
  scale_linetype_manual(values = c("Total (All Students)"="solid",
                                   "Students with Disabilities"="dashed")) +
  scale_y_continuous(labels = percent_format(0.1), limits = y_lim,
                     expand = expansion(mult = c(0, 0.05))) +
  labs(title = "Pooled Suspension Rates by Year • White-Enrollment Quartiles",
       subtitle = "Traditional schools only • Color = Quartile • Line = Category",
       x = NULL, y = "Rate", color = "Quartile", linetype = "Category") +
  base_theme

# Black quartiles plot (same y limits)
plot_black <- ggplot(
  sum_black %>% mutate(series = recode(subgroup,
                                       "Total"="Total (All Students)",
                                       "Students with Disabilities"="Students with Disabilities"),
                       label_txt = percent(pooled_rate, 0.1)),
  aes(x = year, y = pooled_rate,
      color = black_q, linetype = series, group = interaction(black_q, series))) +
  geom_line(linewidth = 1.0) +
  geom_point(shape = 21, size = 2.6, stroke = 0.8, fill = "white") +
  geom_text_repel(aes(label = label_txt),
                  size = 3, box.padding = 0.25, point.padding = 0.2,
                  max.overlaps = 30, segment.color = "grey70", segment.size = 0.3) +
  scale_color_manual(values = quartile_cols,
                     breaks = c("Q1","Q2","Q3","Q4"),
                     labels = c("Q1 (Lowest % Black)","Q2","Q3","Q4 (Highest % Black)")) +
  scale_linetype_manual(values = c("Total (All Students)"="solid",
                                   "Students with Disabilities"="dashed")) +
  scale_y_continuous(labels = percent_format(0.1), limits = y_lim,
                     expand = expansion(mult = c(0, 0.05))) +
  labs(title = "Pooled Suspension Rates by Year • Black-Enrollment Quartiles",
       subtitle = "Traditional schools only • Color = Quartile • Line = Category",
       x = NULL, y = "Rate", color = "Quartile", linetype = "Category") +
  base_theme

# Q4 vs Q4 comparison
plot_q4 <- ggplot(
  sum_q4_compare %>% mutate(series = recode(subgroup,
                                            "Total"="Total (All Students)",
                                            "Students with Disabilities"="Students with Disabilities"),
                            label_txt = percent(pooled_rate, 0.1)),
  aes(x = year, y = pooled_rate,
      color = group, linetype = series,
      group = interaction(group, series))) +
  geom_line(linewidth = 1.0) +
  geom_point(shape = 21, size = 2.8, stroke = 0.9, fill = "white") +
  geom_text_repel(aes(label = label_txt),
                  size = 3,
                  box.padding = 0.25,
                  point.padding = 0.2,
                  max.overlaps = 30,
                  segment.color = "grey70",
                  segment.size = 0.3) +
  scale_color_manual(values = q4_cols) +
  scale_linetype_manual(values = c("Total (All Students)"="solid",
                                   "Students with Disabilities"="dashed")) +
  scale_y_continuous(labels = percent_format(0.1),
                     expand = expansion(mult = c(0.05, 0.15))) +
  labs(
    title = "Q4 vs Q4: Highest-Quartile White vs Black Schools",
    subtitle = "Traditional schools only • Solid = Total, Dashed = Students with Disabilities",
    x = NULL, y = "Rate", color = "Group", linetype = "Category"
  ) +
  base_theme

ggsave(OUT_IMG_WHITE, plot_white, width = 12, height = 7.5, dpi = 300)
ggsave(OUT_IMG_BLACK, plot_black, width = 12, height = 7.5, dpi = 300)
ggsave(OUT_IMG_Q4,    plot_q4,    width = 12, height = 7.0, dpi = 300)

# ───────────────────────────── Excel outputs ──────────────────────────────
wb <- createWorkbook()

# White quartiles — tidy
addWorksheet(wb, "white_quartiles_tidy")
writeData(
  wb, "white_quartiles_tidy",
  sum_white %>% mutate(pooled_rate = percent(pooled_rate, 0.1)) %>%
    arrange(white_q, year, subgroup)
)

# White quartiles — wide (rows = year × quartile; cols = Total, SWD)
addWorksheet(wb, "white_quartiles_wide")
writeData(
  wb, "white_quartiles_wide",
  sum_white %>%
    mutate(pooled_rate = percent(pooled_rate, 0.1)) %>%
    select(year, white_q, subgroup, pooled_rate) %>%
    pivot_wider(names_from = subgroup, values_from = pooled_rate) %>%
    arrange(year, white_q)
)

# Black quartiles — tidy
addWorksheet(wb, "black_quartiles_tidy")
writeData(
  wb, "black_quartiles_tidy",
  sum_black %>% mutate(pooled_rate = percent(pooled_rate, 0.1)) %>%
  arrange(black_q, year, subgroup)
)

# Black quartiles — wide
addWorksheet(wb, "black_quartiles_wide")
writeData(
  wb, "black_quartiles_wide",
  sum_black %>%
    mutate(pooled_rate = percent(pooled_rate, 0.1)) %>%
    select(year, black_q, subgroup, pooled_rate) %>%
    pivot_wider(names_from = subgroup, values_from = pooled_rate) %>%
    arrange(year, black_q)
)

# Q4 vs Q4 comparison
addWorksheet(wb, "Q4_white_vs_black")
writeData(
  wb, "Q4_white_vs_black",
  sum_q4_compare %>%
    mutate(pooled_rate = percent(pooled_rate, 0.1)) %>%
    arrange(group, year, subgroup)
)

saveWorkbook(wb, OUT_XLSX, overwrite = TRUE)

message(
  "Done.\nExcel: ", OUT_XLSX,
  "\nImages:\n- ", OUT_IMG_WHITE,
  "\n- ", OUT_IMG_BLACK,
  "\n- ", OUT_IMG_Q4
)
