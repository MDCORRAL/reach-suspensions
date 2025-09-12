##25_compare_white_vs_black_quartiles_swd.R─────────────────────────────
# Canonical subgroup term: Students with Disabilities (SWD)

suppressPackageStartupMessages({
  library(dplyr); library(stringr); library(forcats); library(janitor)
  library(arrow); library(here); library(ggplot2); library(openxlsx)
  library(scales); library(tidyr); library(ggrepel)
})

source(here::here("R","utils_keys_filters.R"))

# ───────────────────────────── Config / Paths ─────────────────────────────
DATA_STAGE <- here("data-stage")
V6F_PARQ   <- file.path(DATA_STAGE, "susp_v6_features.parquet")  # keys + is_traditional (+ black_q in v6)
V6L_PARQ   <- file.path(DATA_STAGE, "susp_v6_long.parquet")      # tidy metrics (num, den, rate)

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
stopifnot(file.exists(V6F_PARQ), file.exists(V6L_PARQ))
v6_features <- read_parquet(V6F_PARQ) %>% clean_names() %>%
  transmute(
    school_code   = as.character(school_code),
    year          = as.character(year),
    is_traditional = !is.na(is_traditional) & is_traditional,
    black_prop_q  = as.integer(black_prop_q)
  )

# Compute white quartile from v6_long
v6_long <- read_parquet(V6L_PARQ) %>% clean_names() %>%
  transmute(
    school_code     = as.character(school_code),
    year            = as.character(year),
    subgroup        = str_to_lower(subgroup),
    den             = as.numeric(den),
    black_prop_q_v5 = as.integer(black_prop_q)
  )

total_enr <- v6_long %>%
  filter(subgroup %in% c("total","all students","ta")) %>%
  select(school_code, year, total_den = den)

white_enr <- v6_long %>%
  filter(str_detect(subgroup, "white")) %>%
  select(school_code, year, white_den = den)
#### main

white_q <- total_enr %>%
  inner_join(white_enr, by = c("school_code","year")) %>%
  mutate(share = safe_div(white_den, total_den)) %>%
  filter(!is.na(share)) %>%
  group_by(year) %>%
  mutate(white_prop_q = ntile(share, 4L)) %>%
  ungroup() %>%
  select(school_code, year, white_prop_q)

# Merge quartiles onto v6_features
keys <- v6_features %>%
  left_join(white_q, by = c("school_code","year")) %>%
  mutate(
    black_prop_q = ifelse(!is.na(black_prop_q), black_prop_q, black_prop_q_v5),
    black_prop_q_label = factor(paste0("Q", black_prop_q),
                                levels = paste0("Q",1:4)),
    white_prop_q_label = factor(paste0("Q", white_prop_q),
                                levels = paste0("Q",1:4))
  ) %>%
  select(school_code, year, is_traditional,
         black_prop_q, black_prop_q_label,
         white_prop_q, white_prop_q_label)

# Pad codes for robust joins
padw <- max(nchar(keys$school_code), na.rm = TRUE)
keys <- keys %>% mutate(school_code = stringr::str_pad(school_code, padw, "left", "0"))

# ─────────────── Preferred source for subgroup NUM/DEN (v6_long) ───────────
stopifnot(file.exists(V6L_PARQ))
long_counts_all <- read_parquet(V6L_PARQ) %>% clean_names() %>%
  transmute(
    school_code = str_pad(as.character(school_code), padw, "left", "0"),
    year        = as.character(year),
    subgroup    = canon_label(subgroup),
    num         = as.numeric(num),
    den         = as.numeric(den)
  ) %>% filter(!is.na(subgroup))

##codex/rename-variables-for-canonical-term
# Keep only Total & Students with Disabilities (SWD); join keys; filter Traditional

analytic <- long_counts_all %>%
  filter(subgroup %in% c("Total","Students with Disabilities")) %>%
  inner_join(keys, by = c("school_code","year")) %>%
  filter(is_traditional, !is.na(white_prop_q), !is.na(black_prop_q),
         !is.na(num), !is.na(den))

stopifnot(nrow(analytic) > 0)

# ───────────── Summaries (POOLED) by year × quartile × series ─────────────
sum_white <- analytic %>%
  group_by(year, white_prop_q_label, subgroup) %>%
  summarise(
    n_schools    = n(),
    total_susp   = sum(num, na.rm = TRUE),
    total_enroll = sum(den, na.rm = TRUE),
    pooled_rate  = safe_div(total_susp, total_enroll),
    .groups = "drop"
  ) %>%
  mutate(
    year = fct_inorder(year),
    white_prop_q_label = fct_relevel(white_prop_q_label,
                                     "Q1","Q2","Q3","Q4")
  )

sum_black <- analytic %>%
  group_by(year, black_prop_q_label, subgroup) %>%
  summarise(
    n_schools    = n(),
    total_susp   = sum(num, na.rm = TRUE),
    total_enroll = sum(den, na.rm = TRUE),
    pooled_rate  = safe_div(total_susp, total_enroll),
    .groups = "drop"
  ) %>%
  mutate(
    year = fct_inorder(year),
    black_prop_q_label = fct_relevel(black_prop_q_label,
                                     "Q1","Q2","Q3","Q4")
  )

# Focused Q4 vs Q4 (highest quartile) comparison by year (Total vs SWD)
###codex/refactor-quartile-naming-convention-and-scripts
sum_q4_white <- sum_white %>%
  filter(white_prop_q_label == "Q4") %>%
  mutate(group = "White Q4")
sum_q4_black <- sum_black %>%
  filter(black_prop_q_label == "Q4") %>%
  mutate(group = "Black Q4")

### main
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
q4_cols <- c("White Q4"="#0072B2", "Black Q4"="#D55E00")  # Add this line

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

# Lines: color = quartile, linetype = subgroup (Total solid, Students with Disabilities dashed)
# White quartiles plot
plot_white <- ggplot(
  sum_white %>% mutate(series = recode(subgroup,
                                       "Total"="Total (All Students)",
                                       "Students with Disabilities"="Students with Disabilities"),
                       label_txt = percent(pooled_rate, 0.1)),
  aes(x = year, y = pooled_rate,
      color = white_prop_q_label, linetype = series,
      group = interaction(white_prop_q_label, series))) +
  geom_line(linewidth = 1.0) +
  geom_point(shape = 21, size = 2.6, stroke = 0.8, fill = "white") +
    geom_text_repel(aes(label = label_txt),
                    size = 3, box.padding = 0.25, point.padding = 0.2,
                    max.overlaps = 30, segment.color = "grey70", segment.size = 0.3) +
    scale_color_manual(values = quartile_cols,
                       breaks = c("Q1","Q2","Q3","Q4"),
                       labels = get_quartile_label(1:4, "White")) +
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
      color = black_prop_q_label, linetype = series,
      group = interaction(black_prop_q_label, series))) +
  geom_line(linewidth = 1.0) +
  geom_point(shape = 21, size = 2.6, stroke = 0.8, fill = "white") +
    geom_text_repel(aes(label = label_txt),
                    size = 3, box.padding = 0.25, point.padding = 0.2,
                    max.overlaps = 30, segment.color = "grey70", segment.size = 0.3) +
    scale_color_manual(values = quartile_cols,
                       breaks = c("Q1","Q2","Q3","Q4"),
                       labels = get_quartile_label(1:4, "Black")) +
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
    arrange(white_prop_q_label, year, subgroup)
)

# White quartiles — wide (rows = year × quartile; cols = Total, Students with Disabilities)
addWorksheet(wb, "white_quartiles_wide")
writeData(
  wb, "white_quartiles_wide",
  sum_white %>%
    mutate(pooled_rate = percent(pooled_rate, 0.1)) %>%
    select(year, white_prop_q_label, subgroup, pooled_rate) %>%
    pivot_wider(names_from = subgroup, values_from = pooled_rate) %>%
    arrange(year, white_prop_q_label)
)

# Black quartiles — tidy
addWorksheet(wb, "black_quartiles_tidy")
writeData(
  wb, "black_quartiles_tidy",
  sum_black %>% mutate(pooled_rate = percent(pooled_rate, 0.1)) %>%
  arrange(black_prop_q_label, year, subgroup)
)

# Black quartiles — wide
addWorksheet(wb, "black_quartiles_wide")
writeData(
  wb, "black_quartiles_wide",
  sum_black %>%
    mutate(pooled_rate = percent(pooled_rate, 0.1)) %>%
    select(year, black_prop_q_label, subgroup, pooled_rate) %>%
    pivot_wider(names_from = subgroup, values_from = pooled_rate) %>%
    arrange(year, black_prop_q_label)
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
