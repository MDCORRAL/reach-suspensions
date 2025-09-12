##23_rates_by_year_quartile_subgroup 

suppressPackageStartupMessages({
  library(dplyr); library(stringr); library(forcats); library(janitor)
  library(arrow); library(here); library(ggplot2); library(openxlsx); library(scales); library(tidyr)
})

# --- Paths -------------------------------------------------------------------
# CORRECTED, PORTABLE PATH
DATA_STAGE <- here("data-stage")
V6F_PARQ <- file.path(DATA_STAGE, "susp_v6_features.parquet")  # keys + flags
V6L_PARQ <- file.path(DATA_STAGE, "susp_v6_long.parquet")      # tidy subgroup metrics

OUT_IMG  <- here("outputs", "rates_by_year_blackquartile_by_subgroup.png")
OUT_XLSX <- here("outputs", "rates_by_year_blackquartile_by_subgroup.xlsx")
dir.create(here("outputs"), showWarnings = FALSE)

# --- Helpers -----------------------------------------------------------------
safe_div <- function(n, d) ifelse(is.na(d) | d == 0, NA_real_, n / d)

canon_label <- function(x) {
  xl <- stringr::str_to_lower(x %||% "")
  dplyr::case_when(
    stringr::str_detect(xl, "black|african")                               ~ "Black/African American",
    stringr::str_detect(xl, "white")                                       ~ "White",
    stringr::str_detect(xl, "hispanic|latino")                             ~ "Hispanic/Latino",
    stringr::str_detect(xl, "\\basian\\b")                                 ~ "Asian",
    stringr::str_detect(xl, "filipino")                                    ~ "Filipino",
    stringr::str_detect(xl, "american indian|alaska")                      ~ "American Indian/Alaska Native",
    stringr::str_detect(xl, "pacific islander|hawaiian")                   ~ "Native Hawaiian/Pacific Islander",
    stringr::str_detect(xl, "two or more|multiracial|two or more races")   ~ "Two or More Races",
    stringr::str_detect(xl, "english\\s*only|\\beo\\b")                 ~ "English Only",
    stringr::str_detect(xl, "english\\s*learner|\\bell\\b")            ~ "English Learner",
    stringr::str_detect(xl, "socioeconomically disadvantaged|\\bsed\\b|low\\s*income|economically disadvantaged")
    ~ "Socioeconomically Disadvantaged",
    stringr::str_detect(xl, "foster|\\bfy\\b")                           ~ "Foster Youth",
    stringr::str_detect(xl, "migrant|\\bmg\\b")                          ~ "Migrant",
    stringr::str_detect(xl, "homeless|\\bhl\\b")                         ~ "Homeless",
    stringr::str_detect(xl, "students? with disabilities|special\\s*education|\\bswd\\b|\\bsped\\b")
    ~ "Students with Disabilities",
    stringr::str_detect(xl, "all students|\\b(total|all)\\b")              ~ "Total",
    TRUE ~ NA_character_
  )
}

# --- Load v6 features (authoritative keys/flags) -----------------------------
# --- Load v6 features (authoritative keys/flags) -----------------------------
v6_features <- read_parquet(V6F_PARQ) %>%
  clean_names() %>%
  transmute(
    school_code    = as.character(school_code),
    year           = as.character(year),
    black_prop_q   = as.integer(black_prop_q),
    is_traditional = !is.na(is_traditional) & is_traditional
  ) %>%
  filter(!is.na(black_prop_q))

# Pad to common width for stable joins
padw <- max(nchar(v6_features$school_code), na.rm = TRUE)
v6_features <- v6_features %>% mutate(school_code = str_pad(school_code, padw, "left", "0"))

# (Optional) quick diagnostic if you’re curious
# print(v6_features %>% count(black_prop_q, sort = TRUE))


# --- Get subgroup school-level rates (from v6_long) --------------------------
stopifnot(file.exists(V6L_PARQ))
long_src <- read_parquet(V6L_PARQ) %>% clean_names() %>%
  transmute(
    school_code = str_pad(as.character(school_code), padw, "left", "0"),
    year        = as.character(year),
    subgroup    = canon_label(subgroup),
    rate        = as.numeric(rate)
  ) %>%
  filter(!is.na(subgroup))

# --- Join keys + filter to traditional ---------------------------------------
analytic <- long_src %>%
  inner_join(v6_features, by = c("school_code","year")) %>%
  filter(is_traditional, !is.na(black_prop_q), !is.na(rate)) %>%
  mutate(black_prop_q = factor(paste0("Q", black_prop_q), levels = paste0("Q",1:4)))

if (nrow(analytic) == 0) stop("No rows after join/filter. Check year formats or keys.")
# With the fixed normalizer, this will pass:
stopifnot(n_distinct(analytic$black_prop_q) <= 4)


# --- Summarize by year × black quartile × subgroup ---------------------------
sum_by <- analytic %>%
  group_by(year, black_prop_q, subgroup) %>%
  summarise(
    n_schools = dplyr::n(),
    n_valid   = sum(!is.na(rate)),
    mean_rate = mean(rate, na.rm = TRUE),
    sd_rate   = sd(rate, na.rm = TRUE),
    se_rate   = dplyr::if_else(n_valid > 1, sd_rate / sqrt(n_valid), NA_real_),
    ci_low    = mean_rate - qt(0.975, df = pmax(n_valid - 1, 1)) * se_rate,
    ci_high   = mean_rate + qt(0.975, df = pmax(n_valid - 1, 1)) * se_rate,
    .groups   = "drop"
  )%>%
  arrange(subgroup, year, black_prop_q)

# --- Plot: one facet per subgroup; lines = Black quartiles over time ---------
# --- Palette (reuse across scripts) ------------------------------------------
reach_quartile_cols <- c(
  Q1 = "#0072B2",  # blue
  Q2 = "#E69F00",  # orange
  Q3 = "#009E73",  # green
  Q4 = "#D55E00"   # vermillion
)
scale_linetype_manual <- function(...) {
  scale_linetype_manual(
    ...,
    values = c(
      Q1 = "solid",
      Q2 = "dashed",
      Q3 = "dotted",
      Q4 = "dotdash"
    )
  )
}

# make year an ordered factor so lines connect left→right in time
year_levels <- analytic$year |> unique() |> sort()
sum_by <- sum_by |>
  mutate(
    year         = factor(year, levels = year_levels),
    black_prop_q = forcats::fct_relevel(black_prop_q, "Q1","Q2","Q3","Q4")
  )

p <- ggplot(sum_by, aes(x = year, y = mean_rate, group = black_prop_q, color = black_prop_q)) +
  geom_line(size = 0.9) +
  geom_point(size = 2) +
  geom_errorbar(aes(ymin = ci_low, ymax = ci_high), width = 0.08, alpha = 0.7) +
  scale_color_manual(values = reach_quartile_cols, name = "Black Enr. Quartile",
                     breaks = c("Q1","Q2","Q3","Q4")) +
  scale_y_continuous(labels = scales::percent_format(accuracy = 0.1)) +
  labs(
    title = "Suspension Rates by Year, Black Enrollment Quartile, and Subgroup",
    x = "Academic Year",
    y = "Mean Suspension Rate (school-level)"
  ) +
  facet_wrap(~ subgroup, scales = "free_y") +
  theme_minimal(base_size = 12) +
  theme(
    axis.text.x = element_text(angle = 45, hjust = 1),
    strip.text = element_text(face = "bold"),
    legend.position = "bottom"
  )


ggsave(OUT_IMG, p, width = 12, height = 8, dpi = 300)

# --- Excel: tidy + wide -------------------------------------------------------
wb <- createWorkbook() 

addWorksheet(wb, "tidy_by_year_q_subgroup")
writeData(
  wb, "tidy_by_year_q_subgroup",
  sum_by |>
    mutate(
    across(c(year, black_prop_q, subgroup), as.character),
      across(c(mean_rate, ci_low, ci_high), ~ scales::percent(.x, accuracy = 0.1))
    )
)

wide_n <- sum_by |>
  select(year, black_prop_q, subgroup, n_schools) |>
  mutate(across(c(year, black_prop_q), as.character)) |>
  pivot_wider(names_from = subgroup, values_from = n_schools) |>
  arrange(year, black_prop_q)

wide_rates <- sum_by |>
  select(year, black_prop_q, subgroup, mean_rate) |>
  mutate(across(c(year, black_prop_q), as.character),
         mean_rate = scales::percent(mean_rate, accuracy = 0.1)) |>
  tidyr::pivot_wider(names_from = subgroup, values_from = mean_rate) |>
  arrange(year, black_prop_q)

openxlsx::addWorksheet(wb, "wide_rates")
openxlsx::writeData(wb, "wide_rates", wide_rates)
openxlsx::saveWorkbook(wb, OUT_XLSX, overwrite = TRUE)


addWorksheet(wb, "wide_N")
writeData(wb, "wide_N", wide_n) # 


saveWorkbook(wb, OUT_XLSX, overwrite = TRUE)

message("Done:\n- Plot: ", OUT_IMG, "\n- Excel: ", OUT_XLSX,
        "\nSources: ", if (file.exists(V6L_PARQ)) "v6_long + v6_features" else "v5 + v6_features")
