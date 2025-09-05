suppressPackageStartupMessages({
  library(dplyr); library(stringr); library(forcats); library(janitor)
  library(arrow); library(here); library(ggplot2); library(openxlsx); library(scales); library(tidyr)
})

# --- Paths -------------------------------------------------------------------
DATA_STAGE <- normalizePath(
  "~/Library/CloudStorage/GoogleDrive-mdcorral@g.ucla.edu/.shortcut-targets-by-id/1qNAOKIg0UjuT3XWFlk4dkDLN6UPWJVGx/Center for the Transformation of Schools/Research/CA Race Education And Community Healing (REACH)/2. REACH Network (INTERNAL)/15. REACH Baseline Report_Summer 2025/6. R Data Analysis Project Folders/reach-suspensions/data-stage",
  mustWork = TRUE
)
V6F_PARQ <- file.path(DATA_STAGE, "susp_v6_features.parquet")  # keys + flags
V6L_PARQ <- file.path(DATA_STAGE, "susp_v6_long.parquet")      # (optional) tidy subgroup metrics
V5_PARQ  <- file.path(DATA_STAGE, "susp_v5.parquet")           # fallback source for subgroup rates

OUT_IMG  <- here("outputs", "rates_by_year_blackquartile_by_subgroup.png")
OUT_XLSX <- here("outputs", "rates_by_year_blackquartile_by_subgroup.xlsx")
dir.create(here("outputs"), showWarnings = FALSE)

safe_div <- function(n, d) ifelse(is.na(d) | d == 0, NA_real_, n / d)

# Normalize Black quartile labels
norm_black_q <- function(x) {
  x <- as.character(x)
  x <- stringr::str_trim(x)
  num <- suppressWarnings(as.integer(stringr::str_extract(x, "[1-4]")))
  forcats::fct_relevel(factor(paste0("Q", num)), "Q1","Q2","Q3","Q4")
}

# Canonicalize subgroup labels into the set you care about
canon_label <- function(x) {
  xl <- stringr::str_to_lower(x)
  dplyr::case_when(
    stringr::str_detect(xl, "\\b(total|all)\\b")                          ~ "Total",
    stringr::str_detect(xl, "black|african")                               ~ "Black/African American",
    stringr::str_detect(xl, "white")                                       ~ "White",
    stringr::str_detect(xl, "hispanic|latino")                             ~ "Hispanic/Latino",
    stringr::str_detect(xl, "\\basian\\b")                                 ~ "Asian",
    stringr::str_detect(xl, "filipino")                                    ~ "Filipino",
    stringr::str_detect(xl, "american indian|alaska")                      ~ "American Indian/Alaska Native",
    stringr::str_detect(xl, "pacific islander|hawaiian")                   ~ "Native Hawaiian/Pacific Islander",
    stringr::str_detect(xl, "two or more|multiracial|two or more races")   ~ "Two or More Races",
    stringr::str_detect(xl, "students? with disabilities|special\\s*education") ~ "Students with Disabilities",
    TRUE ~ NA_character_
  )
}

# --- Load v6 features (authoritative keys/flags) -----------------------------
v6_features <- read_parquet(V6F_PARQ) %>% clean_names() %>%
  transmute(
    school_code = as.character(school_code),
    year        = as.character(year),
    black_q     = norm_black_q(black_q),
    is_traditional = !is.na(is_traditional) & is_traditional
  )

# Pad school_code to a shared width for stable joins
padw <- max(nchar(v6_features$school_code), na.rm = TRUE)
v6_features <- v6_features %>% mutate(school_code = str_pad(school_code, padw, "left", "0"))

# --- Get subgroup school-level rates (prefer v6_long; else v5) ---------------
if (file.exists(V6L_PARQ)) {
  # v6_long: expected columns: school_code, year, category, subgroup, num, den, rate
  long_src <- read_parquet(V6L_PARQ) %>% clean_names() %>%
    transmute(
      school_code = str_pad(as.character(school_code), padw, "left", "0"),
      year        = as.character(year),
      subgroup    = canon_label(subgroup),
      rate        = as.numeric(rate)
    ) %>%
    filter(!is.na(subgroup))
} else {
  # Fallback to v5
  v5 <- read_parquet(V5_PARQ) %>% clean_names()
  subg_desc <- if ("reporting_category_description" %in% names(v5)) "reporting_category_description" else "reporting_category"
  num_col   <- if ("unduplicated_count_of_students_suspended_total" %in% names(v5))
    "unduplicated_count_of_students_suspended_total" else "total_suspensions"
  den_col   <- "cumulative_enrollment"
  # school-level only if present
  if ("aggregate_level" %in% names(v5)) v5 <- v5 %>% filter(str_to_lower(aggregate_level) %in% c("school","sch"))
  
  long_src <- v5 %>%
    transmute(
      school_code = str_pad(as.character(school_code), padw, "left", "0"),
      year        = as.character(academic_year),
      subgroup    = canon_label(.data[[subg_desc]]),
      rate        = safe_div(as.numeric(.data[[num_col]]), as.numeric(.data[[den_col]]))
    ) %>%
    filter(!is.na(subgroup))
}

# --- Join keys + filter to traditional ---------------------------------------
analytic <- long_src %>%
  inner_join(v6_features, by = c("school_code","year")) %>%
  filter(is_traditional, !is.na(black_q), !is.na(rate))

if (nrow(analytic) == 0) stop("No rows after join/filter. Check year formats or keys.")

# --- Summarize by year × black quartile × subgroup ---------------------------
sum_by <- analytic %>%
  group_by(year, black_q, subgroup) %>%
  summarise(
    n_schools = n(),
    mean_rate = mean(rate, na.rm = TRUE),
    sd_rate   = sd(rate, na.rm = TRUE),
    se_rate   = sd_rate / sqrt(n_schools),
    ci_low    = mean_rate - 1.96 * se_rate,
    ci_high   = mean_rate + 1.96 * se_rate,
    .groups = "drop"
  ) %>%
  arrange(subgroup, year, black_q)

# --- Plot: one facet per subgroup; lines = Black quartiles over time ---------
# make year an ordered factor so lines connect left→right in time
sum_by <- sum_by %>%
  dplyr::mutate(year = forcats::fct_inorder(year),
                black_q = forcats::fct_relevel(black_q, "Q1","Q2","Q3","Q4"))

p <- ggplot(sum_by, aes(x = year, y = mean_rate, group = black_q)) +
  geom_line(aes(linetype = black_q)) +
  geom_point() +
  geom_errorbar(aes(ymin = ci_low, ymax = ci_high), width = 0.08) +
  scale_y_continuous(labels = scales::percent_format(accuracy = 0.1)) +
  labs(
    title = "Suspension Rates by Year, Black Enrollment Quartile, and Subgroup",
    x = "Academic Year",
    y = "Mean Suspension Rate (school-level)",
    linetype = "Black Enr. Quartile",
    caption = "Traditional schools only; points are school-level means with 95% CIs."
  ) +
  facet_wrap(~ subgroup, scales = "free_y") +
  theme_minimal(base_size = 12) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

ggsave(OUT_IMG, p, width = 12, height = 8, dpi = 300)

# --- Excel: tidy + wide -------------------------------------------------------
wb <- createWorkbook()
addWorksheet(wb, "tidy_by_year_q_subgroup")
writeData(
  wb, "tidy_by_year_q_subgroup",
  sum_by %>% mutate(across(c(mean_rate, ci_low, ci_high), ~ percent(.x, accuracy = 0.1)))
)

wide_rates <- sum_by %>%
  select(year, black_q, subgroup, mean_rate) %>%
  mutate(mean_rate = percent(mean_rate, accuracy = 0.1)) %>%
  pivot_wider(names_from = subgroup, values_from = mean_rate) %>%
  arrange(year, black_q)

addWorksheet(wb, "wide_rates")
writeData(wb, "wide_rates", wide_rates)

saveWorkbook(wb, OUT_XLSX, overwrite = TRUE)

message("Done:\n- Plot: ", OUT_IMG, "\n- Excel: ", OUT_XLSX,
        "\nSources: ", if (file.exists(V6L_PARQ)) "v6_long + v6_features" else "v5 + v6_features")
