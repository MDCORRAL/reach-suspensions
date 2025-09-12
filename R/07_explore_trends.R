# R/07_explore_trends.R
# Analysis of Black student suspension rates with data labels for every year.

# --- 1) Setup -----------------------------------------------------------------
suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
  library(tidyr)
  library(ggplot2)
  library(ggrepel)   # non-overlapping labels
  library(scales)
})

# --- 2) Load Data -------------------------------------------------------------
v6 <- arrow::read_parquet(here::here("data-stage", "susp_v6_long.parquet"))

# --- helpers ------------------------------------------------------------------
order_year <- function(x) {
  # "2017-18" style; lexical sort matches chronology here
  factor(x, levels = sort(unique(x)))
}

order_quartile <- function(x) {
  u <- unique(x)
  lv <- c(
    grep("^Q1", u, value = TRUE),
    grep("^Q2", u, value = TRUE),
    grep("^Q3", u, value = TRUE),
    grep("^Q4", u, value = TRUE)
  )
  extras <- setdiff(u, lv) # e.g., "Unknown"
  factor(x, levels = c(lv, extras))
}

# --- 3) Filter for Black Students Data ---------------------------------------
black_students_data <- v6 %>%
  filter(
    category_type == "Race/Ethnicity",
    subgroup == "Black/African American"
  ) %>%
  mutate(
    academic_year     = order_year(academic_year),
    black_prop_q_label= order_quartile(black_prop_q_label),
    white_prop_q_label= order_quartile(white_prop_q_label)
  )

# ==============================================================================
# PART 1: ANALYSIS BY BLACK STUDENT ENROLLMENT QUARTILES
# ==============================================================================

# --- 4) Prepare Data for Total Suspension Rate Plot (by Black Quartile) ------
rate_by_black_quartile <- black_students_data %>%
  filter(!is.na(black_prop_q_label), black_prop_q_label != "Unknown") %>%
  group_by(academic_year, black_prop_q_label) %>%
  summarise(
    total_suspensions     = sum(total_suspensions, na.rm = TRUE),
    cumulative_enrollment = sum(cumulative_enrollment, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    suspension_rate = if_else(cumulative_enrollment > 0,
                              total_suspensions / cumulative_enrollment, NA_real_)
  )

# --- 5) CHART 1A: Black Student Suspension Rate by Black Enrollment Quartile --
p1 <- ggplot(rate_by_black_quartile,
             aes(x = academic_year, y = suspension_rate,
                 group = black_prop_q_label, color = black_prop_q_label)) +
  geom_line(linewidth = 1.2, na.rm = TRUE) +
  geom_point(size = 2.5, na.rm = TRUE) +
  geom_text_repel(aes(label = percent(suspension_rate, accuracy = 0.1)),
                  segment.linetype = "dashed", fontface = "bold", size = 3.5,
                  na.rm = TRUE) +
  scale_y_continuous(labels = percent_format(accuracy = 1), limits = c(0, NA)) +
  labs(
    title = "Black Student Suspension Rate by School's Black Enrollment Quartile",
    subtitle = "Rates for Black students are highest in schools with the fewest Black students.",
    x = "Academic Year", y = "Suspension Rate", color = "School's % Black Students"
  ) +
  theme_minimal(base_size = 14) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1),
        plot.title = element_text(face = "bold"))

# --- 6) Prepare Data for Suspension REASON Rate Plot (by Black Quartile) -----
reason_rate_by_black_quartile <- black_students_data %>%
  filter(!is.na(black_prop_q_label), black_prop_q_label != "Unknown") %>%
  group_by(academic_year, black_prop_q_label) %>%
  summarise(
    Defiance              = sum(suspension_count_defiance_only, na.rm = TRUE),
    `Violent Injury`      = sum(suspension_count_violent_incident_injury, na.rm = TRUE),
    `Violent No Injury`   = sum(suspension_count_violent_incident_no_injury, na.rm = TRUE),
    Weapons               = sum(suspension_count_weapons_possession, na.rm = TRUE),
    Drugs                 = sum(suspension_count_illicit_drug_related, na.rm = TRUE),
    Other                 = sum(suspension_count_other_reasons, na.rm = TRUE),
    enrollment            = sum(cumulative_enrollment, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  pivot_longer(
    cols = -c(academic_year, black_prop_q_label, enrollment),
    names_to = "suspension_reason", values_to = "count"
  ) %>%
  mutate(reason_rate = if_else(enrollment > 0, count / enrollment, NA_real_))

# Data for total labels in each facet
labels_reason_black_q <- reason_rate_by_black_quartile %>%
  group_by(academic_year, black_prop_q_label) %>%
  summarise(total_rate = sum(reason_rate, na.rm = TRUE), .groups = "drop")

# --- 7) CHART 1B: Suspension Reason Rates by Black Enrollment Quartile -------
p1b <- ggplot(reason_rate_by_black_quartile,
              aes(x = academic_year, y = reason_rate,
                  group = suspension_reason, fill = suspension_reason)) +
  geom_area(position = "stack", na.rm = TRUE) +
  geom_text(data = labels_reason_black_q,
            aes(x = academic_year, y = total_rate,
                label = percent(total_rate, accuracy = 0.1)),
            vjust = -0.5, fontface = "bold", inherit.aes = FALSE, na.rm = TRUE) +
  facet_wrap(~ black_prop_q_label, ncol = 2) +
  scale_y_continuous(labels = percent_format(accuracy = 1), limits = c(0, NA)) +
  labs(
    title = "Composition of Black Student Suspensions by School's Black Enrollment",
    subtitle = "Comparing the *reasons* for suspension across different school contexts.",
    x = "Academic Year", y = "Suspension Rate", fill = "Reason for Suspension"
  ) +
  theme_minimal(base_size = 14) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1),
        plot.title = element_text(face = "bold"),
        legend.position = "bottom")

# ==============================================================================
# PART 2: ANALYSIS BY WHITE STUDENT ENROLLMENT QUARTILES
# ==============================================================================

# --- 8) Prepare Data for Total Suspension Rate Plot (by White Quartile) ------
rate_by_white_quartile <- black_students_data %>%
  filter(!is.na(white_prop_q_label), white_prop_q_label != "Unknown") %>%
  group_by(academic_year, white_prop_q_label) %>%
  summarise(
    total_suspensions     = sum(total_suspensions, na.rm = TRUE),
    cumulative_enrollment = sum(cumulative_enrollment, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    suspension_rate = if_else(cumulative_enrollment > 0,
                              total_suspensions / cumulative_enrollment, NA_real_)
  )

# --- 9) CHART 2A: Black Student Suspension Rate by White Enrollment Quartile --
p2 <- ggplot(rate_by_white_quartile,
             aes(x = academic_year, y = suspension_rate,
                 group = white_prop_q_label, color = white_prop_q_label)) +
  geom_line(linewidth = 1.2, na.rm = TRUE) +
  geom_point(size = 2.5, na.rm = TRUE) +
  geom_text_repel(aes(label = percent(suspension_rate, accuracy = 0.1)),
                  segment.linetype = "dashed", fontface = "bold", size = 3.5,
                  na.rm = TRUE) +
  scale_y_continuous(labels = percent_format(accuracy = 1)) +
  labs(
    title = "Black Student Suspension Rate by School's White Enrollment Quartile",
    subtitle = "Comparing suspension rates for Black students in schools with different White populations.",
    x = "Academic Year", y = "Suspension Rate", color = "School's % White Students"
  ) +
  theme_minimal(base_size = 14) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1),
        plot.title = element_text(face = "bold"))

# --- 10) Prepare & Plot Suspension REASON Rates (by White Quartile) ----------
reason_rate_by_white_quartile <- black_students_data %>%
  filter(!is.na(white_prop_q_label), white_prop_q_label != "Unknown") %>%
  group_by(academic_year, white_prop_q_label) %>%
  summarise(
    Defiance              = sum(suspension_count_defiance_only, na.rm = TRUE),
    `Violent Injury`      = sum(suspension_count_violent_incident_injury, na.rm = TRUE),
    `Violent No Injury`   = sum(suspension_count_violent_incident_no_injury, na.rm = TRUE),
    Weapons               = sum(suspension_count_weapons_possession, na.rm = TRUE),
    Drugs                 = sum(suspension_count_illicit_drug_related, na.rm = TRUE),
    Other                 = sum(suspension_count_other_reasons, na.rm = TRUE),
    enrollment            = sum(cumulative_enrollment, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  pivot_longer(
    cols = -c(academic_year, white_prop_q_label, enrollment),
    names_to = "suspension_reason", values_to = "count"
  ) %>%
  mutate(reason_rate = if_else(enrollment > 0, count / enrollment, NA_real_))

labels_reason_white_q <- reason_rate_by_white_quartile %>%
  group_by(academic_year, white_prop_q_label) %>%
  summarise(total_rate = sum(reason_rate, na.rm = TRUE), .groups = "drop")

# --- 11) CHART 2B: Suspension Reason Rates by White Enrollment Quartile -------
p2b <- ggplot(reason_rate_by_white_quartile,
              aes(x = academic_year, y = reason_rate,
                  group = suspension_reason, fill = suspension_reason)) +
  geom_area(position = "stack", na.rm = TRUE) +
  geom_text(
    data = labels_reason_white_q,
    aes(x = academic_year, y = total_rate, label = percent(total_rate, accuracy = 0.1)),
    vjust = -0.5, fontface = "bold", inherit.aes = FALSE, na.rm = TRUE
  ) +
  facet_wrap(~ white_prop_q_label, ncol = 2) +
  scale_y_continuous(labels = percent_format(accuracy = 1), limits = c(0, NA)) +
  labs(
    title = "Composition of Black Student Suspensions by School's White Enrollment",
    subtitle = "Comparing the *reasons* for suspension across different school contexts.",
    x = "Academic Year", y = "Suspension Rate", fill = "Reason for Suspension"
  ) +
  theme_minimal(base_size = 14) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1),
        plot.title = element_text(face = "bold"),
        legend.position = "bottom")

# --- 12) Show & Save ----------------------------------------------------------
# Print to the Plots pane / device even if run via Rscript/source()
print(p1);  print(p1b);  print(p2);  print(p2b)

# Optional file outputs
dir.create(here::here("outputs"), showWarnings = FALSE)
ggsave(here::here("outputs","08a_black_rate_by_black_quartile.png"), p1,  width = 9,  height = 6, dpi = 300)
ggsave(here::here("outputs","08b_black_reason_by_black_quartile.png"), p1b, width = 10, height = 7, dpi = 300)
ggsave(here::here("outputs","08c_black_rate_by_white_quartile.png"), p2,  width = 9,  height = 6, dpi = 300)
ggsave(here::here("outputs","08d_black_reason_by_white_quartile.png"), p2b, width = 10, height = 7, dpi = 300)

invisible(TRUE)
# End of file -------------------------------------------------------------------