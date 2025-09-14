# R/08_analysis_black_student_rates.R
# Analysis of Black student suspension rates with data labels for every year.

# --- 1) Setup -----------------------------------------------------------------
suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
  library(ggplot2)
  library(scales)
  library(tidyr)
  library(ggrepel) # For non-overlapping labels
})

source(here::here("R", "utils_keys_filters.R"))

# --- 2) Load Data -------------------------------------------------------------
v6 <- arrow::read_parquet(here::here("data-stage", "susp_v6_long.parquet")) %>%
  build_keys() %>%          # Standardize CDS codes
  filter_campus_only()      # Exclude special aggregation rows

# --- 3) Filter for Black Students Data ---------------------------------------
black_students_data <- v6 %>%
  filter(
    category_type == "Race/Ethnicity",
    canon_race_label(subgroup) == "Black/African American"
  )

# ==============================================================================
# PART 1: ANALYSIS BY BLACK STUDENT ENROLLMENT QUARTILES
# ==============================================================================

# --- 4) Prepare Data for Total Suspension Rate Plot (by Black Quartile) -----
rate_by_black_quartile <- black_students_data %>%
  filter(black_prop_q_label != "Unknown") %>%
  group_by(academic_year, black_prop_q_label) %>%
  summarise(
    total_suspensions = sum(total_suspensions, na.rm = TRUE),
    cumulative_enrollment = sum(cumulative_enrollment, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    suspension_rate = if_else(cumulative_enrollment > 0, (total_suspensions / cumulative_enrollment), 0)
  )

# --- 5) CHART 1A: Black Student Suspension Rate by Black Enrollment Quartile --
p1_black_by_black <- ggplot(rate_by_black_quartile, aes(x = academic_year, y = suspension_rate,
                                                        group = black_prop_q_label, color = black_prop_q_label)) +
  geom_line(linewidth = 1.2) +
  geom_point(size = 2.5) +
  geom_text_repel(aes(label = percent(suspension_rate, accuracy = 0.1)),
                  segment.linetype = "dashed", fontface = "bold", size = 3.5) +
  scale_y_continuous(labels = percent_format(accuracy = 1)) +
  labs(
    title = "Black Student Suspension Rate by School's Black Enrollment Quartile",
    subtitle = "Rates for Black students are highest in schools with the most Black students.",  # see #2
    x = "Academic Year", y = "Suspension Rate", color = "School's % Black Students"
  ) +
  theme_minimal(base_size = 14) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1), plot.title = element_text(face = "bold"))

# --- 6) Prepare Data for Suspension REASON Rate Plot (by Black Quartile) ----
reason_rate_by_black_quartile <- black_students_data %>%
  # Most robust filter
  filter(
    !is.na(black_prop_q_label),
    black_prop_q_label != "Unknown"
  ) %>%
  group_by(academic_year, black_prop_q_label) %>%
  summarise(
    across(
      c(
        violent_injury     = suspension_count_violent_incident_injury,
        violent_no_injury  = suspension_count_violent_incident_no_injury,
        weapons_possession = suspension_count_weapons_possession,
        illicit_drug       = suspension_count_illicit_drug_related,
        defiance_only      = suspension_count_defiance_only,
        other_reasons      = suspension_count_other_reasons
      ),
      ~ sum(.x, na.rm = TRUE)
    ),
    enrollment = sum(cumulative_enrollment, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  pivot_longer(
    cols = -c(academic_year, black_prop_q_label, enrollment),
    names_to = "reason",
    values_to = "count"
  ) %>%
  add_reason_label() %>%
  mutate(
    # Convert reason labels to factors and compute rates
    reason_lab   = factor(reason_lab, levels = names(pal_reason)),
    reason_rate  = if_else(enrollment > 0, (count / enrollment), 0)
  )

# Data for total labels in each facet
labels_reason_black_q <- reason_rate_by_black_quartile %>%
  group_by(academic_year, black_prop_q_label) %>%
  summarise(total_rate = sum(reason_rate), .groups = "drop")

# --- 7) CHART 1B: Suspension Reason Rates by Black Enrollment Quartile ------
p2_reasons_by_black <- ggplot(reason_rate_by_black_quartile, aes(x = academic_year, y = reason_rate,
                                                                 group = reason_lab, fill = reason_lab)) +
  geom_area(position = "stack") +
  geom_text(data = labels_reason_black_q,
            aes(x = academic_year, y = total_rate, label = percent(total_rate, accuracy = 0.1)),
            vjust = -0.5, fontface = "bold", inherit.aes = FALSE) +
  facet_wrap(~ black_prop_q_label, ncol = 2) +
  scale_fill_manual(values = pal_reason, breaks = names(pal_reason), drop = FALSE) +
  scale_y_continuous(labels = percent_format(accuracy = 1), limits = c(0, NA)) +
  labs(
    title = "Composition of Black Student Suspensions by School's Black Enrollment",
    subtitle = "Comparing the *reasons* for suspension across different school contexts.",
    x = "Academic Year", y = "Suspension Rate", fill = "Reason for Suspension"
  ) +
  theme_minimal(base_size = 14) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1), plot.title = element_text(face = "bold"),
        legend.position = "bottom")
# ==============================================================================
# PART 2: ANALYSIS BY WHITE STUDENT ENROLLMENT QUARTILES
# ==============================================================================

# --- 8) Prepare Data for Total Suspension Rate Plot (by White Quartile) -----
rate_by_white_quartile <- black_students_data %>%
  filter(white_prop_q_label != "Unknown") %>%
  group_by(academic_year, white_prop_q_label) %>%
  summarise(
    total_suspensions = sum(total_suspensions, na.rm = TRUE),
    cumulative_enrollment = sum(cumulative_enrollment, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(suspension_rate = if_else(cumulative_enrollment > 0, (total_suspensions / cumulative_enrollment), 0))

# --- 9) CHART 2A: Black Student Suspension Rate by White Enrollment Quartile --
p3_black_by_white <- ggplot(rate_by_white_quartile, aes(x = academic_year, y = suspension_rate,
                                                        group = white_prop_q_label, color = white_prop_q_label)) +
  geom_line(linewidth = 1.2) +
  geom_point(size = 2.5) +
  geom_text_repel(aes(label = percent(suspension_rate, accuracy = 0.1)),
                  segment.linetype = "dashed", fontface = "bold", size = 3.5) +
  scale_y_continuous(labels = percent_format(accuracy = 1)) +
  labs(
    title = "Black Student Suspension Rate by School's White Enrollment Quartile",
    subtitle = "Comparing suspension rates for Black students in schools with different White populations.",
    x = "Academic Year", y = "Suspension Rate", color = "School's % White Students"
  ) +
  theme_minimal(base_size = 14) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1), plot.title = element_text(face = "bold"))

# --- 10) Prepare & Plot Suspension REASON Rates (by White Quartile) ---------
reason_rate_by_white_quartile <- black_students_data %>%
  filter(white_prop_q_label != "Unknown") %>%
  group_by(academic_year, white_prop_q_label) %>%
  summarise(
    across(
      c(
        violent_injury     = suspension_count_violent_incident_injury,
        violent_no_injury  = suspension_count_violent_incident_no_injury,
        weapons_possession = suspension_count_weapons_possession,
        illicit_drug       = suspension_count_illicit_drug_related,
        defiance_only      = suspension_count_defiance_only,
        other_reasons      = suspension_count_other_reasons
      ),
      ~ sum(.x, na.rm = TRUE)
    ),
    enrollment = sum(cumulative_enrollment, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  pivot_longer(
    cols = -c(academic_year, white_prop_q_label, enrollment),
    names_to = "reason",
    values_to = "count"
  ) %>%
  add_reason_label() %>%
  mutate(
    # Convert reason labels to factors and compute rates
    reason_lab   = factor(reason_lab, levels = names(pal_reason)),
    reason_rate  = if_else(enrollment > 0, (count / enrollment), 0)
  )

# Data for total labels in each facet
labels_reason_white_q <- reason_rate_by_white_quartile %>%
  group_by(academic_year, white_prop_q_label) %>%
  summarise(total_rate = sum(reason_rate), .groups = "drop")

# --- 11) CHART 2B: Suspension Reason Rates by White Enrollment Quartile ------
p4_reasons_by_white <- ggplot(reason_rate_by_white_quartile, aes(x = academic_year, y = reason_rate,
                                                                 group = reason_lab, fill = reason_lab)) +
  geom_area(position = "stack") +
  geom_text(data = labels_reason_white_q,
            aes(x = academic_year, y = total_rate, label = percent(total_rate, accuracy = 0.1)),
            vjust = -0.5, fontface = "bold", inherit.aes = FALSE) +
  facet_wrap(~ white_prop_q_label, ncol = 2) +
  scale_fill_manual(values = pal_reason, breaks = names(pal_reason), drop = FALSE) +
  scale_y_continuous(labels = percent_format(accuracy = 1), limits = c(0, NA)) +
  labs(
    title = "Composition of Black Student Suspensions by School's White Enrollment",
    subtitle = "Comparing the *reasons* for suspension across different school contexts.",
    x = "Academic Year", y = "Suspension Rate", fill = "Reason for Suspension"
  ) +
  theme_minimal(base_size = 14) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1), plot.title = element_text(face = "bold"),
        legend.position = "bottom")

# --- (Optional) Save files ----------------------------------------------------
dir.create(here::here("outputs"), showWarnings = FALSE)
ggsave(here::here("outputs","08b_black_rate_by_black_quartile.png"),  plot = p1_black_by_black, width = 10, height = 6, dpi = 300)
ggsave(here::here("outputs","08b_reason_rates_by_black_quartile.png"), plot = p2_reasons_by_black, width = 10, height = 7, dpi = 300)
ggsave(here::here("outputs","08b_black_rate_by_white_quartile.png"),  plot = p3_black_by_white, width = 10, height = 6, dpi = 300)
ggsave(here::here("outputs","08b_reason_rates_by_white_quartile.png"), plot = p4_reasons_by_white, width = 10, height = 7, dpi = 300)