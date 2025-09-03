# R/08_analysis_black_student_rates.R
# Analysis of Black student suspension rates, contextualized by school racial composition.

# --- 1) Setup -----------------------------------------------------------------
suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
  library(ggplot2)
  library(scales)
  library(tidyr)
})

# --- 2) Load Data -------------------------------------------------------------
v5 <- arrow::read_parquet(here::here("data-stage", "susp_v5.parquet"))

# --- 3) Filter for Black Students Data ---------------------------------------
# All subsequent analysis will focus only on Black students.
black_students_data <- v5 %>%
  filter(reporting_category == "RB")

# ==============================================================================
# PART 1: ANALYSIS BY BLACK STUDENT ENROLLMENT QUARTILES
# ==============================================================================

# --- 4) Prepare Data for Total Suspension Rate Plot (by Black Quartile) -----

rate_by_black_quartile <- black_students_data %>%
  # Filter out schools where the quartile is unknown
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

ggplot(rate_by_black_quartile, aes(x = academic_year, y = suspension_rate, group = black_prop_q_label, color = black_prop_q_label)) +
  geom_line(linewidth = 1.2) +
  geom_point(size = 2.5) +
  scale_y_continuous(labels = percent_format(accuracy = 1)) +
  labs(
    title = "Black Student Suspension Rate by School's Black Enrollment Quartile",
    subtitle = "Suspension rates for Black students are highest in schools with the fewest Black students.",
    x = "Academic Year",
    y = "Suspension Rate (per 100 Black Students)",
    color = "School's % Black Students"
  ) +
  theme_minimal(base_size = 14) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1), plot.title = element_text(face = "bold"))

# --- 6) Prepare Data for Suspension REASON Rate Plot (by Black Quartile) ----

reason_rate_by_black_quartile <- black_students_data %>%
  filter(black_prop_q_label != "Unknown") %>%
  group_by(academic_year, black_prop_q_label) %>%
  summarise(
    Defiance = sum(suspension_count_defiance_only, na.rm = TRUE),
    `Violent Injury` = sum(suspension_count_violent_incident_injury, na.rm = TRUE),
    `Violent No Injury` = sum(suspension_count_violent_incident_no_injury, na.rm = TRUE),
    Weapons = sum(suspension_count_weapons_possession, na.rm = TRUE),
    Drugs = sum(suspension_count_illicit_drug_related, na.rm = TRUE),
    Other = sum(suspension_count_other_reasons, na.rm = TRUE),
    enrollment = sum(cumulative_enrollment, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  pivot_longer(
    cols = -c(academic_year, black_prop_q_label, enrollment),
    names_to = "suspension_reason",
    values_to = "count"
  ) %>%
  mutate(
    reason_rate = if_else(enrollment > 0, (count / enrollment), 0)
  )

# --- 7) CHART 1B: Suspension Reason Rates by Black Enrollment Quartile ------

ggplot(reason_rate_by_black_quartile, aes(x = academic_year, y = reason_rate, group = suspension_reason, fill = suspension_reason)) +
  geom_area(position = "stack") +
  # Facet_wrap creates a separate plot for each quartile
  facet_wrap(~ black_prop_q_label, ncol = 2) +
  scale_y_continuous(labels = percent_format(accuracy = 1)) +
  labs(
    title = "Composition of Black Student Suspensions by School's Black Enrollment",
    subtitle = "Comparing the *reasons* for suspension across different school contexts.",
    x = "Academic Year",
    y = "Suspension Rate",
    fill = "Reason for Suspension"
  ) +
  theme_minimal(base_size = 14) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1), plot.title = element_text(face = "bold"), legend.position = "bottom")


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
  mutate(
    suspension_rate = if_else(cumulative_enrollment > 0, (total_suspensions / cumulative_enrollment), 0)
  )

# --- 9) CHART 2A: Black Student Suspension Rate by White Enrollment Quartile --

ggplot(rate_by_white_quartile, aes(x = academic_year, y = suspension_rate, group = white_prop_q_label, color = white_prop_q_label)) +
  geom_line(linewidth = 1.2) +
  geom_point(size = 2.5) +
  scale_y_continuous(labels = percent_format(accuracy = 1)) +
  labs(
    title = "Black Student Suspension Rate by School's White Enrollment Quartile",
    subtitle = "Comparing suspension rates for Black students in schools with different White populations.",
    x = "Academic Year",
    y = "Suspension Rate (per 100 Black Students)",
    color = "School's % White Students"
  ) +
  theme_minimal(base_size = 14) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1), plot.title = element_text(face = "bold"))

# --- 10) Prepare & Plot Suspension REASON Rates (by White Quartile) ---------
# (This follows the same logic as steps 6 & 7)

reason_rate_by_white_quartile <- black_students_data %>%
  filter(white_prop_q_label != "Unknown") %>%
  group_by(academic_year, white_prop_q_label) %>%
  summarise(
    Defiance = sum(suspension_count_defiance_only, na.rm = TRUE),
    `Violent Injury` = sum(suspension_count_violent_incident_injury, na.rm = TRUE),
    `Violent No Injury` = sum(suspension_count_violent_incident_no_injury, na.rm = TRUE),
    # ... (add other suspension reason columns here)
    enrollment = sum(cumulative_enrollment, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  pivot_longer(
    cols = -c(academic_year, white_prop_q_label, enrollment),
    names_to = "suspension_reason",
    values_to = "count"
  ) %>%
  mutate(
    reason_rate = if_else(enrollment > 0, (count / enrollment), 0)
  )

ggplot(reason_rate_by_white_quartile, aes(x = academic_year, y = reason_rate, group = suspension_reason, fill = suspension_reason)) +
  geom_area(position = "stack") +
  facet_wrap(~ white_prop_q_label, ncol = 2) +
  scale_y_continuous(labels = percent_format(accuracy = 1)) +
  labs(
    title = "Composition of Black Student Suspensions by School's White Enrollment",
    subtitle = "Comparing the *reasons* for suspension across different school contexts.",
    x = "Academic Year",
    y = "Suspension Rate",
    fill = "Reason for Suspension"
  ) +
  theme_minimal(base_size = 14) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1), plot.title = element_text(face = "bold"), legend.position = "bottom")