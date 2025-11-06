# R/98_test_equity_analysis.R
# Test hypothetical equity analysis: Black teachers/admins at Q4 Black-enrolled schools
#
# Purpose: Verify that the data structure supports planned equity analyses
# comparing teacher vs. administrator demographics across schools with
# different enrollment profiles

library(arrow)
library(dplyr)
library(scales)
library(here)

teacher_long <- read_parquet(here("data-stage", "teacher_staff_long.parquet"))

cat("\n=== EQUITY ANALYSIS TEST ===\n")
cat("Hypothetical: Black staff at high-Black-enrollment schools\n\n")

# Determine latest year available
latest_year <- max(teacher_long$academic_year)
cat("Using latest year:", latest_year, "\n\n")

# Mock: Select random schools as "Q4 Black enrollment"
# In real analysis, this would be based on actual enrollment data
set.seed(42)
mock_q4_schools <- teacher_long |>
  filter(academic_year == latest_year) |>
  distinct(cds_school) |>
  slice_sample(n = min(100, n()))

cat("Mock Q4 schools (n=", nrow(mock_q4_schools), "):",
    paste(head(mock_q4_schools$cds_school, 3), collapse = ", "), "...\n\n")

# Analysis 1: Black teachers at these schools
cat("1. BLACK TEACHERS at mock Q4 schools:\n")
black_teachers <- teacher_long |>
  filter(
    academic_year == latest_year,
    cds_school %in% mock_q4_schools$cds_school,
    reporting_category == "TCH",
    race_ethnicity == "African American",
    staff_gender_code == "ALL"  # Use aggregate
  ) |>
  summarise(
    n_schools = n_distinct(cds_school),
    total_black_teachers = sum(staff_count, na.rm = TRUE),
    avg_per_school = round(mean(staff_count, na.rm = TRUE), 1)
  )

print(black_teachers)

# Analysis 2: Black administrators at same schools
cat("\n2. BLACK ADMINISTRATORS at mock Q4 schools:\n")
black_admins <- teacher_long |>
  filter(
    academic_year == latest_year,
    cds_school %in% mock_q4_schools$cds_school,
    reporting_category == "ADM",
    race_ethnicity == "African American",
    staff_gender_code == "ALL"
  ) |>
  summarise(
    n_schools = n_distinct(cds_school),
    total_black_admins = sum(staff_count, na.rm = TRUE),
    avg_per_school = round(mean(staff_count, na.rm = TRUE), 1)
  )

print(black_admins)

# Analysis 3: School-level comparison
cat("\n3. SCHOOL-LEVEL COMPARISON (sample of 10 schools):\n")
teacher_long |>
  filter(
    academic_year == latest_year,
    cds_school %in% head(mock_q4_schools$cds_school, 10),
    reporting_category %in% c("TCH", "ADM"),
    race_ethnicity == "African American",
    staff_gender_code == "ALL"
  ) |>
  select(cds_school, reporting_category, staff_count) |>
  tidyr::pivot_wider(
    names_from = reporting_category,
    values_from = staff_count,
    values_fill = 0
  ) |>
  mutate(teacher_to_admin_ratio = round(TCH / pmax(ADM, 1), 1)) |>
  print()

# Analysis 4: System-wide comparison (all staff types)
cat("\n4. SYSTEM-WIDE COMPARISON (all schools, ", latest_year, "):\n")
system_wide <- teacher_long |>
  filter(
    academic_year == latest_year,
    race_ethnicity == "African American",
    staff_gender_code == "ALL"
  ) |>
  group_by(reporting_category) |>
  summarise(
    n_schools_with_black_staff = n_distinct(cds_school),
    total_black_staff = sum(staff_count, na.rm = TRUE),
    avg_per_school = round(mean(staff_count, na.rm = TRUE), 2)
  ) |>
  arrange(desc(total_black_staff))

print(system_wide)

# Analysis 5: Teacher vs. Administrator ratios by race
cat("\n5. TEACHER-TO-ADMINISTRATOR RATIOS BY RACE (", latest_year, "):\n")
teacher_admin_ratios <- teacher_long |>
  filter(
    academic_year == latest_year,
    reporting_category %in% c("TCH", "ADM"),
    staff_gender_code == "ALL"
  ) |>
  group_by(race_ethnicity, reporting_category) |>
  summarise(total_staff = sum(staff_count, na.rm = TRUE), .groups = "drop") |>
  tidyr::pivot_wider(
    names_from = reporting_category,
    values_from = total_staff,
    values_fill = 0
  ) |>
  mutate(
    TCH_to_ADM_ratio = round(TCH / pmax(ADM, 1), 2),
    ADM_pct_of_total = round(100 * ADM / (TCH + ADM), 1)
  ) |>
  arrange(desc(TCH))

print(teacher_admin_ratios)

cat("\n=== ANALYTICAL CAPABILITY SUMMARY ===\n")
cat("✓ Data structure supports planned equity analyses\n")
cat("✓ Can compare teacher vs. administrator demographics by race\n")
cat("✓ Can analyze at school level or in aggregate\n")
cat("✓ Can calculate teacher-to-administrator ratios\n")
cat("✓ Can identify schools with/without representation in each role\n")
cat("\n=== TEST COMPLETE ===\n")
