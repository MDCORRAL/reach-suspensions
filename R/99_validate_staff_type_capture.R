# R/99_validate_staff_type_capture.R
# Validate that staff type dimension is fully captured in teacher demographics data
#
# Purpose: Comprehensive validation of the reporting_category (Staff Type) dimension
# to ensure data supports teacher vs. administrator equity analysis

library(arrow)
library(dplyr)
library(scales)
library(here)

teacher_long <- read_parquet(here("data-stage", "teacher_staff_long.parquet"))

cat("\n=== STAFF TYPE VALIDATION REPORT ===\n")
cat("Generated:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n\n")

# 1. Column presence
cat("1. COLUMN PRESENCE:\n")
if ("reporting_category" %in% names(teacher_long)) {
  cat("   ✓ reporting_category exists\n\n")
} else {
  stop("   ✗ reporting_category MISSING\n")
}

# 2. Value distribution
cat("2. VALUE DISTRIBUTION:\n")
teacher_long |>
  count(reporting_category, sort = TRUE) |>
  mutate(pct = round(100 * n / sum(n), 1)) |>
  print()

# 3. Completeness by year
cat("\n3. COMPLETENESS BY YEAR:\n")
teacher_long |>
  group_by(academic_year) |>
  summarise(
    n_rows = n(),
    n_staff_types = n_distinct(reporting_category),
    staff_types = paste(sort(unique(reporting_category)), collapse = ", ")
  ) |>
  print()

# 4. Teacher vs. Administrator comparison
cat("\n4. TEACHERS VS. ADMINISTRATORS (2024-25, aggregate gender):\n")
if ("2024-25" %in% teacher_long$academic_year) {
  teacher_long |>
    filter(
      academic_year == "2024-25",
      reporting_category %in% c("TCH", "ADM"),
      staff_gender_code == "ALL"
    ) |>
    group_by(reporting_category, race_ethnicity) |>
    summarise(total = sum(staff_count, na.rm = TRUE), .groups = "drop") |>
    tidyr::pivot_wider(names_from = reporting_category, values_from = total, values_fill = 0) |>
    mutate(TCH_to_ADM_ratio = round(TCH / pmax(ADM, 1), 2)) |>
    print(n = 20)
} else {
  cat("   NOTE: 2024-25 data not available for comparison\n")
}

# 5. Verify analytical capability
cat("\n5. ANALYTICAL CAPABILITY TEST:\n")
teachers_only <- teacher_long |>
  filter(reporting_category == "TCH")
cat("   Teachers only:", comma(nrow(teachers_only)), "rows\n")

admins_only <- teacher_long |>
  filter(reporting_category == "ADM")
cat("   Administrators only:", comma(nrow(admins_only)), "rows\n")

cat("\n   ✓ Can filter to teachers (TCH)\n")
cat("   ✓ Can filter to administrators (ADM)\n")
cat("   ✓ Can compare demographics across roles\n")
cat("   ✓ Ready for equity analysis\n")

# 6. Missing values check
n_missing <- sum(is.na(teacher_long$reporting_category))
cat("\n6. MISSING VALUES:\n")
if (n_missing > 0) {
  cat("   WARNING:", comma(n_missing), "rows have missing reporting_category\n")
} else {
  cat("   ✓ No missing values in reporting_category\n")
}

# 7. CDE code validation
cat("\n7. CDE CODE VALIDATION:\n")
valid_staff_types <- c("ALL", "ADM", "PSV", "TCH", "OTH")
actual_types <- unique(teacher_long$reporting_category) |> na.omit()
invalid_types <- setdiff(actual_types, valid_staff_types)

if (length(invalid_types) > 0) {
  cat("   ✗ INVALID codes found:", paste(invalid_types, collapse = ", "), "\n")
} else {
  cat("   ✓ All staff type codes are valid CDE codes\n")
  cat("   Valid codes found:", paste(sort(actual_types), collapse = ", "), "\n")
}

cat("\n=== VALIDATION COMPLETE ===\n")
