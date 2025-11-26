#!/usr/bin/env Rscript
# Check if diagnostic script pattern matches the same columns as regression

suppressPackageStartupMessages({
  library(arrow)
  library(here)
})

df <- read_parquet(here("data-stage", "susp_v6_teacher_features.parquet"))

cat("="<strrep("=", 70), "\n")
cat("COMPARING COLUMN PATTERNS\n")
cat(strrep("=", 70), "\n\n")

# Pattern from diagnostic script
diagnostic_pattern <- "teacher.*_(african_american|asian|hispanic|white|filipino|american_indian|native_hawaiian|pacific_islander|two_or_more).*_share$"

# Pattern from regression (using TEACHER_RACE_SLUGS)
TEACHER_RACE_SLUGS <- c(
  "african_american",
  "asian",
  "filipino",
  "hispanic_or_latino",
  "american_indian_or_alaska_native",
  "native_hawaiian_pacific_islander",
  "pacific_islander",
  "white",
  "two_or_more_races",
  "not_reported"
)
regression_pattern <- paste0("teacher.*_(", paste(TEACHER_RACE_SLUGS, collapse = "|"), ")_share$")

# Find columns
diagnostic_cols <- grep(diagnostic_pattern, names(df), value = TRUE, ignore.case = TRUE)
regression_cols <- grep(regression_pattern, names(df), value = TRUE, ignore.case = TRUE)

cat("1. DIAGNOSTIC PATTERN MATCHES\n")
cat(strrep("-", 70), "\n")
cat("Total columns:", length(diagnostic_cols), "\n")
cat("Sample (first 5):\n")
cat(paste("  -", head(diagnostic_cols, 5)), sep = "\n")
cat("\n")

cat("2. REGRESSION PATTERN MATCHES\n")
cat(strrep("-", 70), "\n")
cat("Total columns:", length(regression_cols), "\n")
cat("Sample (first 5):\n")
cat(paste("  -", head(regression_cols, 5)), sep = "\n")
cat("\n")

# Find differences
in_regression_not_diagnostic <- setdiff(regression_cols, diagnostic_cols)
in_diagnostic_not_regression <- setdiff(diagnostic_cols, regression_cols)

cat("3. DIFFERENCES\n")
cat(strrep("-", 70), "\n")
if (length(in_regression_not_diagnostic) > 0) {
  cat("⚠️  Columns matched by REGRESSION but NOT by DIAGNOSTIC:\n")
  cat(paste("  -", in_regression_not_diagnostic), sep = "\n")
  cat("\n")
} else {
  cat("✓ No columns in regression but not in diagnostic\n\n")
}

if (length(in_diagnostic_not_regression) > 0) {
  cat("⚠️  Columns matched by DIAGNOSTIC but NOT by REGRESSION:\n")
  cat(paste("  -", in_diagnostic_not_regression), sep = "\n")
  cat("\n")
} else {
  cat("✓ No columns in diagnostic but not in regression\n\n")
}

if (length(in_regression_not_diagnostic) == 0 && length(in_diagnostic_not_regression) == 0) {
  cat("✓ ✓ ✓ PATTERNS MATCH EXACTLY ✓ ✓ ✓\n\n")
} else {
  cat("❌ PATTERNS DO NOT MATCH - DIAGNOSTIC SCRIPT MAY BE INACCURATE\n\n")
}
