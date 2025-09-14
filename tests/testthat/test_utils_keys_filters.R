source(normalizePath(file.path('..','..','R','utils_keys_filters.R')))
library(testthat)

test_that('palettes have names matching their levels', {
  expect_equal(names(pal_level), LEVEL_LABELS)
  expect_equal(names(pal_locale), locale_levels)
  expect_equal(names(pal_reason), reason_labels$reason_lab)
})

test_that('add_reason_label returns no NA values for known keys', {
  df <- data.frame(reason = reason_labels$reason)
  out <- add_reason_label(df)
  expect_false(any(is.na(out$reason_lab)))
})
