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

test_that('scale_fill_manual uses pal_reason without warning', {
  df <- tibble::tibble(
    reason_lab = factor(names(pal_reason), levels = names(pal_reason)),
    n = seq_along(pal_reason)
  )
  p <- ggplot2::ggplot(df, ggplot2::aes(reason_lab, n, fill = reason_lab)) +
    ggplot2::geom_col() +
    ggplot2::scale_fill_manual(values = pal_reason,
                               breaks = names(pal_reason),
                               drop = FALSE)
  expect_no_warning(ggplot2::ggplot_build(p))
})
