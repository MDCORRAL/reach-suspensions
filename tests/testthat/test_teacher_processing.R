library(testthat)

source(test_path("..", "..", "R", "teacher_processing.R"))

test_that("teacher_slugify handles blanks", {
  expect_equal(teacher_slugify("Black / African American"), "black_african_american")
  expect_equal(teacher_slugify(NA_character_), "unknown")
})

test_that("teacher_longify_wide_counts pivots plain race columns", {
  df <- tibble::tibble(
    academic_year = "2023-24",
    cds_school = "01000010100001",
    total_staff_count = 15,
    african_american = 5,
    white = 10
  )

  res <- teacher_longify_wide_counts(df)

  expect_true(all(c("race_ethnicity", "staff_count") %in% names(res)))
  expect_equal(sort(unique(res$race_ethnicity)), sort(c("Black/African American", "White")))
  expect_equal(res$staff_count[res$race_ethnicity == "Black/African American"], 5)
  expect_equal(res$staff_count[res$race_ethnicity == "White"], 10)
  expect_true(all(res$total_staff_count == 15))
})

test_that("teacher_longify_wide_counts handles suffixed metrics", {
  df <- tibble::tibble(
    academic_year = "2023-24",
    cds_school = "01000010100001",
    fte_african_american = 4,
    fte_white = 6,
    headcount_african_american = 5,
    headcount_white = 7
  )

  res <- teacher_longify_wide_counts(df)

  expect_true(all(c("fte", "headcount") %in% names(res)))
  expect_equal(res$fte[res$race_ethnicity == "Black/African American"], 4)
  expect_equal(res$headcount[res$race_ethnicity == "White"], 7)
})

test_that("teacher_summarise_long aggregates race and gender totals", {
  df <- tibble::tibble(
    academic_year = rep("2023-24", 5),
    cds_school    = rep("01000010100001", 5),
    school_code   = rep("0000001", 5),
    county_code   = rep("01", 5),
    district_code = rep("00001", 5),
    aggregate_level = rep("S", 5),
    charter_yn = rep("No", 5),
    reporting_category = c("RB", "RB", "RW", "RW", "TA"),
    reporting_category_description = c(
      "Black/African American", "Black/African American", "White", "White", "Total"
    ),
    staff_gender_code = c("GF", "GM", "GF", "GM", "ALL"),
    fte = c(4, 3, 6, 5, 18),
    headcount = c(4, 3, 6, 5, 18)
  )

  summary <- teacher_summarise_long(df)

  expect_equal(summary$teacher_fte_total, 18)
  expect_equal(summary$teacher_fte_black_african_american, 7)
  expect_equal(summary$teacher_fte_white, 11)
  expect_equal(summary$teacher_fte_by_gender_female, 10)
  expect_equal(summary$teacher_fte_by_gender_male, 8)

  expect_equal(summary$teacher_fte_black_african_american_share, 7 / 18)
  expect_equal(summary$teacher_fte_by_gender_female_share, 10 / 18)
})
