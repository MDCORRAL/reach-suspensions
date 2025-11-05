library(testthat)

source(test_path("..", "..", "R", "teacher_processing.R"))

test_that("teacher_slugify handles blanks", {
  expect_equal(teacher_slugify("Black / African American"), "black_african_american")
  expect_equal(teacher_slugify(NA_character_), "unknown")
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
