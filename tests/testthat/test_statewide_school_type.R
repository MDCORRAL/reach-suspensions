library(testthat)
library(dplyr)

test_that("statewide totals include school_type field", {
  df <- tibble(
    category_type = "Race/Ethnicity",
    subgroup = "All Students",
    academic_year = 2020,
    school_level = c("Elementary", "Adult"),
    total_suspensions = c(10, 5),
    cumulative_enrollment = c(100, 50)
  )

  v6 <- df %>%
    mutate(
      school_group = school_level,
      school_type = case_when(
        school_level %in% c("Adult", "Alternative", "Community Day", "Juvenile Court", "Other") ~ "Non-traditional",
        TRUE ~ "Traditional"
      )
    )

  v6_all <- bind_rows(
    v6,
    v6 %>% mutate(school_group = "All"),
    v6 %>% mutate(school_type = "All"),
    v6 %>% mutate(school_group = "All", school_type = "All")
  )

  statewide_all <- v6_all %>%
    group_by(academic_year, subgroup, school_group, school_type) %>%
    summarise(total_suspensions = sum(total_suspensions),
              total_enrollment = sum(cumulative_enrollment),
              .groups = "drop")

  expect_true(all(c("school_group", "school_type") %in% names(statewide_all)))
  expect_equal(
    statewide_all %>%
      filter(school_group == "All", school_type == "All") %>%
      pull(total_suspensions),
    sum(df$total_suspensions)
  )
})

