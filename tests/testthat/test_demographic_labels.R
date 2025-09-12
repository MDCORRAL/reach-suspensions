source(normalizePath(file.path('..','..','R','demographic_labels.R')))
library(testthat)

test_that('desc_to_canon handles descriptors and abbreviations', {
  expect_equal(desc_to_canon(c('English Learner','EL','swd','Total')),
               c('EL','EL','SE','TA'))
})

test_that('code_alias_to_canon resolves known aliases', {
  expect_equal(code_alias_to_canon(c('gf','GM','sh')),
               c('SF','SM','HL'))
})

test_that('canon_demo_label returns canonical labels', {
  inputs <- c('english learner','students with disabilities','Total')
  expect_equal(canon_demo_label(inputs),
               c('English Learner','Students with Disabilities','Total'))
})

test_that('canonicalize_demo adds canonical fields', {
  df <- data.frame(subgroup='english learner', subgroup_code=NA_character_)
  out <- canonicalize_demo(df, desc_col='subgroup', code_col='subgroup_code')
  expect_equal(out$subgroup_code, 'EL')
  expect_equal(out$subgroup, 'English Learner')
  expect_equal(out$category_type, 'English Learner')
})

