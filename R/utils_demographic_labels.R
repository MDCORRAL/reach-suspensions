# ---- R/utils_demographic_labels.R -----------------------------------------
# Helpers for canonical demographic subgroup labels.

suppressPackageStartupMessages({
  library(dplyr)
  library(stringr)
  library(rlang)
})

#' Map common demographic descriptors to canonical labels.
#'
#' This function standardizes various non-race subgroup descriptions such as
#' English Learner or Students with Disabilities. Inputs are case-insensitive
#' and may include common abbreviations.
#'
#' @param x Character vector of subgroup labels.
#' @return Character vector of canonical labels or `NA` when no match.
canon_demo_label <- function(x) {
  xl <- stringr::str_to_lower(x %||% "")
  dplyr::case_when(
    stringr::str_detect(xl, "\\b(total|all)\\b|all students")                           ~ "Total",
    stringr::str_detect(xl, "english\\s*only|\\beo\\b")                               ~ "English Only",
    stringr::str_detect(xl, "english\\s*learner|\\bell\\b")                           ~ "English Learner",
    stringr::str_detect(xl, "socioeconomically disadvantaged|\\bsed\\b|low\\s*income|economically disadvantaged") ~ "Socioeconomically Disadvantaged",
    stringr::str_detect(xl, "foster|\\bfy\\b")                                         ~ "Foster Youth",
    stringr::str_detect(xl, "migrant|\\bmg\\b")                                        ~ "Migrant",
    stringr::str_detect(xl, "homeless|\\bhl\\b")                                       ~ "Homeless",
    stringr::str_detect(xl, "students? with disabilities|special\\s*education|\\bswd\\b") ~ "Students with Disabilities",
    TRUE ~ NA_character_
  )
}

