# R/02b_drop_charter_all.R
library(dplyr); library(arrow)

v1 <- arrow::read_parquet("data-stage/susp_v1.parquet")

v1_noall <- v1 %>%
  filter(is.na(charter_yn) | charter_yn != "All")

# sanity
stopifnot(!any(v1_noall$charter_yn %in% "All", na.rm = TRUE))

arrow::write_parquet(v1_noall, "data-stage/susp_v1_noall.parquet")
invisible(TRUE)

source("R/02b_drop_charter_all.R")
# Rerun subsequent steps on the _noall file