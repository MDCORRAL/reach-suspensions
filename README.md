## R Project Setup with renv

This project uses [`renv`](https://rstudio.github.io/renv/) to manage R package versions.

### First time setup
```r
# Activate renv in this project
renv::activate()

# Install the exact package versions from renv.lock
renv::restore()

# After installing/removing packages
renv::snapshot()

# Just restore to sync packages with renv.lock
renv::restore()
```

##codex/add-canonical-label-for-rd-in-filters
### Canonical race labels

`R/utils_keys_filters.R` defines `canon_race_label()` and `ALLOWED_RACES` used across analyses. The function now maps CRDC code `RD` and strings such as "Not Reported" to the canonical label "Not Reported." This group is recognized for completeness but excluded from plots to avoid conflating missing data with student populations.


