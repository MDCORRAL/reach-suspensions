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

### Canonical race labels

`R/utils_keys_filters.R` defines `canon_race_label()` and `ALLOWED_RACES` used across analyses. The function maps CRDC code `RD` and strings such as "Not Reported" to the canonical label "Not Reported." This category is tracked for completeness but omitted from plots to avoid conflating missing data with student populations.

## Analysis scripts

The canonical analysis of Black student suspension rates by school racial composition lives at
`Analysis/02_black_rates_by_quartiles.R`.

## Environment variables

These optional environment variables allow the project to run without hard-coded paths. Set them in your shell or `.Renviron`.

- `REACH_PROJECT_ROOT`: path to the project root. Defaults to the current working directory if unset.
- `REACH_DATA_DIR`: directory for staged data files. Defaults to `data-stage/` under the project root.
- `RAW_PATH`: full path to the raw Excel file `copy_CDE_suspensions_1718-2324_sc_race.xlsx`. Defaults to `data-raw/` under the project root.

