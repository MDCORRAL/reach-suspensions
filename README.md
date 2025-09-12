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

### Race label handling

Race/ethnicity values are standardized with `canon_race_label()`. The CRDC code `RD` and text variants such as "Not Reported" map to a canonical "Not Reported" label. This category is excluded from visualizations by default and is filtered out in analysis scripts.

