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
