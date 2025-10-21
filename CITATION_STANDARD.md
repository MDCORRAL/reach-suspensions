# Citation Standard for REACH Suspension Analysis

## Overview

All outputs from this repository (graphs, tables, reports, dashboards) use a **standardized, human-readable citation** that clearly describes the data source and analysis methodology. This ensures that anyone reading our analyses can easily understand what data was used and how it was processed.

## Standard Citation Text

The standardized citation used across all outputs is:

> **Source:** REACH analysis of 2017-18 through 2023-24 suspension data from the California Department of Education's California Longitudinal Pupil Achievement Data System (CALPADS). Analysis includes traditional public schools aggregated at the school level, with suspension rates calculated as total suspensions divided by cumulative enrollment.

## Why This Citation?

This citation format was chosen to provide clear, accessible information:

1. **Who did the analysis**: REACH (not just "we" or unnamed)
2. **What time period**: Specific academic years (2017-18 through 2023-24)
3. **What data source**: California Department of Education's CALPADS system (not just file names like "susp_v6_long.parquet")
4. **What was analyzed**: Traditional public schools
5. **How rates were calculated**: Total suspensions divided by cumulative enrollment

### What We Changed

**Before** (cryptic):
```
Source: California statewide suspension data (susp_v6_long + v6 features)
```

**After** (human-readable):
```
Source: REACH analysis of 2017-18 through 2023-24 suspension data from the
California Department of Education's California Longitudinal Pupil Achievement
Data System (CALPADS). Analysis includes traditional public schools aggregated
at the school level, with suspension rates calculated as total suspensions
divided by cumulative enrollment.
```

## Quartile-Specific Citation

For analyses that use **quartile-based groupings** (e.g., schools divided by percentage of Black enrollment), use the enhanced **quartile citation** that includes an explanation of how quartiles were calculated:

> **Source:** REACH analysis of 2017-18 through 2023-24 suspension data from the California Department of Education's California Longitudinal Pupil Achievement Data System (CALPADS). Analysis includes traditional public schools aggregated at the school level, with suspension rates calculated as total suspensions divided by cumulative enrollment. Schools are divided into quartiles based on the percentage of Black student enrollment (calculated separately for each academic year). Q1 represents schools with the lowest percentage, Q4 represents schools with the highest percentage.

This provides readers with essential context about:
- **What quartiles represent**: Schools grouped by racial enrollment composition
- **How they were calculated**: Percentage of specific demographic group, calculated yearly
- **What Q1 vs Q4 means**: Lowest vs highest percentage

## How to Use the Standard Citation

### In R Scripts

Use the `standard_citation()` function from `graph_scripts/graph_utils.R`:

```r
# Load the utilities
source(here::here("graph_scripts", "graph_utils.R"))

# Use in ggplot2 captions
ggplot(data, aes(x, y)) +
  geom_line() +
  labs(
    title = "My Title",
    caption = standard_citation()  # Standard citation
  )

# Or with text wrapping for long labels
labs(
  caption = standard_citation(wrap_width = 120)
)

# For quartile-based analyses
labs(
  caption = quartile_citation("Black")  # Explains Black enrollment quartiles
)

# For analyses using multiple quartile types
labs(
  caption = quartile_citation("Black and White")
)
```

### In Python Scripts

Import the `STANDARD_CITATION` constant from `graph_scripts/palette_utils.py`:

```python
from palette_utils import STANDARD_CITATION, quartile_citation

# Use in matplotlib captions (standard)
fig.text(0.07, 0.05, STANDARD_CITATION, fontsize=10, ha="left")

# For quartile-based analyses
caption = quartile_citation("Black")
fig.text(0.07, 0.05, caption, fontsize=10, ha="left")

# For analyses using multiple quartile types
caption = quartile_citation("Black and White")
fig.text(0.07, 0.05, caption, fontsize=10, ha="left")
```

### In HTML/Dashboards

Use the full citation text in data source sections:

```html
<section aria-labelledby="data-sources-heading">
  <h3 id="data-sources-heading">Data sources</h3>
  <p>
    <strong>Source:</strong> REACH analysis of 2017-18 through 2023-24
    suspension data from the California Department of Education's California
    Longitudinal Pupil Achievement Data System (CALPADS). Analysis includes
    traditional public schools aggregated at the school level, with suspension
    rates calculated as total suspensions divided by cumulative enrollment.
  </p>
</section>
```

## Updated Files

The following files have been updated to use the standardized citation:

### R Graph Scripts (Standard Citation)
- `graph_scripts/01_statewide_disparities.R` - Uses `standard_citation()`
- `graph_scripts/03_elementary_disparities.R` - Uses `standard_citation()`
- `graph_scripts/05_unequal_burden.R` - Uses `standard_citation()`
- `graph_scripts/08_comprehensive_rates_plots.R` - Uses `standard_citation()`
- `graph_scripts/09_nonrace_demographic_trends.R` - Uses `standard_citation()`

### R Graph Scripts (Quartile Citation)
- `graph_scripts/02_statewide_quartiles.R` - Uses `quartile_citation("Black")`
- `graph_scripts/04_elementary_quartiles.R` - Uses `quartile_citation("Black")`
- `graph_scripts/07_quartile_enrollment_comparison.R` - Uses `quartile_citation("Black and White")`
- `graph_scripts/21_black_quartile_suspension_trends.R` - Uses `quartile_citation("Black and White")`

### Python Graph Scripts
- `graph_scripts/06_statewide_trends.py` - Uses `STANDARD_CITATION` and `quartile_citation()` for quartile figures

### Utility Files
- `graph_scripts/graph_utils.R` - Added `standard_citation()` and `quartile_citation()` functions
- `graph_scripts/palette_utils.py` - Added `STANDARD_CITATION` constant and `quartile_citation()` function

### HTML Dashboards
- `tail_concentration_dashboard.html` - Uses standard citation text

## Data Details

### Academic Years Covered
The data covers the following academic years:
- 2017-18
- 2018-19
- 2019-20
- 2021-22 (2020-21 is missing due to COVID-19 disruptions)
- 2022-23
- 2023-24

### Data Source
- **Primary Source**: California Department of Education (CDE)
- **Specific System**: California Longitudinal Pupil Achievement Data System (CALPADS)
- **Data Type**: School-level suspension records with demographic breakdowns

### Analysis Scope
- **Schools Included**: Traditional public schools (excludes alternative settings such as continuation schools, community day schools, etc.)
- **Geographic Scope**: California statewide
- **Aggregation Level**: School (campus) level
- **Rate Calculation**: Total suspensions ÷ cumulative enrollment

### Quartile Methodology
For analyses using quartile groupings:
- **Calculation**: Schools are divided into quartiles (Q1-Q4) based on the percentage of a specific demographic group's enrollment
- **Frequency**: Quartiles are calculated separately for each academic year
- **Interpretation**: Q1 = lowest percentage, Q4 = highest percentage
- **Common Uses**: Black enrollment quartiles, White enrollment quartiles, or combined analyses

## Questions or Updates

If you need to modify the citation (e.g., to reflect updated year ranges or different data sources), update:

1. **For R scripts**: The `standard_citation()` function in `graph_scripts/graph_utils.R`
2. **For Python scripts**: The `STANDARD_CITATION` constant in `graph_scripts/palette_utils.py`
3. **For HTML**: Update each HTML file's data sources section individually

All changes should maintain the same level of clarity and detail.
