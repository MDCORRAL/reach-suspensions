# Graph scripts

This directory contains Python helpers used to produce statewide suspension graphics. The most common entry point is the statewide trends script described below.

## Python requirements

The scripts rely on these Python packages:

- `matplotlib`
- `numpy`
- `pandas`
- `pyarrow`

Install them into your active environment with pip:

```bash
python -m pip install matplotlib numpy pandas pyarrow adjustText
```

## Data dependencies

`graph_scripts/06_statewide_trends.py` expects the staged parquet files to be present in the repository's `data-stage/` directory:

- `data-stage/susp_v6_long.parquet`
- `data-stage/susp_v6_features.parquet`

The script filters to campus-level records, drops the special placeholder
school codes (`0000000`, `0000001`), and keeps Traditional schools by default.
Adjust the `SETTINGS_TO_INCLUDE` constant (defined alongside the loader in each
script) if you need to include non-traditional settings as well.

## Run statewide trends

From the project root, execute:

```bash
python graph_scripts/06_statewide_trends.py
```

The script will read the parquet inputs, generate plots, and write supporting narrative text.

## Outputs

Image files are saved to `outputs/graphs/` and narrative text files are saved to `outputs/graphs/descriptions/`. These directories are created automatically if they do not already exist.

## Suspension reason trends by level and locale

The UCLA-branded suspension reason trend charts can be regenerated with:

```bash
python graph_scripts/20_suspension_reason_trends_by_level_and_locale.py
```

The script reads `data-stage/susp_v6_long.parquet`, aggregates suspension counts
for "All Students" across each school level and locale, and exports one chart
per combination to `outputs/20_suspension_reason_trends_by_level_and_locale/`.
An additional "All Traditional" (non-charter) series is created for each level
to highlight the systemwide trend across traditional public schools. A
statewide "All Traditional" chart summarises elementary, middle, and high
schools in a single view alongside the level/locale outputs.
PNG is the default output; pass `--image-format svg` for vector renders or

`--levels`/`--locales` to limit the generated charts.

## Working from R

Analysts who prefer R can call the script via [`reticulate`](https://rstudio.github.io/reticulate/). Use `reticulate::py_install(c("matplotlib", "numpy", "pandas", "pyarrow"))` to install the Python dependencies inside the active reticulate environment, and then invoke the script with `reticulate::py_run_file("graph_scripts/06_statewide_trends.py")` or an equivalent wrapper.
