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

- `data-stage/susp_v5.parquet`
- `data-stage/susp_v6_features.parquet`

Ensure both files exist before running the script.

## Run statewide trends

From the project root, execute:

```bash
python graph_scripts/06_statewide_trends.py
```

The script will read the parquet inputs, generate plots, and write supporting narrative text.

## Outputs

Image files are saved to `outputs/graphs/` and narrative text files are saved to `outputs/graphs/descriptions/`. These directories are created automatically if they do not already exist.

## Working from R

Analysts who prefer R can call the script via [`reticulate`](https://rstudio.github.io/reticulate/). Use `reticulate::py_install(c("matplotlib", "numpy", "pandas", "pyarrow"))` to install the Python dependencies inside the active reticulate environment, and then invoke the script with `reticulate::py_run_file("graph_scripts/06_statewide_trends.py")` or an equivalent wrapper.
