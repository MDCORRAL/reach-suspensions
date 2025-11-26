# Custom Charts Directory

This directory contains individual custom chart scripts that generate one-off visualizations or variations of standard charts.

## Purpose

Use this directory when you need:
- A single custom chart (not part of the main pipeline)
- A variation of an existing chart with different styling
- An experimental visualization
- A one-time analysis chart

## Creating a New Custom Chart

### Quick Start

1. **Copy the template**:
   ```bash
   cp template_custom_chart.py my_chart_name.py
   ```

2. **Edit the configuration** section at the top

3. **Customize the plotting** function

4. **Run your script**:
   ```bash
   source ../../.venv/bin/activate
   python my_chart_name.py
   ```

### Available Templates

- `template_custom_chart.py` - General-purpose template
- `smooth_statewide_reasons.py` - Example: smoothed line chart

## Best Practices

✅ **DO**:
- Use descriptive filenames (e.g., `smooth_elementary_trends.py`)
- Include docstring explaining what the chart shows
- Use shared utilities from `palette_utils.py`, `plotting_helpers.py`
- Add standard citation and subtitle
- Save outputs to `outputs/custom_charts/`

❌ **DON'T**:
- Hard-code colors (use `DISCIPLINE_BASE_PALETTE`)
- Skip data validation (use `audit_counts_against_enrollment`)
- Forget citations
- Use generic names like `test.py` or `chart1.py`

## Output Location

Custom charts are saved to:
```
outputs/custom_charts/your_chart_name.png
```

## Examples

See `smooth_statewide_reasons.py` for a complete working example that:
- Loads data using shared utilities
- Applies smooth line interpolation
- Removes data labels for clean presentation
- Uses UCLA brand styling

## Help

- **Workflow Guide**: `../CUSTOM_GRAPH_WORKFLOW.md`
- **Consistency Review**: `../CONSISTENCY_REVIEW_REPORT.md`
- **UCLA Colors**: `docs/protocols/UCLA-Brand-Colors.md`
