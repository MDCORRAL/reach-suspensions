+"""Generate UCLA-branded suspension reason trend charts by school level."""

+from __future__ import annotations

+import math
+from pathlib import Path
+
+import numpy as np
+import pandas as pd
+import pyarrow.parquet as pq
+from adjustText import adjust_text
+import matplotlib.pyplot as plt
+
+# ----------------------------------------------------------------------------
+# Configuration
+# ----------------------------------------------------------------------------
+DATA_PATH = Path("data-stage") / "susp_v6_long.parquet"
+OUTPUT_DIR = Path("outputs") / "20_reason_trends_by_level"
+OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
+
+# UCLA brand colors
+UCLA_DARKEST_BLUE = "#003B5C"
+UCLA_DARKER_BLUE = "#005587"
+UCLA_BLUE = "#2774AE"
+UCLA_DARKEST_GOLD = "#FFB81C"
+UCLA_DARKER_GOLD = "#FFC72C"
+UCLA_GOLD = "#FFD100"
+UCLA_LIGHTEST_BLUE = "#DAEBFE"
+
+TEXT_COLOR = UCLA_DARKEST_BLUE
+GRID_COLOR = UCLA_LIGHTEST_BLUE
+
+REASON_COLUMNS = {
+    "suspension_count_violent_incident_injury": "Violent (Injury)",
+    "suspension_count_violent_incident_no_injury": "Violent (No Injury)",
+    "suspension_count_weapons_possession": "Weapons",
+    "suspension_count_illicit_drug_related": "Illicit Drugs",
+    "suspension_count_defiance_only": "Willful Defiance",
+    "suspension_count_other_reasons": "Other",
+}
+
+REASON_PALETTE = {
+    "Violent (Injury)": UCLA_DARKEST_BLUE,
+    "Violent (No Injury)": UCLA_DARKER_BLUE,
+    "Weapons": UCLA_BLUE,
+    "Illicit Drugs": UCLA_DARKEST_GOLD,
+    "Willful Defiance": UCLA_DARKER_GOLD,
+    "Other": UCLA_GOLD,
+}
+
+LEVEL_ORDER = ["Elementary", "Middle", "High"]
+
+# ----------------------------------------------------------------------------
+# Data preparation
+# ----------------------------------------------------------------------------
+
+read_columns = [
+    "academic_year",
+    "school_level",
+    "subgroup",
+    "category_type",
+    "cumulative_enrollment",
+    *REASON_COLUMNS.keys(),
+]
+
+print("Loading suspension detail parquet…")
+parquet_table = pq.read_table(DATA_PATH, columns=read_columns)
+raw_df = parquet_table.to_pandas()
+
+filtered = (
+    raw_df
+    .loc[
+        (raw_df["category_type"] == "Race/Ethnicity")
+        & (raw_df["subgroup"] == "All Students")
+        & (raw_df["school_level"].isin(LEVEL_ORDER))
+    ]
+    .copy()
+)
+
+filtered["academic_year"] = pd.Categorical(
+    filtered["academic_year"],
+    ordered=True,
+    categories=sorted(filtered["academic_year"].dropna().unique()),
+)
+filtered["school_level"] = pd.Categorical(
+    filtered["school_level"], categories=LEVEL_ORDER, ordered=True
+)
+
+agg_dict = {col: "sum" for col in REASON_COLUMNS}
+agg_dict["cumulative_enrollment"] = "sum"
+
+aggregated = (
+    filtered
+    .groupby(["academic_year", "school_level"], observed=True, dropna=False)
+    .agg(agg_dict)
+    .reset_index()
+)
+
+melted = aggregated.melt(
+    id_vars=["academic_year", "school_level", "cumulative_enrollment"],
+    value_vars=list(REASON_COLUMNS.keys()),
+    var_name="reason",
+    value_name="count",
+)
+
+melted["reason_label"] = melted["reason"].map(REASON_COLUMNS)
+
+melted["rate"] = np.where(
+    melted["cumulative_enrollment"] > 0,
+    melted["count"] / melted["cumulative_enrollment"],
+    np.nan,
+)
+
+melted = melted.dropna(subset=["reason_label"]).copy()
+
+if melted.empty:
+    raise SystemExit("No suspension reason data available after filtering.")
+
+# ----------------------------------------------------------------------------
+# Plotting helpers
+# ----------------------------------------------------------------------------
+
+def _format_percent(value: float) -> str:
+    if value is None or (isinstance(value, float) and (math.isnan(value) or math.isinf(value))):
+        return "NA"
+    return f"{value * 100:.1f}%"
+
+
+def plot_level(df: pd.DataFrame, level: str) -> None:
+    level_df = df[df["school_level"] == level].copy()
+    level_df = level_df.dropna(subset=["rate"])
+
+    if level_df.empty:
+        print(f"Skipping {level}: no data to plot.")
+        return
+
+    years = level_df["academic_year"].cat.categories
+    x_positions = {year: idx for idx, year in enumerate(years)}
+
+    fig, ax = plt.subplots(figsize=(10, 6), dpi=300)
+    fig.patch.set_facecolor("white")
+    ax.set_facecolor("white")
+
+    texts = []
+    for reason_label, color in REASON_PALETTE.items():
+        reason_df = level_df[level_df["reason_label"] == reason_label]
+        if reason_df.empty:
+            continue
+        reason_df = reason_df.sort_values("academic_year")
+        xs = [x_positions[year] for year in reason_df["academic_year"]]
+        ys = reason_df["rate"].to_numpy() * 100
+        ax.plot(xs, ys, label=reason_label, color=color, linewidth=2.2, marker="o", markersize=6)
+        for x_val, y_val, rate_val in zip(xs, ys, reason_df["rate"]):
+            label = _format_percent(rate_val)
+            text = ax.text(
+                x_val,
+                y_val,
+                label,
+                color=color,
+                fontsize=9,
+                fontweight="bold",
+            )
+            texts.append(text)
+
+    adjust_text(
+        texts,
+        ax=ax,
+        expand_points=(1.1, 1.4),
+        expand_text=(1.1, 1.4),
+        arrowprops=dict(arrowstyle="-", color=TEXT_COLOR, lw=0.5),
+        only_move={"points": "y", "text": "xy"},
+    )
+
+    ax.set_xticks(list(x_positions.values()))
+    ax.set_xticklabels(years, rotation=45, ha="right", color=TEXT_COLOR)
+    ax.tick_params(axis="y", colors=TEXT_COLOR)
+
+    ax.set_ylabel("Suspension Rate (Percent)", color=TEXT_COLOR, fontweight="bold")
+    ax.set_xlabel("Academic Year", color=TEXT_COLOR, fontweight="bold")
+    ax.set_title(
+        f"{level} Schools — Suspension Rates by Reason",
+        color=TEXT_COLOR,
+        fontsize=16,
+        fontweight="bold",
+        pad=15,
+    )
+
+    ax.grid(True, axis="y", color=GRID_COLOR, linestyle="-", linewidth=0.8)
+    ax.grid(False, axis="x")
+
+    for spine in ax.spines.values():
+        spine.set_visible(False)
+
+    legend = ax.legend(
+        loc="upper center",
+        bbox_to_anchor=(0.5, -0.2),
+        ncol=3,
+        frameon=False,
+        labelcolor=TEXT_COLOR,
+    )
+    for text in legend.get_texts():
+        text.set_fontweight("bold")
+
+    ax.set_ylim(bottom=0)
+    ax.margins(x=0.02, y=0.15)
+
+    plt.tight_layout()
+    output_path = OUTPUT_DIR / f"20_suspension_reason_trends_{level.lower()}.png"
+    fig.savefig(output_path, dpi=300)
+    plt.close(fig)
+    print(f"Saved chart: {output_path}")
+
+
+for school_level in LEVEL_ORDER:
+    plot_level(melted, school_level)
+
+print(f"Completed charts saved in {OUTPUT_DIR.resolve()}")
