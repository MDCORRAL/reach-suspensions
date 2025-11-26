# Graph Scripts Consistency Review Report

**Date**: 2025-11-26
**Reviewer**: Claude Code
**Purpose**: Ensure all visualization scripts follow established guidelines for consistency across outputs

---

## Executive Summary

**Overall Assessment**: The graph scripts demonstrate strong consistency in color palettes, data validation, and core styling. However, there are **critical gaps** in source citation and **moderate inconsistencies** in title formatting and subtitles that should be addressed to ensure professional, publication-ready outputs.

### Critical Issues (Must Fix)
1. ❌ **Missing standard citation** in UCLA-branded scripts
2. ⚠️ **Inconsistent title formatting** (dash types, structure)
3. ⚠️ **Missing subtitles** in UCLA-branded scripts

### Strengths (Working Well)
1. ✅ **Color palettes** - Consistent UCLA brand colors
2. ✅ **Data validation** - All scripts use shared validation utilities
3. ✅ **Reason labels and palettes** - Uniform across all scripts
4. ✅ **Figure styling** - Consistent white background, grid, typography

---

## Detailed Findings

### 1. Color Palettes ✅ CONSISTENT

**Status**: All scripts correctly use UCLA brand colors from `palette_utils.py`

**Evidence**:
- All scripts import `DISCIPLINE_BASE_PALETTE` and `DISCIPLINE_REASON_PALETTE`
- Consistent use of:
  - UCLA Blue (#2774AE)
  - UCLA Gold (#FFD100)
  - Darkest Blue (#003B5C)
  - Purple (#8A69D4)
  - Grey, Black, etc.

**Scripts Reviewed**:
- `06_statewide_trends.py`: ✅ Uses `RACE_PALETTE` with UCLA colors
- `20_suspension_reason_trends_ucla.py`: ✅ Uses `DISCIPLINE_REASON_PALETTE`
- `20_suspension_reason_trends_by_level_and_locale.py`: ✅ Uses `DISCIPLINE_REASON_PALETTE`
- `locale_locale_snapshot.py`: ✅ Uses module from `06_statewide_trends.py`

**Recommendation**: ✅ No changes needed

---

### 2. Standard Citation ❌ CRITICAL ISSUE

**Status**: Only `06_statewide_trends.py` uses the standard citation from `palette_utils.py`

**Established Standard** (from `palette_utils.py`):
```python
STANDARD_CITATION = (
    "Source: REACH analysis of 2017-18 through 2023-24 suspension data "
    "from the California Department of Education's California Longitudinal Pupil "
    "Achievement Data System (CALPADS). Analysis includes traditional public schools "
    "aggregated at the school level, with suspension rates calculated as total "
    "suspensions divided by cumulative enrollment."
)
```

**Current Usage**:
- `06_statewide_trends.py`: ✅ Imports and uses `STANDARD_CITATION`
- `20_suspension_reason_trends_ucla.py`: ❌ **MISSING** - No citation visible
- `20_suspension_reason_trends_by_level_and_locale.py`: ❌ **MISSING** - No citation visible
- `locale_locale_snapshot.py`: ✅ Uses citation via `06_statewide_trends.py` module

**Impact**:
- UCLA-branded charts lack proper source attribution
- Inconsistent citation across outputs
- Not publication-ready without citations

**Recommendation**:
1. Import `STANDARD_CITATION` in both UCLA-branded scripts
2. Add citation text at bottom of all figures
3. Use `fig.text()` similar to `06_statewide_trends.py:621`

---

### 3. Title Formatting ⚠️ INCONSISTENT

**Issue**: Different dash types and structural patterns across scripts

**Current Patterns**:

**`06_statewide_trends.py`** (uses en-dash "–"):
```python
"Suspension Rates by Race – Elementary Schools"
"Suspension Rates by Race – City Schools"
"Suspension Rates in Highest-Black vs. Highest-White Enrollment Schools"
```

**`20_suspension_reason_trends_ucla.py`** (uses em-dash "—"):
```python
"Elementary Schools — Suspension Rates by Reason"
"Middle Schools — Suspension Rates by Reason"
```

**`20_suspension_reason_trends_by_level_and_locale.py`** (uses em-dash "—" with newline):
```python
"Elementary Schools — City Locale\nSuspension Rates by Reason"
"All Traditional Schools — Statewide Suspension Rates by Reason"
```

**Typography Note**:
- **En-dash (–)**: Used between numbers/ranges or to show relationship
- **Em-dash (—)**: Used for emphasis or separation of ideas

**Recommendation**:
Standardize on **em-dash (—)** with consistent structure:
```python
# Pattern for race-based charts:
"{Level/Locale} Schools — Suspension Rates by Race"

# Pattern for reason-based charts:
"{Level} Schools — {Locale} Locale — Suspension Rates by Reason"
# OR
"{Level} Schools ({Locale}) — Suspension Rates by Reason"
```

---

### 4. Subtitles ⚠️ MISSING

**Status**: Only `06_statewide_trends.py` includes descriptive subtitles

**Current Usage**:

**`06_statewide_trends.py`** (has subtitles):
```python
subtitle = "By grade span, 2017-18 through 2023-24 (no statewide reporting in 2020-21)."
subtitle = "Traditional schools, 2023-24"
```
Placed via: `fig.text(0.07, 0.933, subtitle, fontsize=13, ha="left", color=TEXT_COLOR)`

**`20_suspension_reason_trends_ucla.py`**: ❌ No subtitle
**`20_suspension_reason_trends_by_level_and_locale.py`**: ❌ No subtitle

**Impact**:
- Missing context about time period and scope
- Readers don't know if charts show traditional schools only
- Less informative than `06_statewide_trends.py` outputs

**Recommendation**:
Add subtitles to all UCLA-branded scripts:
```python
# For level/locale charts:
subtitle = "Traditional schools, 2017-18 through 2023-24 (no statewide reporting in 2020-21)"

# For statewide aggregate:
subtitle = "All traditional public schools, 2017-18 through 2023-24"
```

---

### 5. Data Validation ✅ CONSISTENT

**Status**: All scripts properly use shared validation utilities

**Evidence**:
- All scripts import from `data_validations.py`:
  - `audit_counts_against_enrollment()`
  - `sanitize_rate_column()`
  - `ensure_audit_dir()`

**Usage Examples**:
```python
# 06_statewide_trends.py (line 364-370)
joined = audit_counts_against_enrollment(
    joined,
    count_columns=["total_suspensions"],
    enrollment_column="cumulative_enrollment",
    context="06_statewide_trends.load_joined_data",
    audit_dir=AUDIT_DIR,
)

# 20_suspension_reason_trends_ucla.py (line 111-118)
aggregated = audit_counts_against_enrollment(
    aggregated,
    count_columns=list(REASON_COLUMNS.keys()),
    enrollment_column="cumulative_enrollment",
    context="20_reason_level.aggregated",
    audit_dir=AUDIT_DIR,
)
```

**Recommendation**: ✅ No changes needed

---

### 6. Reason Labels and Colors ✅ CONSISTENT

**Status**: All scripts use identical reason definitions and color mappings

**Shared Definition** (all scripts):
```python
REASON_COLUMNS = {
    "suspension_count_violent_incident_injury": "Violent (Injury)",
    "suspension_count_violent_incident_no_injury": "Violent (No Injury)",
    "suspension_count_weapons_possession": "Weapons",
    "suspension_count_illicit_drug_related": "Illicit Drugs",
    "suspension_count_defiance_only": "Willful Defiance",
    "suspension_count_other_reasons": "Other",
}
```

**Shared Color Palette** (from `palette_utils.py`):
```python
DISCIPLINE_REASON_PALETTE = {
    "Violent (Injury)": UCLA Blue (#2774AE),
    "Violent (No Injury)": Black,
    "Weapons": Grey,
    "Illicit Drugs": Purple (#8A69D4),
    "Willful Defiance": red (with dashed linestyle),
    "Other": UCLA Gold (#FFD100),
}
```

**Special Treatment**: "Willful Defiance" consistently uses dashed linestyle (`linestyle="--"`)

**Recommendation**: ✅ No changes needed

---

### 7. Figure Styling ✅ CONSISTENT

**Status**: All scripts follow consistent styling patterns

**Common Elements**:
- White background: `fig.patch.set_facecolor("white")`, `ax.set_facecolor("white")`
- No spine visibility: `spine.set_visible(False)`
- Grid styling:
  - Y-axis: solid grid with UCLA Lighter Blue
  - X-axis: optional dotted/dashed grid
- Text colors: Darkest Blue for labels, Grey for captions
- Label adjustment: `adjustText` library for avoiding overlaps

**Recommendation**: ✅ No changes needed

---

### 8. Output Format and DPI ⚠️ MINOR INCONSISTENCY

**Status**: Minor variation in default DPI settings

**Current Defaults**:
- `06_statewide_trends.py`: DPI=320 (line 709)
- `20_suspension_reason_trends_ucla.py`: DPI=300 (line 174, 302)
- `20_suspension_reason_trends_by_level_and_locale.py`: DPI=300 (line 327, 593)

**Recommendation**:
Standardize on **DPI=300** for consistency (already the most common setting)

Update `06_statewide_trends.py` line 709:
```python
# BEFORE:
fig.savefig(out_path, dpi=320)

# AFTER:
fig.savefig(out_path, dpi=300)
```

---

### 9. School Level and Locale Categories ✅ CONSISTENT

**Status**: All scripts use identical ordering

**Evidence**:
```python
LEVEL_ORDER = ["Elementary", "Middle", "High"]
LOCALE_ORDER = ["City", "Suburban", "Town", "Rural"]  # with "Unknown" handling
```

**Recommendation**: ✅ No changes needed

---

### 10. Legend Formatting ✅ MOSTLY CONSISTENT

**Status**: All scripts use similar legend positioning and styling

**Common Pattern**:
```python
legend = ax.legend(
    loc="upper center",
    bbox_to_anchor=(0.5, -0.2),  # Below plot
    ncol=3,  # or 4 for race charts
    frameon=False,
    labelcolor=TEXT_COLOR,
)
```

**Recommendation**: ✅ No changes needed

---

## Priority Recommendations

### Priority 1: CRITICAL (Must Fix)

#### 1.1 Add Standard Citation to UCLA-Branded Scripts

**Files to Update**:
- `20_suspension_reason_trends_ucla.py`
- `20_suspension_reason_trends_by_level_and_locale.py`

**Changes Required**:

**Step 1**: Import `STANDARD_CITATION`
```python
# Add to imports section (around line 33-34)
from palette_utils import DISCIPLINE_BASE_PALETTE, DISCIPLINE_REASON_PALETTE, STANDARD_CITATION
```

**Step 2**: Add caption to figure
```python
# Add to plot_level() or plot_level_locale() function, before plt.tight_layout()
# For 20_suspension_reason_trends_ucla.py (around line 251):
fig.text(
    0.07, 0.02,  # x, y position (bottom left)
    STANDARD_CITATION,
    fontsize=9,
    ha="left",
    color=DISCIPLINE_BASE_PALETTE["Grey"],
    wrap=True,
)

# Adjust figure layout to make room for caption:
# BEFORE:
plt.tight_layout()

# AFTER:
fig.subplots_adjust(left=0.07, right=0.98, top=0.95, bottom=0.14)
```

**Step 3**: Repeat for statewide plot in `20_suspension_reason_trends_by_level_and_locale.py`

---

### Priority 2: IMPORTANT (Should Fix)

#### 2.1 Standardize Title Formatting

**Proposed Standard**: Use **em-dash (—)** consistently

**Files to Update**:
- `06_statewide_trends.py`
- `20_suspension_reason_trends_ucla.py`
- `20_suspension_reason_trends_by_level_and_locale.py`

**Changes**:

**`06_statewide_trends.py`** (replace "–" with "—"):
```python
# Line 698: BEFORE
title=f"Suspension Rates by Race – {level} Schools",

# Line 698: AFTER
title=f"Suspension Rates by Race — {level} Schools",
```

Apply to all title strings in the file.

**`20_suspension_reason_trends_by_level_and_locale.py`** (simplify multiline titles):
```python
# Line 379: BEFORE
ax.set_title(
    f"{level} Schools — {locale} Locale\nSuspension Rates by Reason",
    ...
)

# Line 379: AFTER (single line, clearer)
ax.set_title(
    f"{level} Schools ({locale}) — Suspension Rates by Reason",
    ...
)
```

#### 2.2 Add Subtitles to UCLA-Branded Scripts

**Files to Update**:
- `20_suspension_reason_trends_ucla.py`
- `20_suspension_reason_trends_by_level_and_locale.py`

**Changes**:

Add subtitle text below title, similar to `06_statewide_trends.py`:

```python
# Add after ax.set_title() calls
subtitle = "Traditional schools, 2017-18 through 2023-24 (no statewide reporting in 2020-21)"
fig.text(
    0.07, 0.92,  # Below title
    subtitle,
    fontsize=11,
    ha="left",
    color=TEXT_COLOR,
)
```

---

### Priority 3: MINOR (Nice to Have)

#### 3.1 Standardize DPI to 300

**File to Update**: `06_statewide_trends.py`

**Changes**:
```python
# Line 709, 793, 984, 1104, 1208: Change dpi=320 to dpi=300
fig.savefig(out_path, dpi=300)
```

---

## Implementation Plan

### Phase 1: Critical Fixes (Do First)
1. Add `STANDARD_CITATION` to `20_suspension_reason_trends_ucla.py`
2. Add `STANDARD_CITATION` to `20_suspension_reason_trends_by_level_and_locale.py`
3. Test both scripts to ensure citations display correctly

### Phase 2: Important Improvements
1. Standardize title formatting to use em-dash (—) across all scripts
2. Add subtitles to UCLA-branded scripts
3. Test all scripts for visual consistency

### Phase 3: Minor Refinements
1. Update DPI to 300 in `06_statewide_trends.py`
2. Final visual inspection of all outputs

---

## Testing Checklist

After making changes, verify:

- [ ] All scripts run without errors
- [ ] Citations appear at bottom of all charts
- [ ] Citations are readable (not cut off)
- [ ] Titles use consistent dash type (—)
- [ ] Subtitles provide clear context
- [ ] Color palettes remain consistent
- [ ] Data validation still functions
- [ ] Output files generated successfully
- [ ] Figure layouts accommodate new elements (citation, subtitle)

---

## Files for Reference

### Key Documentation
- `docs/protocols/UCLA-Brand-Colors.md` - UCLA brand guidelines
- `docs/protocols/CITATION_STANDARD.md` - Citation requirements
- `graph_scripts/README.md` - Graph scripts documentation
- `graph_scripts/palette_utils.py` - Color palette and citation definitions

### Scripts Reviewed
1. `graph_scripts/06_statewide_trends.py` - Statewide trends (race-based)
2. `graph_scripts/20_suspension_reason_trends_ucla.py` - UCLA-branded reason trends by level
3. `graph_scripts/20_suspension_reason_trends_by_level_and_locale.py` - Reason trends by level and locale
4. `graph_scripts/locale_locale_snapshot.py` - Locale snapshot chart
5. `graph_scripts/palette_utils.py` - Shared color palettes
6. `graph_scripts/data_validations.py` - Shared validation utilities

---

## Conclusion

The graph scripts demonstrate **strong technical consistency** in color palettes, data validation, and styling. The primary gaps are in **professional presentation elements** (citations, subtitles) that are critical for publication-ready outputs.

**Recommended Action**: Implement Phase 1 (Critical Fixes) immediately to ensure all charts have proper source attribution. Phase 2 improvements will enhance professionalism and consistency.

---

**Report Generated**: 2025-11-26
**Next Review**: After implementing Priority 1 and 2 recommendations
