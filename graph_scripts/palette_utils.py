"""Shared UCLA-aligned color palettes for discipline graphics."""

# Standardized citation text for all outputs
# Use this consistent citation across all graphs and reports
STANDARD_CITATION = (
    "Source: REACH analysis of 2017-18 through 2023-24 suspension data "
    "from the California Department of Education's California Longitudinal Pupil "
    "Achievement Data System (CALPADS). Analysis includes traditional public schools "
    "aggregated at the school level, with suspension rates calculated as total "
    "suspensions divided by cumulative enrollment."
)

DISCIPLINE_BASE_PALETTE = {
    "Darkest Blue": "#003B5C",
    "Darker Blue": "#005587",
    "UCLA Blue": "#2774AE",
    "Lighter Blue": "#8BB8E8",
    "UCLA Gold": "#FFD100",
    "Darker Gold": "#FFC72C",
    "Darkest Gold": "#FFB81C",
    "Purple": "#8A69D4",
    "Grey": "Grey",
    "Black": "black",
}

# Prefer distinctive non-UCLA accent colors before reusing darker/lighter variants.
DISCIPLINE_COLOR_PRIORITY = [
    "UCLA Blue",
    "UCLA Gold",
    "Grey",
    "Black",
    "Purple",
    "Darker Blue",
    "Darkest Blue",
    "Lighter Blue",
    "Darker Gold",
    "Darkest Gold",
]

DISCIPLINE_REASON_PALETTE = {
    "Violent (Injury)": DISCIPLINE_BASE_PALETTE["UCLA Blue"],
    # Keep "Violent (No Injury)" visually distinct from the red Willful Defiance line
    # by assigning it the cyan accent before reusing any darker UCLA shades.
    "Violent (No Injury)": DISCIPLINE_BASE_PALETTE["Black"],
    "Weapons": DISCIPLINE_BASE_PALETTE["Grey"],
    "Illicit Drugs": DISCIPLINE_BASE_PALETTE["Purple"],
    "Willful Defiance": "red",
    "Other": DISCIPLINE_BASE_PALETTE["UCLA Gold"],
}
