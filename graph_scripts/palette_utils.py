"""Shared UCLA-aligned color palettes for discipline graphics."""

DISCIPLINE_BASE_PALETTE = {
    "Darkest Blue": "#003B5C",
    "Darker Blue": "#005587",
    "UCLA Blue": "#2774AE",
    "Lighter Blue": "#8BB8E8",
    "UCLA Gold": "#FFD100",
    "Darker Gold": "#FFC72C",
    "Darkest Gold": "#FFB81C",
    "Purple": "#8A69D4",
    "Magenta": "#FF00A5",
    "Cyan": "#00FFFF",
}

# Prefer distinctive non-UCLA accent colors before reusing darker/lighter variants.
DISCIPLINE_COLOR_PRIORITY = [
    "UCLA Blue",
    "UCLA Gold",
    "Magenta",
    "Cyan",
    "Purple",
    "Darker Blue",
    "Darkest Blue",
    "Lighter Blue",
    "Darker Gold",
    "Darkest Gold",
]

DISCIPLINE_REASON_PALETTE = {
    "Violent (Injury)": DISCIPLINE_BASE_PALETTE["UCLA Blue"],
    "Violent (No Injury)": DISCIPLINE_BASE_PALETTE["Magenta"],
    "Weapons": DISCIPLINE_BASE_PALETTE["Cyan"],
    "Illicit Drugs": DISCIPLINE_BASE_PALETTE["Purple"],
    "Willful Defiance": "red",
    "Other": DISCIPLINE_BASE_PALETTE["UCLA Gold"],
}
