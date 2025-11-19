# Protocol: Script Request Reminder and CLAUDE Gate

**Last Updated**: 2025-01-05

## Purpose
Create an explicit trigger so that every time a user asks for a new or updated script/process, the response automatically references **CLAUDE.md** (root-level) for required conventions, paths, and workflows.

## Scope
Use this protocol whenever drafting or updating any script/process (R, Python, HTML, or documentation-driven workflows).

## Trigger Checklist (apply before writing)
1. **Open CLAUDE.md first**: Review relevant sections (path sourcing, output directories, canonical definitions).
2. **Confirm stage + sourcing**: Identify the target stage and note `source("R/00_paths.R")` and other required utilities.
3. **Identify outputs**: Determine which `outputs/` subdirectory the work will write to.
4. **Note dependencies**: Reuse canonical helpers (e.g., `utils_keys_filters.R`) and existing color palettes/labels.

## Response Reminder (use in replies)
When a user requests a new/updated script, include a short reminder in the reply:
- Acknowledge the request.
- State that you will follow `CLAUDE.md` and `R/00_paths.R` conventions.
- Mention where outputs will go (e.g., `outputs/graphs/` or `outputs/tables/`).

**Example wording**:
> "I’ll follow the CLAUDE.md conventions (starting with `R/00_paths.R`, using existing keys/colors) and place outputs in the appropriate `outputs/` subdirectory."

## Application Notes
- Applies to scripts in `R/`, `Analysis/`, `graph_scripts/`, `scripts/`, and any new utilities.
- If creating documentation to accompany a script, update `docs/README.md` and link back to this protocol.
- For dashboards or HTML changes, still cite CLAUDE.md for data sources, color palettes, and citation standards.
