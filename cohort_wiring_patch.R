# =============================================================================
# WIRING PATCHES — Cohort Analysis Module
# Two surgical additions to your existing ui.R and server.R.
# No other lines change.
# =============================================================================


# ─────────────────────────────────────────────────────────────────────────────
# PATCH 1 OF 2 — ui.R
# ─────────────────────────────────────────────────────────────────────────────
#
# Location A: Inside sidebarMenu(), after the "Data Management" menuItem.
# Add this block:

menuItem("Cohort Win Rate", tabName = "cohort", icon = icon("layer-group")),

#
# Location B: Inside tabItems(), after the closing brace of the "admin" tabItem.
# Add this block:

# ── COHORT WIN RATE ──────────────────────────────────────────────────────
tabItem(tabName = "cohort",
        cohortAnalysisUI("cohort")
)

#
# No other changes to ui.R are needed. The module's pickerInput uses
# shinyWidgets which is already loaded in global.R.


# ─────────────────────────────────────────────────────────────────────────────
# PATCH 2 OF 2 — server.R
# ─────────────────────────────────────────────────────────────────────────────
#
# Location A: At the very top of server.R, alongside any other source() calls.
# Add:

source("mod_cohort_analysis.R")

#
# Location B: Inside the server function, after the last output/observer block
# (e.g., after the download_report downloadHandler). Add:

# ── COHORT WIN RATE MODULE ────────────────────────────────────────────────
# Pass get_data (the unfiltered pool reactive) so the module has full
# history for attribution windows that extend past the sidebar date range.
cohortAnalysisServer("cohort", get_data = get_data)

#
# That's it. The module manages its own estimator picker, date range,
# forward window, and all outputs internally.
#
# NOTE: `get_data` in server.R is a reactive expression (not a reactive value),
# so pass it without parentheses: get_data = get_data  ✓
#                                            get_data = get_data()  ✗


# ─────────────────────────────────────────────────────────────────────────────
# DEPENDENCY CHECK
# All packages used by the module are already present in global.R:
#   purrr   → loaded via tidyverse
#   tidyr   → loaded via tidyverse
#   scales  → already library(scales)
#   DT      → already library(DT)
#   plotly  → already library(plotly)
#   shinyWidgets → already library(shinyWidgets)
# No new library() calls needed.
# =============================================================================