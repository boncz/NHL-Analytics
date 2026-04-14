"""
config.py — Central configuration for the NHL Analytics project.
Update DB_PATH to match your local environment.
"""

from pathlib import Path

# --- Project Root ---
ROOT_DIR = Path(__file__).parent

# --- Data ---
DATA_DIR = ROOT_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
DB_PATH = DATA_DIR / "NHL_data.db"

# --- Models ---
MODELS_DIR = ROOT_DIR / "models" / "saved"

# --- Dashboard ---
DASHBOARD_DIR = ROOT_DIR / "dashboard"

# --- NHL Constants ---
TEAM_ID_PENGUINS = 5

ALL_SEASONS = [
    20102011, 20112012, 20122013, 20132014,
    20142015, 20152016, 20162017, 20172018
]

PLAYOFF_TYPE = "P"
REGULAR_TYPE = "R"

# --- Plot Styling ---
PLOT_THEME = "plotly_dark"   # swap to "plotly_white" for light mode
PRIMARY_COLOR = "#FCB514"    # Penguins gold — used for highlights
SECONDARY_COLOR = "#000000"  # Penguins black