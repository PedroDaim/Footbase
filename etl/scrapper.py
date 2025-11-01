import pandas as pd
from datetime import datetime
import os

# === CONFIG ===
DATA_URL = "https://www.football-data.co.uk/mmz4281/1718/E0.csv"
LEAGUE_NAME = "Premier League"
SEASON = "2017/18"
CSV_DIR = "../data/raw"
os.makedirs(CSV_DIR, exist_ok=True)  # Ensure folder exists

# === 1. EXTRACT ===
print("Downloading data...")
df = pd.read_csv(DATA_URL)

# === 2. TRANSFORM ===
print("Cleaning data...")
df = df.rename(columns={
    'Date': 'date',
    'HomeTeam': 'home_team',
    'AwayTeam': 'away_team',
    'FTHG': 'home_goals',
    'FTAG': 'away_goals',
    'FTR': 'result',
    'HS': 'home_shots',
    'AS': 'away_shots',
    'HST': 'home_shots_on_target',
    'AST': 'away_shots_on_target',
    'B365H': 'odds_home',
    'B365D': 'odds_draw',
    'B365A': 'odds_away'
})

keep_cols = [
    'date', 'home_team', 'away_team', 'home_goals', 'away_goals', 'result',
    'home_shots', 'away_shots', 'home_shots_on_target', 'away_shots_on_target',
    'odds_home', 'odds_draw', 'odds_away'
]
df = df[keep_cols]

# Add metadata
df['league'] = LEAGUE_NAME
df['season'] = SEASON

# Convert date
try:
    df['date'] = pd.to_datetime(df['date'], dayfirst=True)
except Exception:
    df['date'] = pd.to_datetime(df['date'], errors='coerce')

# === 3. SAVE RAW CSV ===
# Create dynamic filename
season_fmt = SEASON.replace("/", "-")
league_fmt = LEAGUE_NAME.replace(" ", "_")
csv_filename = os.path.join(CSV_DIR, f"{league_fmt}_{season_fmt}.csv")

df.to_csv(csv_filename, index=False)
print(f"Saved raw matches to {csv_filename}")