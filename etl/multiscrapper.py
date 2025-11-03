import pandas as pd
import os

BASE_URL = "https://www.football-data.co.uk/mmz4281"
LEAGUES = {
    "D1": "Bundesliga",
    "I1": "Serie A",
    "F1": "Ligue 1",
    "SP1": "La Liga",
    "P1" : "Primeira Liga"
}

SEASONS = ["1718", "1819", "1920", "2021", "2122", "2223", "2324", "2425"]

CSV_DIR = "../data/raw"
os.makedirs(CSV_DIR, exist_ok=True)

for code, league_name in LEAGUES.items():
    for season in SEASONS:
        url = f"{BASE_URL}/{season}/{code}.csv"
        print(f"📥 Downloading {league_name} {season} from {url}")
        try:
            df = pd.read_csv(url)
            if df.empty:
                print(f"⚠️ Skipped {league_name} {season} (empty file)")
                continue
        except Exception as e:
            print(f"❌ Error loading {league_name} {season}: {e}")
            continue

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
        df = df[[col for col in keep_cols if col in df.columns]]
        df['league'] = league_name
        df['season'] = f"20{season[:2]}/{season[2:]}"

        # Save file
        fname = f"{league_name.replace(' ', '_')}_{season}.csv"
        fpath = os.path.join(CSV_DIR, fname)
        df.to_csv(fpath, index=False)
        print(f"💾 Saved {fpath} ({len(df)} rows)")