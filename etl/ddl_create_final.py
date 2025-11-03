import pandas as pd
import sqlite3
import glob
import os


DB_PATH = "../db/footbase_big5.db"
CSV_DIR = "../data/raw"
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Drop existing table (if you’re reloading from scratch)
cursor.execute("DROP TABLE IF EXISTS matches;")

# Create with full column list
cursor.execute("""
CREATE TABLE matches (
    date TEXT,
    home_team TEXT,
    away_team TEXT,
    home_goals INTEGER,
    away_goals INTEGER,
    result TEXT,
    home_shots INTEGER,
    away_shots INTEGER,
    home_shots_on_target INTEGER,
    away_shots_on_target INTEGER,
    odds_home REAL,
    odds_draw REAL,
    odds_away REAL,
    league TEXT,
    season TEXT
);
""")

conn.commit()
conn.close()
print("✅ Table recreated with full schema!")

#====LOAD SCRIPT====

conn = sqlite3.connect(DB_PATH)

for league in ["Premier_League", "La_Liga", "Bundesliga", "Serie_A", "Ligue_1"]:
    files = glob.glob(os.path.join(CSV_DIR, league, "*.csv"))
    for file in files:
        df = pd.read_csv(file)
        df["league"] = league.replace("_", " ")
        df.to_sql("matches", conn, if_exists="append", index=False)
        print(f"✅ Loaded {file}")

conn.close()