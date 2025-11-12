import sqlite3
import pandas as pd

# Connect to database (it will be created if it doesn’t exist)
conn = sqlite3.connect("../db/footbase.db")

# Create a cursor to run SQL commands
cursor = conn.cursor()

# --- create tables ---
cursor.executescript("""
CREATE TABLE IF NOT EXISTS competitions (
    competition_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    country TEXT,
    level INTEGER,
    start_year INTEGER,
    end_year INTEGER
);

CREATE TABLE IF NOT EXISTS teams (
    team_id INTEGER PRIMARY KEY AUTOINCREMENT,
    team_name TEXT UNIQUE NOT NULL,
    competition_id INTEGER,
    FOREIGN KEY (competition_id) REFERENCES competitions (competition_id)
);

CREATE TABLE IF NOT EXISTS matches (
    match_id INTEGER PRIMARY KEY AUTOINCREMENT,
    competition_id INTEGER,
    season TEXT,
    date TEXT,
    home_team_id INTEGER,
    away_team_id INTEGER,
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
    FOREIGN KEY (competition_id) REFERENCES competitions (competition_id),
    FOREIGN KEY (home_team_id) REFERENCES teams (team_id),
    FOREIGN KEY (away_team_id) REFERENCES teams (team_id)
);
""")

conn.commit()
print("✅ Tables created successfully!")

#==VALIDATION==(UNCOMMENT CODE BELOW IF NEEDED)
#cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
#print(cursor.fetchall())

cursor.execute("PRAGMA table_info(competitions);")
for col in cursor.fetchall():
    print(col)