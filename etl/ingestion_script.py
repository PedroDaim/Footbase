"""
Football Data Warehouse - Fixed CSV Ingestion Script
=====================================================
Loads Football-Data.co.uk CSVs into SQLite with proper date parsing.
Fixes the DD/MM/YYYY date format issue.
"""

import pandas as pd
import sqlite3
import glob
import os
from datetime import datetime

print("=" * 60)
print("Football Data CSV Ingestion (Fixed)")
print("=" * 60)

# Configuration
DB_PATH = "../db/footbase_big5.db"
CSV_DIR = "../data/raw"

# ============================================================================
# 1. BACKUP EXISTING DATABASE
# ============================================================================
print(f"\n[1/5] Checking for existing database...")

if os.path.exists(DB_PATH):
    backup_path = DB_PATH.replace('.db', f'_backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.db')
    print(f"  ✓ Creating backup: {os.path.basename(backup_path)}")
    import shutil
    shutil.copy(DB_PATH, backup_path)
else:
    print(f"  ℹ No existing database found")

# ============================================================================
# 2. CREATE DATABASE & SCHEMA
# ============================================================================
print(f"\n[2/5] Creating database schema...")

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Drop existing table (reloading from scratch)
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
print("  ✓ Table 'matches' created successfully")

# ============================================================================
# 3. LOAD CSVs BY LEAGUE
# ============================================================================
print(f"\n[3/5] Loading CSV files...")

leagues = ["Premier_League", "La_Liga", "Bundesliga", "Serie_A", "Ligue_1"]
total_loaded = 0
league_stats = {}

for league in leagues:
    league_dir = os.path.join(CSV_DIR, league)
    files = glob.glob(os.path.join(league_dir, "*.csv"))
    
    if not files:
        print(f"  ⚠ No files found for {league} in {league_dir}")
        continue
    
    league_matches = 0
    league_name = league.replace("_", " ")
    
    for file in files:
        try:
            # Read CSV
            df = pd.read_csv(file)
            
            # CRITICAL: Parse dates with DD/MM/YYYY format
            df['date'] = pd.to_datetime(df['date'], format='%d/%m/%Y', errors='coerce')
            
            # Convert back to string for SQLite (YYYY-MM-DD format)
            df['date'] = df['date'].dt.strftime('%Y-%m-%d')
            
            # Add league name
            df["league"] = league_name
            
            # Append to database
            df.to_sql("matches", conn, if_exists="append", index=False)
            
            matches_count = len(df)
            league_matches += matches_count
            total_loaded += matches_count
            
            print(f"  ✓ {os.path.basename(file):40s} → {matches_count:>4} matches")
            
        except Exception as e:
            print(f"  ✗ Error loading {os.path.basename(file)}: {e}")
    
    league_stats[league_name] = league_matches

conn.close()

# ============================================================================
# 4. VALIDATE DATA
# ============================================================================
print(f"\n[4/5] Validating database...")

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Check total records
cursor.execute("SELECT COUNT(*) FROM matches")
total_count = cursor.fetchone()[0]

# Check valid dates
cursor.execute("SELECT COUNT(*) FROM matches WHERE date IS NOT NULL AND date != 'NaT'")
valid_dates = cursor.fetchone()[0]

# Check NULL dates
cursor.execute("SELECT COUNT(*) FROM matches WHERE date IS NULL OR date = 'NaT'")
null_dates = cursor.fetchone()[0]

print(f"  Total matches: {total_count:,}")
print(f"  Valid dates: {valid_dates:,} ({100*valid_dates/total_count:.1f}%)")
print(f"  Invalid dates: {null_dates:,} ({100*null_dates/total_count:.1f}%)")

# Show date range
cursor.execute("SELECT MIN(date), MAX(date) FROM matches WHERE date IS NOT NULL AND date != 'NaT'")
date_range = cursor.fetchone()
if date_range[0]:
    print(f"  Date range: {date_range[0]} to {date_range[1]}")

# Matches by league
print("\n  Matches by league:")
for league, count in league_stats.items():
    print(f"    {league:20s}: {count:>5,} matches")

# Matches by season
cursor.execute("SELECT season, COUNT(*) FROM matches GROUP BY season ORDER BY season")
seasons = cursor.fetchall()
print("\n  Matches by season:")
for season, count in seasons:
    print(f"    {season}: {count:>5,} matches")

conn.close()

# ============================================================================
# 5. SUMMARY
# ============================================================================
print("\n" + "=" * 60)
print("✅ INGESTION COMPLETE!")
print("=" * 60)
print(f"Database: {DB_PATH}")
print(f"Total matches: {total_count:,}")
print(f"Valid dates: {valid_dates:,} ({100*valid_dates/total_count:.1f}%)")
print(f"Leagues loaded: {len(league_stats)}")
print(f"Seasons: {len(seasons)}")

if null_dates > 0:
    print(f"\n⚠ Warning: {null_dates:,} matches have invalid dates")
    print("  Check the source CSV files for date formatting issues")


print("=" * 60)