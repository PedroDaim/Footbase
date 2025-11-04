"""
Football Data Warehouse - Merge xG Data from Understat
========================================================
This script merges Football-Data.co.uk match data with Understat xG data,
standardizing team names and updating the SQLite database.
"""

import pandas as pd
import sqlite3
from datetime import datetime

print("=" * 60)
print("Football Data + Understat xG Merge Script")
print("=" * 60)

# ============================================================================
# 1. LOAD DATA
# ============================================================================
print("\n[1/5] Loading data...")

# Load Football-Data from SQLite
conn = sqlite3.connect('../db/footbase_big5.db')
matches_fd = pd.read_sql_query("SELECT * FROM matches", conn)
print(f"  ✓ Loaded {len(matches_fd):,} matches from Football-Data")

# Load Understat xG data
understat = pd.read_csv('../data/raw/understat_xg_data3.csv')
understat['date'] = pd.to_datetime(understat['date'])
matches_fd['date'] = pd.to_datetime(matches_fd['date'])
print(f"  ✓ Loaded {len(understat):,} matches from Understat")

# ============================================================================
# 2. STANDARDIZE TEAM NAMES
# ============================================================================
print("\n[2/5] Standardizing team names...")

# Mapping: Football-Data abbreviated names → Understat proper names
team_name_map = {
    # Premier League
    'Man City': 'Manchester City',
    'Man United': 'Manchester United',
    'Newcastle': 'Newcastle United',
    "Nott'm Forest": 'Nottingham Forest',
    'West Brom': 'West Bromwich Albion',
    'Wolves': 'Wolverhampton Wanderers',
    
    # La Liga
    'Ath Bilbao': 'Athletic Club',
    'Ath Madrid': 'Atletico Madrid',
    'Celta': 'Celta Vigo',
    'La Coruna': 'Deportivo La Coruna',
    'Espanol': 'Espanyol',
    'Vallecano': 'Rayo Vallecano',
    'Betis': 'Real Betis',
    'Sociedad': 'Real Sociedad',
    'Valladolid': 'Real Valladolid',
    'Huesca': 'SD Huesca',
    
    # Bundesliga
    'Bielefeld': 'Arminia Bielefeld',
    'Leverkusen': 'Bayer Leverkusen',
    'Dortmund': 'Borussia Dortmund',
    "M'gladbach": 'Borussia M.Gladbach',
    'Ein Frankfurt': 'Eintracht Frankfurt',
    'FC Koln': 'FC Cologne',
    'Heidenheim': 'FC Heidenheim',
    'Fortuna Dusseldorf': 'Fortuna Duesseldorf',
    'Greuther Furth': 'Greuther Fuerth',
    'Hamburg': 'Hamburger SV',
    'Hannover': 'Hannover 96',
    'Hertha': 'Hertha Berlin',
    'Mainz': 'Mainz 05',
    'Nurnberg': 'Nuernberg',
    'RB Leipzig': 'RasenBallsport Leipzig',
    'St Pauli': 'St. Pauli',
    'Stuttgart': 'VfB Stuttgart',
    
    # Serie A
    'Milan': 'AC Milan',
    'Parma': 'Parma Calcio 1913',
    'Spal': 'SPAL 2013',
    
    # Ligue 1
    'Clermont': 'Clermont Foot',
    'Paris SG': 'Paris Saint Germain',
    'St Etienne': 'Saint-Etienne',
}

# Apply standardization to Football-Data
matches_fd['home_team'] = matches_fd['home_team'].replace(team_name_map)
matches_fd['away_team'] = matches_fd['away_team'].replace(team_name_map)
print(f"  ✓ Standardized {len(team_name_map)} team names to proper format")

# ============================================================================
# 3. MERGE DATASETS
# ============================================================================
print("\n[3/5] Merging datasets...")

# Merge on date + home_team + away_team
merged = matches_fd.merge(
    understat[['date', 'home_team', 'away_team', 'home_xg', 'away_xg', 
               'home_goals_us', 'away_goals_us']],
    on=['date', 'home_team', 'away_team'],
    how='left',
    indicator=True
)

# Calculate merge statistics
total_matches = len(merged)
matched = (merged['_merge'] == 'both').sum()
unmatched = (merged['_merge'] == 'left_only').sum()

print(f"  ✓ Total matches: {total_matches:,}")
print(f"  ✓ Matched with xG: {matched:,} ({100*matched/total_matches:.1f}%)")
print(f"  ✓ Without xG: {unmatched:,} ({100*unmatched/total_matches:.1f}%)")

# ============================================================================
# 4. VALIDATE MERGE
# ============================================================================
print("\n[4/5] Validating merge quality...")

# Check goal consistency (where both datasets have data)
has_both = merged['_merge'] == 'both'
goal_mismatch = (
    (merged.loc[has_both, 'home_goals'] != merged.loc[has_both, 'home_goals_us']) |
    (merged.loc[has_both, 'away_goals'] != merged.loc[has_both, 'away_goals_us'])
)

if goal_mismatch.sum() > 0:
    print(f"  ⚠ Warning: {goal_mismatch.sum()} matches have goal mismatches")
    print(f"    (This might indicate date/team matching issues)")
else:
    print(f"  ✓ All matched goals are consistent!")

# Show xG coverage by league
print("\n  xG coverage by league:")
coverage = merged.groupby('league', as_index=False).apply(
    lambda x: pd.Series({
        'matched': x['home_xg'].notna().sum(),
        'total': len(x),
        'percentage': 100 * x['home_xg'].notna().sum() / len(x)
    }), include_groups=False
)
for _, row in coverage.iterrows():
    print(f"    {row['league']:20s}: {int(row['matched']):>4} / {int(row['total']):<4} ({row['percentage']:>5.1f}%)")

# ============================================================================
# 5. SAVE TO DATABASE
# ============================================================================
print("\n[5/5] Saving to database...")

# Drop helper columns before saving
merged_clean = merged.drop(columns=['_merge', 'home_goals_us', 'away_goals_us'])

# Backup existing table (optional but recommended)
backup_name = f"matches_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
cursor = conn.cursor()
cursor.execute(f"CREATE TABLE {backup_name} AS SELECT * FROM matches")
conn.commit()
print(f"  ✓ Backup created: {backup_name}")

# Save merged data
merged_clean.to_sql('matches', conn, if_exists='replace', index=False)
print(f"  ✓ Updated 'matches' table with xG data")

# Close connection
conn.close()

# ============================================================================
# SUMMARY
# ============================================================================
print("\n" + "=" * 60)
print("✅ MERGE COMPLETE!")
print("=" * 60)
print(f"Total matches in database: {len(merged_clean):,}")
print(f"Matches with xG data: {matched:,} ({100*matched/total_matches:.1f}%)")
print(f"Team names standardized: {len(team_name_map)}")
print("\nNext steps:")
print("  • Check the database: SELECT * FROM matches LIMIT 10;")
print("  • Verify xG data: SELECT league, AVG(home_xg) FROM matches GROUP BY league;")
print("  • If needed, restore backup table")
print("=" * 60)