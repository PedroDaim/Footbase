# ⚽ Footbase

**A modern, analytics-ready data warehouse for football (soccer) match data built with dbt + SQLite**

[![dbt](https://img.shields.io/badge/dbt-1.10-orange.svg)](https://www.getdbt.com/)
[![SQLite](https://img.shields.io/badge/SQLite-3.x-blue.svg)](https://www.sqlite.org/)
[![Python](https://img.shields.io/badge/Python-3.8+-green.svg)](https://www.python.org/)

---

## 📋 Table of Contents

- [Overview](#-overview)
- [Features](#-features)
- [Architecture](#-architecture)
- [Quick Start](#-quick-start)
- [Data Models](#-data-models)
- [Sample Queries](#-sample-queries)
- [Project Structure](#-project-structure)
- [Design Decisions](#-design-decisions)
- [Roadmap](#-roadmap)
- [Contributing](#-contributing)

---

## 🎯 Overview

Footbase is a **production-grade data warehouse** designed for football analytics. It transforms raw match data from multiple leagues into clean, normalized tables following star schema best practices.

**Perfect for:**
- 📊 Football analysts and data scientists
- 🎓 Students learning data engineering
- 🤖 Building ML models for match prediction
- 📈 Creating football analytics dashboards
- 📝 Portfolio projects

**Data Coverage:**
- **Leagues:** Premier League, La Liga, Bundesliga, Serie A, Ligue 1
- **Seasons:** 2017/18 - 2023/24 (7 seasons)
- **Matches:** ~15,000 matches
- **Metrics:** Goals, xG, shots, betting odds, and more

---

## ✨ Features

### 🏗️ Modern Data Architecture
- **dbt-powered transformations** - Version-controlled SQL with testing
- **Star schema design** - Optimized for analytical queries
- **Natural keys** - Human-readable, deterministic identifiers
- **Full test coverage** - Data quality validation built-in

### 📊 Rich Analytics
- Match-level facts with xG data (from Understat)
- Team season aggregates with 30+ metrics
- Home/away performance splits
- xG overperformance tracking
- Shot accuracy and conversion rates

### 🔄 Reproducible & Extensible
- Idempotent pipelines (run multiple times safely)
- Easy to add new leagues/competitions
- Documented models and design decisions
- SQLite for local development, ready for production DBs

---

## 🏛️ Architecture

Footbase follows a **three-layer data warehouse** pattern:

```
┌─────────────────────────────────────────────┐
│  RAW LAYER (SQLite tables)                  │
│  • Source of truth                          │
│  • No transformations                       │
└─────────────────┬───────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────┐
│  STAGING LAYER (stg_*)                      │
│  • Type casting & standardization           │
│  • Stable match_id generation               │
│  • Matchday calculation                     │
└─────────────────┬───────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────┐
│  MARTS LAYER (dim_*, fct_*)                 │
│                                             │
│  Dimensions:        Facts:                  │
│  • dim_teams        • fct_matches           │
│  • dim_competitions • fct_team_season_stats │
│                                             │
│  ⭐ Star Schema - Ready for BI Tools        │
└─────────────────────────────────────────────┘
```

### Star Schema Overview

```
    dim_competitions
            │
            ├──────────┐
            │          │
    dim_teams ──→ fct_matches ──→ fct_team_season_stats
            │          │
            └──────────┘
```

---

## 🚀 Quick Start

### Prerequisites

- Python 3.8+
- dbt-core 1.10+
- dbt-sqlite adapter

### Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/footbase.git
cd footbase

# Install dependencies
pip install dbt-core dbt-sqlite

# Initialize dbt (if needed)
dbt debug

# Build the entire warehouse
dbt build
```

### Verify Installation

```bash
# Check that all models and tests pass
dbt build

# Expected output:
# Completed successfully
# Done. PASS=32 WARN=0 ERROR=0 SKIP=0 TOTAL=32
```

### Explore the Data

```bash
# Generate and serve documentation
dbt docs generate
dbt docs serve

# View lineage graph and model documentation at:
# http://localhost:8080
```

---

## 📊 Data Models

### Staging Layer

#### `stg_matches`
Clean, standardized match data with stable identifiers.

**Key Features:**
- Deterministic `match_id` generation
- Matchday calculation (league-specific)
- Handles missing dates in 2017/18 season

**Grain:** One row per match

### Marts Layer

#### Dimensions

##### `dim_teams`
All unique teams across leagues and seasons.

**Columns:**
- `team_id` (PK) - Natural key: `bayern_munich`, `manchester_city`
- `team_name` - Display name
- `league` - Primary league
- `seasons_played` - Total seasons in dataset

**Grain:** One row per team

##### `dim_competitions`
League/competition metadata.

**Columns:**
- `competition_id` (PK) - Natural key: `premier_league`, `bundesliga`
- `competition_name` - Display name
- `country` - Country

**Grain:** One row per competition

#### Facts

##### `fct_matches`
Match-level measurements and foreign keys.

**Key Metrics:**
- Goals (home/away)
- Expected goals (xG)
- Shots and shots on target
- Betting odds

**Grain:** One row per match (~15,000 rows)

##### `fct_team_season_stats`
Aggregated team performance by season.

**Key Metrics:**
- Record: W-D-L, points
- Goals: For/against, averages
- xG: For/against, overperformance
- Shots: Accuracy, conversion rate
- Home advantage: PPG differential

**Grain:** One row per team per season per competition (~500 rows)

---

## 💡 Sample Queries

### League Table for 2023/24 Season

```sql
SELECT 
    t.team_name,
    s.matches_played,
    s.wins,
    s.draws,
    s.losses,
    s.total_points,
    s.goal_difference
FROM fct_team_season_stats s
JOIN dim_teams t ON s.team_id = t.team_id
JOIN dim_competitions c ON s.competition_id = c.competition_id
WHERE s.season = '2023/24' 
  AND c.competition_name = 'Premier League'
ORDER BY s.total_points DESC, s.goal_difference DESC;
```

### xG Overperformers (Lucky or Clinical?)

```sql
SELECT 
    t.team_name,
    c.competition_name,
    s.season,
    s.goals_for,
    ROUND(s.xg_for, 2) as expected_goals,
    s.xg_overperformance,
    CASE 
        WHEN s.xg_overperformance > 5 THEN 'Clinical finishers'
        WHEN s.xg_overperformance < -5 THEN 'Wasteful'
        ELSE 'As expected'
    END as finishing_quality
FROM fct_team_season_stats s
JOIN dim_teams t ON s.team_id = t.team_id
JOIN dim_competitions c ON s.competition_id = c.competition_id
WHERE s.season = '2023/24'
ORDER BY s.xg_overperformance DESC
LIMIT 10;
```

### Home Advantage Analysis

```sql
SELECT 
    c.competition_name,
    ROUND(AVG(f.home_goals), 2) as avg_home_goals,
    ROUND(AVG(f.away_goals), 2) as avg_away_goals,
    ROUND(AVG(f.home_goals) - AVG(f.away_goals), 2) as home_advantage,
    ROUND(
        SUM(CASE WHEN result = 'H' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 
        1
    ) as home_win_pct
FROM fct_matches f
JOIN dim_competitions c ON f.competition_id = c.competition_id
WHERE f.season = '2023/24'
GROUP BY c.competition_name
ORDER BY home_advantage DESC;
```

### Team Form (Last 5 Matches)

```sql
WITH team_matches AS (
    SELECT 
        home_team_id as team_id,
        match_date,
        CASE 
            WHEN result = 'H' THEN 3 
            WHEN result = 'D' THEN 1 
            ELSE 0 
        END as points
    FROM fct_matches
    WHERE season = '2023/24'
    
    UNION ALL
    
    SELECT 
        away_team_id as team_id,
        match_date,
        CASE 
            WHEN result = 'A' THEN 3 
            WHEN result = 'D' THEN 1 
            ELSE 0 
        END as points
    FROM fct_matches
    WHERE season = '2023/24'
),

recent_form AS (
    SELECT 
        team_id,
        points,
        ROW_NUMBER() OVER (
            PARTITION BY team_id 
            ORDER BY match_date DESC
        ) as recency_rank
    FROM team_matches
)

SELECT 
    t.team_name,
    SUM(rf.points) as last_5_points,
    ROUND(AVG(rf.points), 2) as avg_points_per_match
FROM recent_form rf
JOIN dim_teams t ON rf.team_id = t.team_id
WHERE rf.recency_rank <= 5
GROUP BY t.team_name
ORDER BY last_5_points DESC
LIMIT 10;
```

---

## 📁 Project Structure

```
footbase/
├── README.md
├── dbt_project.yml
├── models/
│   ├── staging/
│   │   ├── sources.yml           # Raw data source definitions
│   │   ├── schema.yml            # Staging tests
│   │   └── stg_matches.sql       # Match staging model
│   └── marts/
│       ├── schema.yml            # Marts tests
│       ├── dim_teams.sql         # Teams dimension
│       ├── dim_competitions.sql  # Competitions dimension
│       ├── fct_matches.sql       # Match facts
│       └── fct_team_season_stats.sql  # Team season aggregates
├── data/
│   └── matches.csv              # Raw match data
├── tests/                       # Custom data tests (optional)
├── macros/                      # Custom dbt macros (optional)
└── docs/
    ├── stg_matches.md          # Model documentation
    ├── dim_teams.md
    ├── dim_competitions.md
    └── fct_matches.md
```

---

## 🧠 Design Decisions

### Why Natural Keys?

We use **natural keys** (business-meaningful identifiers) instead of surrogate keys:

```sql
-- Natural key example:
team_id = 'bayern_munich'  -- Immediately clear!

-- vs Surrogate key:
team_id = 42  -- What team is this?
```

**Advantages:**
- ✅ Self-documenting - Know what the ID refers to without joining
- ✅ Deterministic - Same input always produces same ID
- ✅ Easier debugging - Readable in logs and queries
- ✅ SQLite compatible - No MD5 hashing required
- ✅ Perfect for small dimensions (<1000 rows)

**Technical reason:** `dbt_utils.surrogate_key()` doesn't work in SQLite (requires MD5 hashing). Natural keys are the pragmatic solution.

### Why SQLite?

**Development:**
- ✅ Zero setup - No server installation
- ✅ Portable - Single file database
- ✅ Fast for <100K rows
- ✅ Perfect for learning and portfolios

**Production migration path:**
```sql
-- All SQL is portable to:
- PostgreSQL
- DuckDB
- BigQuery
- Snowflake

-- Just change adapter in profiles.yml!
```

### Why This Schema?

**Star schema benefits:**
- ✅ Optimized for analytical queries (BI tools love it)
- ✅ Easy to understand (business users can query)
- ✅ Fast aggregations (dimension lookups are quick)
- ✅ Industry standard (familiar to data teams)

### Matchday Calculation

We calculate matchdays from row order, not dates:

```sql
CASE 
    WHEN league = 'Bundesliga' THEN CAST((rn / 9.0 + 0.999999) AS INTEGER)
    ELSE CAST((rn / 10.0 + 0.999999) AS INTEGER)
END AS matchday
```

**Why:** 2017/18 season has missing dates for 3 leagues. Football CSVs are naturally ordered chronologically, so row order is reliable.

---

## 🛠️ Development

### Running Specific Models

```bash
# Run single model
dbt run --select stg_matches

# Run model and downstream dependencies
dbt run --select stg_matches+

# Run model and upstream dependencies
dbt run --select +fct_matches

# Run all marts
dbt run --select marts.*
```

### Testing

```bash
# Run all tests
dbt test

# Run tests for specific model
dbt test --select fct_matches

# Run only relationship tests
dbt test --select test_type:relationships
```

### Documentation

```bash
# Generate docs
dbt docs generate

# Serve docs locally
dbt docs serve

# View at http://localhost:8080
```

### Adding New Data

```bash
# 1. Add new season data to data/matches.csv
# 2. Rebuild warehouse
dbt build

# All dimensions and facts update automatically!
```

---

## 🗺️ Roadmap

### ✅ Completed
- [x] Star schema foundation
- [x] Match-level facts with xG
- [x] Team season aggregates
- [x] Comprehensive testing
- [x] Full documentation

### 🚧 In Progress
- [ ] Brazilian Série A integration
- [ ] Copa Libertadores data
- [ ] UEFA Champions League data

### 🔮 Future Enhancements

**Short Term:**
- [ ] `fct_team_form` - Rolling form metrics (last 5, 10 matches)
- [ ] `fct_head_to_head` - H2H records between teams
- [ ] `fct_league_tables` - Historical standings by matchday
- [ ] Jupyter notebooks with analysis examples

**Medium Term:**
- [ ] Player-level statistics (if data available)
- [ ] `dim_referees` - Referee dimension
- [ ] `dim_venues` - Stadium dimension
- [ ] Elo rating system for team strength
- [ ] ML feature engineering models

**Long Term:**
- [ ] Event-level data (passes, shots, tackles)
- [ ] Real-time match updates
- [ ] Interactive Streamlit dashboard
- [ ] REST API for match data
- [ ] Migration to PostgreSQL/DuckDB

---

## 📈 Use Cases

### Data Analysis
```python
# Connect with pandas
import sqlite3
import pandas as pd

conn = sqlite3.connect('footbase.db')
df = pd.read_sql_query("""
    SELECT * FROM fct_team_season_stats 
    WHERE season = '2023/24'
""", conn)
```

### Machine Learning
```python
# Pre-computed features ready for scikit-learn
from sklearn.ensemble import RandomForestClassifier

features = ['xg_for', 'xg_against', 'home_advantage_ppg', 'form']
X = df[features]
y = df['made_champions_league']

model = RandomForestClassifier()
model.fit(X, y)
```

### BI Dashboards
- Connect Tableau/PowerBI/Metabase to SQLite
- Pre-joined star schema = fast queries
- Dimension tables for filters/slicers

### API Backend
```python
# Flask/FastAPI endpoint
@app.get("/teams/{team_id}/stats")
def get_team_stats(team_id: str, season: str):
    return db.query(fct_team_season_stats).filter(
        team_id=team_id, season=season
    ).first()
```

---

## 🤝 Contributing

Contributions welcome! Here's how:

1. **Fork the repository**
2. **Create a feature branch**
   ```bash
   git checkout -b feature/new-competition
   ```
3. **Make your changes**
   - Add models in `models/`
   - Add tests in `schema.yml`
   - Update documentation
4. **Run tests**
   ```bash
   dbt build
   ```
5. **Submit a pull request**

### Contribution Ideas
- Add new competitions (MLS, J-League, etc.)
- Create analytical views/aggregates
- Build example notebooks
- Improve documentation
- Add custom dbt tests
- Create macros for common patterns

---

## 📝 License

MIT License - Feel free to use this project for learning, portfolios, or commercial applications.

---

## 🙏 Acknowledgments

- **Data Sources:**
  - Match data: Various football-data.co.uk sources
  - xG data: [Understat](https://understat.com/)
  
- **Tools:**
  - [dbt](https://www.getdbt.com/) - Data transformation framework
  - [SQLite](https://www.sqlite.org/) - Database engine
  
- **Inspiration:**
  - Modern data warehouse best practices
  - Kimball dimensional modeling methodology
  - dbt community examples

---

## 📧 Contact

**Questions or suggestions?**
- Open an issue on GitHub
- Connect on [LinkedIn](https://linkedin.com/in/data-daim )
- Follow on [Twitter](https://x.com/FutebolViz)

---

## ⭐ Show Your Support

If you found this project helpful:
- ⭐ Star this repository
- 🍴 Fork it for your own projects
- 📢 Share it with others learning data engineering
- 💬 Provide feedback via issues

---

**Built with ❤️ for the football analytics community**
