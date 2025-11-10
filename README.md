# ⚽ FootBase: Football Data Warehouse

**FootBase** is a modular, Python-based football data warehouse designed to centralize and enrich match data from multiple public sources.  
It merges **Football-Data.co.uk** traditional statistics with **Understat** advanced metrics (xG, forecasts), storing everything in a unified SQLite database (`footbase_big5.db`).

This project is both:
- A **portfolio piece**, showcasing full-stack football data engineering.
- A **foundation** for deeper analytics, tactical studies, and BI integration.

---

## 🚀 Features

- Collects, cleans, and merges data from multiple football data sources  
- Integrates **expected goals (xG)** and **forecast data** from Understat  
- Standardizes team names across leagues for seamless joins  
- Validates and ensures data consistency (goals, dates, etc.)  
- Stores data in a **SQLite database** for querying and visualization  
- Automatically creates **versioned backups** for safety  
- Ready for integration with **pandas**, **BI tools**, and future **dbt pipelines**

---

## 🧠 Project Overview

### Current Sources
- **Football-Data.co.uk** → Traditional match stats  
- **Understat** → xG, goals, and forecasts  

### Storage
- `footbase_big5.db` (SQLite)
  - `matches` table: merged traditional + xG data  
  - `matches_backup_*`: automated backups  

### Example Columns
date | league | season | home_team | away_team | home_goals | away_goals | home_xg | away_xg | result | ...

## ⚙️ Pipeline Summary

1. **Load data**  
   - Reads Football-Data.co.uk data from SQLite  
   - Loads Understat xG dataset (`understat_xg_data3.csv`)  

2. **Standardize**  
   - Cleans and normalizes team names (e.g. `Man City` → `Manchester City`)  

3. **Merge**  
   - Joins datasets on `date`, `home_team`, `away_team`  
   - Adds xG columns where matches align  

4. **Validate**  
   - Confirms goals match between datasets  
   - Reports xG coverage per league  

5. **Save**  
   - Backs up existing matches table  
   - Writes merged results into `matches`  

## 🧩 Key Script: `merge_xg_data.py`

This is the main processing script that merges Football-Data.co.uk and Understat data.

Example console output:
FootBase: Football Data + Understat xG Merge
✓ Loaded 14,359 matches from Football-Data
✓ Matched with xG: 13,812 (96.2%)
✓ All matched goals consistent!
✓ Backup created: matches_backup_20251104_153200
✓ Updated 'matches' table with xG data

## 💻 Tech Stack

| Component | Technology |
|------------|-------------|
| Language | Python 3.11 |
| Data Handling | pandas, numpy |
| Storage | SQLite |
| Web/Data | requests, understat |
| Visualization | matplotlib |
| Environment | Conda (`environment.yml`) |
| Future | dbt, CRON jobs, BI dashboards |

---

## 🧰 Setup & Usage

### 1. Clone the repository
```bash
git clone https://github.com/PedroDaim/Footbase.git
cd footbase
```
### **2. Create the environment**
conda env create -f environment.yml
conda activate footbase
conda env create -f environment.yml
conda activate footbase
```
3. Run the merge script
bash
Copy code
python scripts/merge_xg_data.py
This will:

Load Football-Data and Understat datasets

Merge and validate matches

Update your SQLite database with xG columns

Create a backup table automatically
```
4. Query the data
python
Copy code
import pandas as pd
import sqlite3
conn = sqlite3.connect('db/footbase_big5.db')
matches = pd.read_sql("SELECT * FROM matches LIMIT 10;", conn)
print(matches.head())
```
📈 Roadmap
Add UEFA and Copa Libertadores data

Introduce dbt for transformations and lineage

Schedule auto-updates via CRON or Prefect

Build dashboards and tactical visualizations

📁 Project Structure
Copy code
footbase/
│
├── data/
│   ├── raw/
│   │   └── understat_xg_data3.csv
│   └── processed/
│
├── db/
│   └── footbase_big5.db
│
├── scripts/
│   └── merge_xg_data.py
│
├── notebooks/
│   └── exploratory_analysis.ipynb
│
├── environment.yml
└── README.md
```
👤 Author
Pedro Daim
Data Analyst
```
📧 pdaim.analytics@gmail.com
🐦 @PlotTheGame







