-- SQLite
SELECT
    league,
    season,
    home_team,
    away_team,
    home_goals,
    away_goals,
    result
FROM matches
WHERE season = '2017/18'
  AND (date IS NULL OR date = '')

SELECT
    home_team,
    away_team,
    home_goals,
    away_goals
FROM matches
WHERE season = '2017/18'
  AND league = 'La Liga'
  AND date IS NULL
LIMIT 20;

SELECT COUNT(*) 
FROM matches
WHERE season = '2017/18'
  AND league = 'Series A'
  AND date IS NOT NULL;

SELECT 
    league,
    COUNT(*) AS total_rows,
    SUM(CASE WHEN home_xg IS NULL OR home_xg = '' THEN 1 ELSE 0 END) AS missing_home_xg,
    SUM(CASE WHEN away_xg IS NULL OR away_xg = '' THEN 1 ELSE 0 END) AS missing_away_xg
FROM matches
GROUP BY league
ORDER BY league;

-- Check how many matchdays per league and season
SELECT season, league, MAX(matchday) AS total_matchdays
FROM stg_matches
GROUP BY season, league
ORDER BY season, league;


