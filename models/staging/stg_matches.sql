{{ config(materialized='view') }}

WITH ordered AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY league, season
            ORDER BY home_team, away_team
        ) AS rn
    FROM {{ source('football_data', 'matches') }}
),

with_matchday AS (
    SELECT
        *,
        CASE 
            WHEN league = 'Bundesliga' THEN CEIL(rn / 9.0)
            ELSE CEIL(rn / 10.0)
        END AS matchday
    FROM ordered
),

final AS (
    SELECT
        season,
        league,
        matchday,
        home_team,
        away_team,
        home_goals,
        away_goals,
        result,
        home_shots,
        away_shots,
        home_shots_on_target,
        away_shots_on_target,
        odds_home,
        odds_draw,
        odds_away,
        home_xg,
        away_xg,
        date,
        CURRENT_TIMESTAMP AS _created_at,
        -- Build match_id
        lower(
            replace(season, '/', '') || '_' ||
            replace(league, ' ', '') || '_' ||
            matchday || '_' ||
            replace(home_team, ' ', '') || '_' ||
            replace(away_team, ' ', '') || '_' ||
            rn
        ) AS match_id
    FROM with_matchday
)

SELECT *
FROM final
