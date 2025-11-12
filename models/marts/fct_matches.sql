{{
    config(
        materialized='table'
    )
}}

with matches as (
    select * from {{ ref('stg_matches') }}
),

final as (
    select
        -- Primary key
        match_id,
        
        -- Time dimensions
        date as match_date,
        season,
        matchday,
        
        -- Foreign keys to dimensions (using natural key generation)
        lower(replace(league, ' ', '_')) as competition_id,
        lower(replace(home_team, ' ', '_')) as home_team_id,
        lower(replace(away_team, ' ', '_')) as away_team_id,
        
        -- Match outcome
        result,
        
        -- Goal facts
        home_goals,
        away_goals,
        
        -- Expected goals facts
        home_xg,
        away_xg,
        
        -- Shot facts
        home_shots,
        away_shots,
        home_shots_on_target,
        away_shots_on_target,
        
        -- Betting odds facts
        odds_home,
        odds_draw,
        odds_away,
        
        -- Metadata
        current_timestamp as _created_at
        
    from matches
)

select * from final