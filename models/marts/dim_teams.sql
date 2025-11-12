{{
    config(
        materialized='table'
    )
}} 

With teams_from_matches as (
    -- Get all unique teams from home games
    select distinct
        home_team as team_name,
        league,
        season
    from {{ ref('stg_matches') }}
    
    union
    
    -- Get all unique teams from away games
    select distinct
        away_team as team_name,
        league,
        season
    from {{ ref('stg_matches') }}
),

team_aggregated as (
    -- Aggregate to get one row per team with their primary league
    select
        team_name,
        -- Get the most recent league they played in
        max(league) as primary_league,
        -- Get the most recent season they appeared
        max(season) as last_season,
        -- Count total seasons played
        count(distinct season) as seasons_played
    from teams_from_matches
    group by team_name
),

final as (
    select
        -- Generate stable team_id
        lower(replace(team_name, ' ', '_')) as team_id,
        team_name,
        primary_league as league,
        last_season,
        seasons_played,
        -- Metadata
        current_timestamp as _created_at
    from team_aggregated
)

select * from final