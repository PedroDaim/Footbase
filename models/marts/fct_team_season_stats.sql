{{
    config(
        materialized='table'
    )
}}

with home_stats as (
    select
        home_team_id as team_id,
        season,
        competition_id,
        -- Match counts
        count(*) as home_matches,
        -- Results
        sum(case when result = 'H' then 1 else 0 end) as home_wins,
        sum(case when result = 'D' then 1 else 0 end) as home_draws,
        sum(case when result = 'A' then 1 else 0 end) as home_losses,
        -- Points
        sum(case 
            when result = 'H' then 3 
            when result = 'D' then 1 
            else 0 
        end) as home_points,
        -- Goals
        sum(home_goals) as home_goals_for,
        sum(away_goals) as home_goals_against,
        -- xG
        sum(home_xg) as home_xg_for,
        sum(away_xg) as home_xg_against,
        -- Shots
        sum(home_shots) as home_shots_for,
        sum(away_shots) as home_shots_against,
        sum(home_shots_on_target) as home_shots_on_target_for,
        sum(away_shots_on_target) as home_shots_on_target_against
    from {{ ref('fct_matches') }}
    group by home_team_id, season, competition_id
),

away_stats as (
    select
        away_team_id as team_id,
        season,
        competition_id,
        -- Match counts
        count(*) as away_matches,
        -- Results
        sum(case when result = 'A' then 1 else 0 end) as away_wins,
        sum(case when result = 'D' then 1 else 0 end) as away_draws,
        sum(case when result = 'H' then 1 else 0 end) as away_losses,
        -- Points
        sum(case 
            when result = 'A' then 3 
            when result = 'D' then 1 
            else 0 
        end) as away_points,
        -- Goals
        sum(away_goals) as away_goals_for,
        sum(home_goals) as away_goals_against,
        -- xG
        sum(away_xg) as away_xg_for,
        sum(home_xg) as away_xg_against,
        -- Shots
        sum(away_shots) as away_shots_for,
        sum(home_shots) as away_shots_against,
        sum(away_shots_on_target) as away_shots_on_target_for,
        sum(home_shots_on_target) as away_shots_on_target_against
    from {{ ref('fct_matches') }}
    group by away_team_id, season, competition_id
),

combined_stats as (
    select
        coalesce(h.team_id, a.team_id) as team_id,
        coalesce(h.season, a.season) as season,
        coalesce(h.competition_id, a.competition_id) as competition_id,
        
        -- Match counts
        coalesce(h.home_matches, 0) + coalesce(a.away_matches, 0) as matches_played,
        coalesce(h.home_matches, 0) as home_matches,
        coalesce(a.away_matches, 0) as away_matches,
        
        -- Overall results
        coalesce(h.home_wins, 0) + coalesce(a.away_wins, 0) as wins,
        coalesce(h.home_draws, 0) + coalesce(a.away_draws, 0) as draws,
        coalesce(h.home_losses, 0) + coalesce(a.away_losses, 0) as losses,
        
        -- Home/Away splits
        coalesce(h.home_wins, 0) as home_wins,
        coalesce(h.home_draws, 0) as home_draws,
        coalesce(h.home_losses, 0) as home_losses,
        coalesce(a.away_wins, 0) as away_wins,
        coalesce(a.away_draws, 0) as away_draws,
        coalesce(a.away_losses, 0) as away_losses,
        
        -- Points
        coalesce(h.home_points, 0) + coalesce(a.away_points, 0) as total_points,
        coalesce(h.home_points, 0) as home_points,
        coalesce(a.away_points, 0) as away_points,
        
        -- Goals
        coalesce(h.home_goals_for, 0) + coalesce(a.away_goals_for, 0) as goals_for,
        coalesce(h.home_goals_against, 0) + coalesce(a.away_goals_against, 0) as goals_against,
        coalesce(h.home_goals_for, 0) as home_goals_for,
        coalesce(a.away_goals_for, 0) as away_goals_for,
        
        -- xG
        coalesce(h.home_xg_for, 0) + coalesce(a.away_xg_for, 0) as xg_for,
        coalesce(h.home_xg_against, 0) + coalesce(a.away_xg_against, 0) as xg_against,
        
        -- Shots
        coalesce(h.home_shots_for, 0) + coalesce(a.away_shots_for, 0) as shots_for,
        coalesce(h.home_shots_against, 0) + coalesce(a.away_shots_against, 0) as shots_against,
        coalesce(h.home_shots_on_target_for, 0) + coalesce(a.away_shots_on_target_for, 0) as shots_on_target_for,
        coalesce(h.home_shots_on_target_against, 0) + coalesce(a.away_shots_on_target_against, 0) as shots_on_target_against
        
    from home_stats h
    full outer join away_stats a 
        on h.team_id = a.team_id 
        and h.season = a.season
        and h.competition_id = a.competition_id
),

final as (
    select
        team_id,
        season,
        competition_id,
        
        -- Match counts
        matches_played,
        home_matches,
        away_matches,
        
        -- Overall record
        wins,
        draws,
        losses,
        total_points,
        
        -- Home/Away record
        home_wins,
        home_draws,
        home_losses,
        home_points,
        away_wins,
        away_draws,
        away_losses,
        away_points,
        
        -- Goals
        goals_for,
        goals_against,
        goals_for - goals_against as goal_difference,
        home_goals_for,
        away_goals_for,
        
        -- Averages
        round(cast(goals_for as float) / matches_played, 2) as goals_for_per_match,
        round(cast(goals_against as float) / matches_played, 2) as goals_against_per_match,
        round(cast(total_points as float) / matches_played, 2) as points_per_match,
        
        -- xG metrics
        xg_for,
        xg_against,
        round(xg_for - xg_against, 2) as xg_difference,
        round(cast(xg_for as float) / matches_played, 2) as xg_for_per_match,
        round(cast(xg_against as float) / matches_played, 2) as xg_against_per_match,
        
        -- xG performance (actual vs expected)
        round(goals_for - xg_for, 2) as xg_overperformance,
        round((goals_for - xg_for) / matches_played, 2) as xg_overperformance_per_match,
        
        -- Shot metrics
        shots_for,
        shots_against,
        shots_on_target_for,
        shots_on_target_against,
        round(cast(shots_on_target_for as float) / shots_for * 100, 1) as shot_accuracy_pct,
        round(cast(goals_for as float) / shots_on_target_for * 100, 1) as conversion_rate_pct,
        
        -- Home advantage
        round(cast(home_points as float) / home_matches - cast(away_points as float) / away_matches, 2) as home_advantage_ppg,
        
        -- Metadata
        current_timestamp as _created_at
        
    from combined_stats
    where matches_played > 0  -- Filter out teams with no matches
)

select * from final