{{
    config(
        materialized='table'
    )
}}

with competitions_from_matches as (
    select distinct
        league as competition_name
    from {{ ref('stg_matches') }}
),

final as (
    select
        -- Generate stable competition_id
        lower(replace(competition_name, ' ', '_')) as competition_id,
        competition_name,
        -- Infer country from league name
        case
            when competition_name = 'Premier League' then 'England'
            when competition_name = 'La Liga' then 'Spain'
            when competition_name = 'Bundesliga' then 'Germany'
            when competition_name = 'Serie A' then 'Italy'
            when competition_name = 'Ligue 1' then 'France'
            else null
        end as country,
        current_timestamp as _created_at
    from competitions_from_matches
)

select * from final