-- incremental materialization via load_id
{{
    config(
        materialized='incremental'
    )
}}

with games as
    -- reflect the games so we can join to either side with ease.
      (SELECT distinct url,
        end_time,
        black__rating as player_rating,
        black__username as player_username,
        black__result as player_result,
        white__rating as opponent_rating,
        white__username as opponent_username,
        white__result as opponent_result
      FROM {{ source('chess', 'players_games') }}
      union distinct
      SELECT distinct url,
        end_time,
        white__rating as player_rating,
        white__username as player_username,
        white__result as player_result,
        black__rating as opponent_rating,
        black__username as opponent_username,
        black__result as opponent_result
      FROM {{ source('chess', 'players_games') }}
      ),
    view_player_games as (select distinct p.username,
        g.url,
        g.end_time,
        g.player_rating,
        g.opponent_rating,
        g.player_result,
        g.opponent_result
        from {{ source('chess', 'players_profiles') }} p
        inner join games as g
        on lower(g.player_username) = lower(p.username)
    )
    select * from view_player_games
    --TODO: join load_ids