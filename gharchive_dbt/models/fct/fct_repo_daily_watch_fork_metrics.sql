{{ config(
    materialized='table',
    partition_by={
      "field": "event_date",
      "data_type": "date"
    },
    cluster_by=["repo_id"]
) }}


with watch_event_agg as (
    select
        event_date,
        repo_id,
        repo_name,
        count(*) as watch_events_cnt
    from {{ ref('stg_watch_events') }}
    group by 1, 2, 3
),

fork_event_agg as (
    select
        event_date,
        repo_id,
        repo_name,
        count(*) as fork_events_cnt
    from {{ ref('stg_fork_events') }}
    group by 1, 2, 3
)

select
    coalesce(w.event_date, f.event_date) as event_date,
    coalesce(w.repo_id, f.repo_id) as repo_id,
    coalesce(w.repo_name, f.repo_name) as repo_name,
    coalesce(w.watch_events_cnt, 0) as watch_events_cnt,
    coalesce(f.fork_events_cnt, 0) as fork_events_cnt
from watch_event_agg w
full outer join fork_event_agg f
    on w.event_date = f.event_date
   and w.repo_id = f.repo_id
   and w.repo_name = f.repo_name
