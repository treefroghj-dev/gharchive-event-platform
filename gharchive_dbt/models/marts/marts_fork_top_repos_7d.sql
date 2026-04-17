with weekly_fork_events_agg as (
    select
        repo_id,
        repo_name,
        sum(fork_events_cnt) as weekly_fork_count
    from {{ ref('fct_repo_daily_watch_fork_metrics') }}
    where event_date >= date_sub({{ anchor_date() }}, interval 7 day)
    and event_date < {{ anchor_date() }}
    group by 1,2
)

select
    * 
from weekly_fork_events_agg
order by weekly_fork_count desc
limit 10