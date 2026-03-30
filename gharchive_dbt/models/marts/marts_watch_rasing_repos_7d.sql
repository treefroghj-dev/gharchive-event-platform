with current_week_watch_events_agg as (
    select
        repo_id,
        repo_name,
        sum(watch_events_cnt) as current_week_watch_count
    from {{ ref('fct_repo_daily_watch_fork_metrics') }}
    where event_date >= date_sub(current_date(), interval 7 day)
    and event_date < current_date()
    group by 1,2
),
prev_week_watch_events_agg as (
    select
        repo_id,
        repo_name,
        sum(watch_events_cnt) as prev_week_watch_count
    from {{ ref('fct_repo_daily_watch_fork_metrics') }}
    where
        event_date >= date_sub(current_date(), interval 14 day)
        and
        event_date < date_sub(current_date(), interval 7 day)
    group by 1,2
),
watch_events_agg as (
    select
        cw.repo_id,
        cw.repo_name,
        cw.current_week_watch_count as current_week_watch_count,
        coalesce(prev.prev_week_watch_count, 0) as prev_week_watch_count,
        cw.current_week_watch_count - coalesce(prev.prev_week_watch_count, 0) as watch_count_delta,
        (cw.current_week_watch_count + 1.0) / (coalesce(prev.prev_week_watch_count, 0) + 1.0) as watch_growth_ratio
    from current_week_watch_events_agg as cw
    left join prev_week_watch_events_agg as prev
    on
        cw.repo_id = prev.repo_id
        and
        cw.repo_name = prev.repo_name
)

select
    *
from watch_events_agg
where current_week_watch_count >= 20
order by watch_count_delta desc, current_week_watch_count desc

