-- Exclude today's data to avoid partial/incomplete results

with repos_weekly_metrics as (
    select
        repo_id,
        repo_name,
        sum(watch_events_cnt) as weekly_watch_count,
        sum(fork_events_cnt) as weekly_fork_count
    from {{ ref('fct_repo_daily_watch_fork_metrics') }}
    where event_date >= date_sub(current_date(), interval 7 day)
    and event_date < current_date()
    group by 1,2
)

-- Filter to repos with >= 20 watches in the last 7 days.
-- With smaller volumes, `conversion_rate` is too noisy to interpret.

select
    repo_id,
    repo_name,
    weekly_watch_count,
    weekly_fork_count,
    coalesce(weekly_fork_count / nullif(weekly_watch_count, 0), 0) as conversion_rate
from repos_weekly_metrics
where weekly_watch_count >= 20
order by conversion_rate desc, weekly_watch_count desc

