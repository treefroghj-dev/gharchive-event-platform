-- Rolling windows use anchor_date (from Airflow --vars, same as DAG logical date); excludes the anchor calendar day (partial data).

with repos_weekly_metrics as (
    select
        repo_id,
        repo_name,
        sum(watch_events_cnt) as weekly_watch_count,
        sum(fork_events_cnt) as weekly_fork_count
    from {{ ref('fct_repo_daily_watch_fork_metrics') }}
    where event_date >= date_sub({{ anchor_date() }}, interval 7 day)
    and event_date < {{ anchor_date() }}
    group by 1,2
),
repos_with_activity as (
    select
        repo_id,
        repo_name,
        weekly_watch_count,
        weekly_fork_count,
        case
            when weekly_watch_count < 20 and weekly_fork_count < 20 then 'low_signal'
            else 'active'
        end as activity_label
    from repos_weekly_metrics
),
repo_intent_classification as (
    select
        repo_id,
        repo_name,
        weekly_watch_count,
        weekly_fork_count,
        case
            when safe_divide(weekly_fork_count, nullif(weekly_watch_count, 0)) >= 1.2 then 'usage_driven'
            when safe_divide(weekly_fork_count, nullif(weekly_watch_count, 0)) <= 0.5 then 'interest_driven'
            else 'balanced'
        end as repo_intent_type
    from repos_with_activity
    where activity_label = 'active'
),
repo_intent_classification_summary as (
    select
    'usage_driven' as intent_type,
    safe_divide(countif(repo_intent_type = 'usage_driven'), count(*)) as rate
    from repo_intent_classification
    union all
    select
    'interest_driven' as intent_type,
    safe_divide(countif(repo_intent_type = 'interest_driven'), count(*)) as rate
    from repo_intent_classification
    union all
    select
    'balanced' as intent_type,
    safe_divide(countif(repo_intent_type = 'balanced'), count(*)) as rate
    from repo_intent_classification
)

select * from repo_intent_classification_summary
