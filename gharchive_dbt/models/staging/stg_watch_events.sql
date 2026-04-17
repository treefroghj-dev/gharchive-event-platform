with watch_events as (
    select
        event_id,
        event_type,
        event_ts,
        date(event_ts) as event_date,
        event_hour,
        repo_id,
        repo_name,
        actor_id,
        actor_login,
        org_id,
        org_login,
        action,
        is_public
    from {{ source('gharchive_events', 'watch_events') }}
    where event_id is not null
      and repo_id is not null
      and event_ts is not null
),

event_id_counts as (
    select
        event_id,
        count(*) as row_count
    from watch_events
    group by event_id
),

non_duplicate_event_ids as (
    select event_id
    from event_id_counts
    where row_count = 1
),

duplicate_event_ids as (
    select event_id
    from event_id_counts
    where row_count > 1
),

non_duplicate_watch_events as (
    select w.*
    from watch_events w
    join non_duplicate_event_ids n
      on w.event_id = n.event_id
),

duplicate_watch_events as (
    select w.*
    from watch_events w
    join duplicate_event_ids d
      on w.event_id = d.event_id
),

deduped_duplicate_watch_events as (
    select distinct
        event_id,
        event_type,
        event_ts,
        event_date,
        event_hour,
        repo_id,
        repo_name,
        actor_id,
        actor_login,
        org_id,
        org_login,
        action,
        is_public
    from duplicate_watch_events
),

final_watch_events as (
    select * from non_duplicate_watch_events
    union distinct
    select * from deduped_duplicate_watch_events
)

select *
from final_watch_events
