select
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
from {{ source('gharchive_events', 'watch_events') }}
where event_id is not null
  and repo_id is not null
  and event_date is not null