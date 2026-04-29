select
  event_id as interaction_id,
  lower(trim(event_type)) as event_type,
  trim(article_id) as article_id,
  trim(user_id) as user_id,
  coalesce(nullif(lower(trim(country)), ''), 'global') as country,
  coalesce(nullif(lower(trim(topic)), ''), 'general') as topic,
  nullif(trim(query), '') as query,
  event_ts,
  ingested_at
from {{ source('raw', 'interaction_events_raw') }}
where trim(user_id) <> ''
  and trim(article_id) <> ''
  and lower(trim(user_id)) not like 'test-%'
  and lower(trim(user_id)) not like 'fixture-%'
