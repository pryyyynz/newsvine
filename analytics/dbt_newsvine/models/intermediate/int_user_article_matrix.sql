with scored as (
  select
    user_id,
    article_id,
    max(topic) as topic,
    sum(
      case event_type
        when 'click' then 1.0
        when 'like' then 2.0
        when 'bookmark' then 1.8
        when 'search' then 1.5
        else 1.0
      end
    ) as rating,
    count(*) as interaction_count,
    max(event_ts) as last_interaction_ts
  from {{ ref('stg_interactions') }}
  group by user_id, article_id
)

select
  user_id,
  article_id,
  user_id || '|' || article_id as user_article_key,
  topic,
  rating,
  interaction_count,
  last_interaction_ts
from scored
