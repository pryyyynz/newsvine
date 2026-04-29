with weighted as (
  select
    i.user_id,
    a.category,
    i.rating,
    i.interaction_count,
    i.last_interaction_ts
  from {{ ref('int_user_article_matrix') }} i
  join {{ ref('stg_articles') }} a
    on a.article_id = i.article_id
),
aggregated as (
  select
    user_id,
    category,
    sum(rating) as topic_weight,
    sum(interaction_count) as interaction_count,
    max(last_interaction_ts) as last_interaction_ts
  from weighted
  group by user_id, category
),
normalizer as (
  select
    user_id,
    sum(topic_weight) as total_weight
  from aggregated
  group by user_id
)

select
  a.user_id,
  a.category,
  a.user_id || '|' || a.category as user_category_key,
  a.topic_weight,
  case
    when n.total_weight > 0 then a.topic_weight / n.total_weight
    else 0.0
  end as normalized_topic_weight,
  a.interaction_count,
  a.last_interaction_ts
from aggregated a
join normalizer n
  on a.user_id = n.user_id
