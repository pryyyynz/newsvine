with interaction_totals as (
  select
    article_id,
    sum(rating) as interaction_weight,
    sum(interaction_count) as interaction_count
  from {{ ref('int_user_article_matrix') }}
  group by article_id
)

select
  a.article_id,
  a.category,
  a.country,
  a.published_at,
  a.title,
  a.content,
  case when a.category = 'tech' then 1 else 0 end as cat_tech,
  case when a.category = 'business' then 1 else 0 end as cat_business,
  case when a.category = 'sports' then 1 else 0 end as cat_sports,
  case when a.category = 'health' then 1 else 0 end as cat_health,
  case when a.category = 'science' then 1 else 0 end as cat_science,
  case when a.category = 'politics' then 1 else 0 end as cat_politics,
  case when a.category = 'general' then 1 else 0 end as cat_general,
  cast(1.0 / (1.0 + {{ days_since('a.published_at') }}) as {{ float_type() }}) as recency_score,
  coalesce(t.interaction_weight, 0.0) as interaction_weight,
  coalesce(t.interaction_count, 0) as interaction_count
from {{ ref('stg_articles') }} a
left join interaction_totals t
  on a.article_id = t.article_id
