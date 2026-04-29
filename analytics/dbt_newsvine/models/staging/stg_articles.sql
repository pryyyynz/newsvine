select
  id as article_id,
  source,
  lower(trim(country)) as country,
  case
    when lower(trim(category)) in ('tech', 'technology') then 'tech'
    when lower(trim(category)) in ('business', 'finance', 'economy') then 'business'
    when lower(trim(category)) in ('sports', 'sport') then 'sports'
    when lower(trim(category)) in ('health', 'wellness') then 'health'
    when lower(trim(category)) in ('science', 'sci-tech') then 'science'
    when lower(trim(category)) in ('politics', 'political') then 'politics'
    else 'general'
  end as category,
  url,
  published_at,
  coalesce(nullif(trim({{ json_text('payload', 'title') }}), ''), '[untitled]') as title,
  coalesce(nullif(trim({{ json_text('payload', 'content') }}), ''), '') as content,
  ingested_at
from {{ source('raw', 'news_raw_articles') }}
