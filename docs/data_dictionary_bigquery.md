# BigQuery Data Dictionary (Phase 6)

This dictionary reflects migration-target analytics and feature tables.

## Dataset: `news_features`

### Table: `int_user_article_matrix`

- `user_id` STRING: user identifier
- `article_id` STRING: article identifier
- `topic` STRING: inferred/observed topic
- `rating` FLOAT64: interaction-weighted signal
- `interaction_count` INT64: number of interactions for pair
- `last_interaction_ts` TIMESTAMP: latest interaction timestamp

### Table: `fct_article_features`

- `article_id` STRING
- `category` STRING
- `country` STRING
- `published_at` TIMESTAMP
- `title` STRING
- `content` STRING
- `cat_*` INT64 category one-hot columns
- `recency_score` FLOAT64
- `interaction_weight` FLOAT64
- `interaction_count` INT64

### Table: `fct_user_profiles`

- `user_id` STRING
- `category` STRING
- `topic_weight` FLOAT64
- `normalized_topic_weight` FLOAT64
- `interaction_count` INT64
- `last_interaction_ts` TIMESTAMP

### Table: `article_embeddings`

- `article_id` STRING
- `category` STRING
- `model_name` STRING
- `embedding_json` STRING
- `embedding_dim` INT64
- `source_published_at` TIMESTAMP
- `refreshed_at` TIMESTAMP

### Table: `als_user_recommendations`

- `user_id` STRING
- `article_id` STRING
- `score` FLOAT64
- `model_run_id` STRING
- `refreshed_at` TIMESTAMP
