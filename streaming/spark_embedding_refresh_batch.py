import json
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, current_timestamp, expr, lit, size, udf
from pyspark.sql.types import ArrayType, FloatType, StringType

_EMBEDDER = None


def build_spark() -> SparkSession:
    return SparkSession.builder.appName("newsvine-embedding-refresh-batch").getOrCreate()


def load_articles(spark: SparkSession):
    backend = os.getenv("PHASE5_SOURCE_BACKEND", "local_postgres").strip().lower()

    if backend == "bigquery":
        table = os.getenv("PHASE5_BQ_ARTICLE_FEATURES_TABLE", "news_features.fct_article_features")
        source_df = spark.read.format("bigquery").option("table", table).load()
        return source_df.select("article_id", "category", "title", "content", "published_at")

    jdbc_url = os.getenv("PHASE5_POSTGRES_JDBC_URL", "jdbc:postgresql://postgres:5432/users")
    jdbc_user = os.getenv("PHASE5_POSTGRES_USER", "newsvine")
    jdbc_password = os.getenv("PHASE5_POSTGRES_PASSWORD", "newsvine")
    source_table = os.getenv("PHASE5_ARTICLE_FEATURES_TABLE", "marts.fct_article_features")

    return (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", source_table)
        .option("user", jdbc_user)
        .option("password", jdbc_password)
        .option("driver", "org.postgresql.Driver")
        .load()
        .select("article_id", "category", "title", "content", "published_at")
    )


def _encode_text(text: str) -> list[float]:
    global _EMBEDDER

    if _EMBEDDER is None:
        try:
            from sentence_transformers import SentenceTransformer
        except ImportError as exc:
            raise RuntimeError(
                "sentence-transformers is required for embedding refresh. "
                "Install it in the Spark image before running phase 5 batch jobs."
            ) from exc

        model_name = os.getenv("PHASE5_EMBEDDING_MODEL", "all-MiniLM-L6-v2")
        _EMBEDDER = SentenceTransformer(model_name)

    cleaned = (text or "").strip()
    if not cleaned:
        return []

    values = _EMBEDDER.encode(cleaned, normalize_embeddings=True)
    return [float(value) for value in values]


def _to_sparse_json(values: list[float]) -> str:
    payload = {
        str(idx): float(value)
        for idx, value in enumerate(values)
        if value != 0.0
    }
    return json.dumps(payload, separators=(",", ":"))


def main() -> None:
    spark = build_spark()

    lookback_days = int(os.getenv("PHASE5_EMBEDDING_LOOKBACK_DAYS", "7"))
    model_name = os.getenv("PHASE5_EMBEDDING_MODEL", "all-MiniLM-L6-v2")

    source_df = load_articles(spark)
    recent_df = source_df.where(
        col("published_at") >= expr(f"current_timestamp() - INTERVAL {lookback_days} DAYS")
    )

    encode_udf = udf(_encode_text, ArrayType(FloatType()))
    sparse_json_udf = udf(_to_sparse_json, StringType())

    text_df = recent_df.withColumn("combined_text", concat_ws(" ", col("title"), col("content")))
    embedded_df = text_df.withColumn("embedding", encode_udf(col("combined_text")))

    result_df = (
        embedded_df.withColumn("embedding_json", sparse_json_udf(col("embedding")))
        .withColumn("embedding_dim", size(col("embedding")))
        .withColumn("model_name", lit(model_name))
        .withColumn("source_published_at", col("published_at"))
        .withColumn("refreshed_at", current_timestamp())
        .select(
            "article_id",
            "category",
            "model_name",
            "embedding_json",
            "embedding_dim",
            "source_published_at",
            "refreshed_at",
        )
        .where(col("embedding_dim") > 0)
    )
    result_df = result_df.cache()
    row_count = result_df.count()

    destination_backend = os.getenv("PHASE5_EMBEDDINGS_DEST_BACKEND", "local_postgres").strip().lower()

    if destination_backend == "bigquery":
        destination_table = os.getenv(
            "PHASE5_BQ_EMBEDDINGS_TABLE",
            "news_features.article_embeddings",
        )
        (
            result_df.write.format("bigquery")
            .option("table", destination_table)
            .mode("overwrite")
            .save()
        )
    else:
        jdbc_url = os.getenv("PHASE5_POSTGRES_JDBC_URL", "jdbc:postgresql://postgres:5432/users")
        jdbc_user = os.getenv("PHASE5_POSTGRES_USER", "newsvine")
        jdbc_password = os.getenv("PHASE5_POSTGRES_PASSWORD", "newsvine")
        destination_table = os.getenv(
            "PHASE5_EMBEDDINGS_TABLE",
            "news_features.article_embeddings",
        )
        (
            result_df.write.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", destination_table)
            .option("user", jdbc_user)
            .option("password", jdbc_password)
            .option("driver", "org.postgresql.Driver")
            .option("truncate", "true")
            .mode("overwrite")
            .save()
        )

    print(f"Embedding refresh completed. rows={row_count}")


if __name__ == "__main__":
    main()
