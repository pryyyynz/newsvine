import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit, substring
from pyspark.sql.types import StringType, StructField, StructType


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("newsvine-article-consumer")
        .config("spark.sql.streaming.schemaInference", "true")
        .getOrCreate()
    )


def main() -> None:
    bootstrap = os.getenv("SPARK_KAFKA_BOOTSTRAP", "kafka:29092")
    topic = os.getenv("SPARK_NEWS_TOPIC", "news-articles")
    checkpoint = os.getenv("SPARK_CHECKPOINT", "/tmp/checkpoints/newsvine_articles")
    raw_output = os.getenv("SPARK_RAW_OUTPUT", "/tmp/news_raw_articles")

    schema = StructType(
        [
            StructField("id", StringType()),
            StructField("title", StringType()),
            StructField("content", StringType()),
            StructField("category", StringType()),
            StructField("timestamp", StringType()),
            StructField("source", StringType()),
            StructField("country", StringType()),
            StructField("url", StringType()),
        ]
    )

    spark = build_spark()

    source_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed = (
        source_df.select(from_json(col("value").cast("string"), schema).alias("article"))
        .select("article.*")
        .where(col("id").isNotNull())
        .withColumn("content_snippet", substring(col("content"), 1, 500))
    )

    # Local substitute for BigQuery in development: write raw records to parquet.
    parquet_query = (
        parsed.withColumn("ingest_source", lit("spark"))
        .writeStream.outputMode("append")
        .format("parquet")
        .option("path", raw_output)
        .option("checkpointLocation", checkpoint + "_parquet")
        .start()
    )

    es_url = os.getenv("SPARK_ELASTICSEARCH_URL", "http://elasticsearch:9200")

    def write_to_elasticsearch(batch_df, _batch_id: int) -> None:
        rows = batch_df.collect()
        if not rows:
            return

        import requests

        for row in rows:
            payload = row.asDict()
            requests.put(
                f"{es_url}/articles/_doc/{payload['id']}",
                json=payload,
                timeout=10,
            ).raise_for_status()

    es_query = (
        parsed.writeStream.outputMode("append")
        .foreachBatch(write_to_elasticsearch)
        .option("checkpointLocation", checkpoint + "_es")
        .start()
    )

    spark.streams.awaitAnyTermination()
    parquet_query.stop()
    es_query.stop()


if __name__ == "__main__":
    main()
