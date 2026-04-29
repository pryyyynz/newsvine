import json
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    current_timestamp,
    exp,
    from_json,
    lit,
    sum as spark_sum,
    unix_timestamp,
    window,
)
from pyspark.sql.types import StringType, StructField, StructType


def build_spark() -> SparkSession:
    return SparkSession.builder.appName("newsvine-trending-scorer").getOrCreate()


def main() -> None:
    bootstrap = os.getenv("SPARK_KAFKA_BOOTSTRAP", "kafka:29092")
    input_topic = os.getenv("SPARK_EVENTS_TOPIC", "user-interactions")
    output_topic = os.getenv("SPARK_TRENDING_TOPIC", "trending-updates")
    checkpoint = os.getenv("SPARK_CHECKPOINT", "/tmp/checkpoints/newsvine_trending")
    redis_url = os.getenv("SPARK_REDIS_URL", "redis://redis:6379/0")
    decay_seconds = float(os.getenv("SPARK_TRENDING_DECAY_SECONDS", "3600"))
    top_n = int(os.getenv("SPARK_TRENDING_TOP_N", "50"))

    schema = StructType(
        [
            StructField("event_id", StringType()),
            StructField("event_type", StringType()),
            StructField("article_id", StringType()),
            StructField("user_id", StringType()),
            StructField("country", StringType()),
            StructField("topic", StringType()),
            StructField("timestamp", StringType()),
        ]
    )

    spark = build_spark()

    raw_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", input_topic)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed = (
        raw_df.select(from_json(col("value").cast("string"), schema).alias("event"))
        .select("event.*")
        .where(col("article_id").isNotNull())
        .withColumn("country", coalesce(col("country"), lit("global")))
        .withColumn("event_ts", coalesce(col("timestamp").cast("timestamp"), current_timestamp()))
        .withColumn("age_seconds", unix_timestamp(current_timestamp()) - unix_timestamp(col("event_ts")))
        .withColumn("decay", exp(-col("age_seconds") / lit(decay_seconds)))
    )

    weighted = (
        parsed.withWatermark("event_ts", "2 hours")
        .groupBy(window(col("event_ts"), "1 hour", "1 minute"), col("article_id"), col("country"))
        .agg(spark_sum(col("decay")).alias("score"))
    )

    def write_batch(batch_df, _batch_id: int) -> None:
        rows = batch_df.collect()
        if not rows:
            return

        import redis
        from kafka import KafkaProducer

        client = redis.from_url(redis_url, decode_responses=True)
        producer = KafkaProducer(
            bootstrap_servers=bootstrap,
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        )

        global_scores: dict[str, float] = {}

        for row in rows:
            article_id = row["article_id"]
            country = str(row["country"] or "global").lower()
            score = float(row["score"] or 0.0)

            client.zadd("trending:global", {article_id: score})
            client.zadd(f"trending:country:{country}", {article_id: score})
            global_scores[article_id] = max(global_scores.get(article_id, 0.0), score)

        top_ids = [
            member
            for member, _score in sorted(global_scores.items(), key=lambda kv: kv[1], reverse=True)[:top_n]
        ]
        producer.send(
            output_topic,
            {
                "timestamp": str(batch_df.sparkSession.sql("SELECT current_timestamp()").collect()[0][0]),
                "article_ids": top_ids,
            },
        )
        producer.flush(timeout=10)
        producer.close()

    query = (
        weighted.writeStream.outputMode("update")
        .foreachBatch(write_batch)
        .option("checkpointLocation", checkpoint)
        .start()
    )

    spark.streams.awaitAnyTermination()
    query.stop()


if __name__ == "__main__":
    main()
