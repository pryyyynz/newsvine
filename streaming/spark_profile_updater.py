import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, from_json, lit
from pyspark.sql.types import StringType, StructField, StructType

SIGNALS = {
    "click": 1.0,
    "like": 2.0,
    "search": 1.5,
    "bookmark": 1.8,
}


def build_spark() -> SparkSession:
    return SparkSession.builder.appName("newsvine-profile-updater").getOrCreate()


def main() -> None:
    bootstrap = os.getenv("SPARK_KAFKA_BOOTSTRAP", "kafka:29092")
    topic = os.getenv("SPARK_EVENTS_TOPIC", "user-interactions")
    checkpoint = os.getenv("SPARK_CHECKPOINT", "/tmp/checkpoints/newsvine_profile")
    redis_url = os.getenv("SPARK_REDIS_URL", "redis://redis:6379/0")

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
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
    )

    events = (
        raw_df.select(from_json(col("value").cast("string"), schema).alias("event"))
        .select("event.*")
        .where(col("user_id").isNotNull())
        .withColumn("topic", coalesce(col("topic"), lit("general")))
    )

    def write_batch(batch_df, _batch_id: int) -> None:
        rows = batch_df.collect()
        if not rows:
            return

        import redis

        client = redis.from_url(redis_url, decode_responses=True)

        for row in rows:
            user_id = row["user_id"]
            article_id = row["article_id"]
            topic_name = str(row["topic"] or "general").lower()
            event_type = str(row["event_type"] or "click")
            signal = float(SIGNALS.get(event_type, 1.0))

            vector_key = f"user:{user_id}:vector"
            old_raw = client.hget(vector_key, topic_name)
            old_value = float(old_raw) if old_raw is not None else 0.0
            new_value = (0.9 * old_value) + (0.1 * signal)
            client.hset(vector_key, topic_name, new_value)

            if article_id:
                history_key = f"user:{user_id}:history"
                client.lpush(history_key, article_id)
                client.ltrim(history_key, 0, 499)

    query = (
        events.writeStream.outputMode("append")
        .foreachBatch(write_batch)
        .option("checkpointLocation", checkpoint)
        .start()
    )

    spark.streams.awaitAnyTermination()
    query.stop()


if __name__ == "__main__":
    main()
