import os
from datetime import datetime

import mlflow
from mlflow.tracking import MlflowClient
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import StringIndexer
from pyspark.ml.recommendation import ALS
from pyspark.mllib.evaluation import RankingMetrics
from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_set, col, current_timestamp, explode, lit


def build_spark() -> SparkSession:
    return SparkSession.builder.appName("newsvine-als-training-batch").getOrCreate()


def load_matrix(spark: SparkSession):
    backend = os.getenv("PHASE5_SOURCE_BACKEND", "local_postgres").strip().lower()

    if backend == "bigquery":
        table = os.getenv(
            "PHASE5_BQ_MATRIX_TABLE",
            "news_features.int_user_article_matrix",
        )
        return spark.read.format("bigquery").option("table", table).load()

    jdbc_url = os.getenv("PHASE5_POSTGRES_JDBC_URL", "jdbc:postgresql://postgres:5432/users")
    jdbc_user = os.getenv("PHASE5_POSTGRES_USER", "newsvine")
    jdbc_password = os.getenv("PHASE5_POSTGRES_PASSWORD", "newsvine")
    matrix_table = os.getenv("PHASE5_MATRIX_TABLE", "intermediate.int_user_article_matrix")

    return (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", matrix_table)
        .option("user", jdbc_user)
        .option("password", jdbc_password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )


def _index_lookup(spark: SparkSession, labels: list[str], *, index_col: str, value_col: str):
    rows = [(float(idx), value) for idx, value in enumerate(labels)]
    return spark.createDataFrame(rows, [index_col, value_col])


def evaluate_ndcg(model, validation_df, *, k: int) -> float:
    truth_rows = (
        validation_df.groupBy("user_idx")
        .agg(collect_set("article_idx").alias("truth_items"))
        .collect()
    )
    truth_map = {
        float(row["user_idx"]): [float(item) for item in row["truth_items"]]
        for row in truth_rows
        if row["truth_items"]
    }
    if not truth_map:
        return 0.0

    candidate_users = validation_df.select("user_idx").distinct()
    recommended_rows = model.recommendForUserSubset(candidate_users, k).collect()

    ranking_input: list[tuple[list[float], list[float]]] = []
    for row in recommended_rows:
        user_idx = float(row["user_idx"])
        truth = truth_map.get(user_idx)
        if not truth:
            continue
        predictions = [
            float(rec["article_idx"])
            for rec in row["recommendations"]
        ]
        ranking_input.append((predictions, truth))

    if not ranking_input:
        return 0.0

    metrics = RankingMetrics(validation_df.sparkSession.sparkContext.parallelize(ranking_input))
    return float(metrics.ndcgAt(k))


def _best_production_metric(client: MlflowClient, model_name: str, metric_key: str) -> float | None:
    best: float | None = None
    versions = client.search_model_versions(f"name='{model_name}'")
    for version in versions:
        stage = str(getattr(version, "current_stage", "")).lower()
        if stage != "production":
            continue

        run_id = getattr(version, "run_id", None)
        if not run_id:
            continue

        run = client.get_run(run_id)
        value = run.data.metrics.get(metric_key)
        if value is None:
            continue

        if best is None or value > best:
            best = float(value)

    return best


def main() -> None:
    spark = build_spark()

    rank = int(os.getenv("PHASE5_ALS_RANK", "50"))
    max_iter = int(os.getenv("PHASE5_ALS_MAX_ITER", "20"))
    reg_param = float(os.getenv("PHASE5_ALS_REG_PARAM", "0.1"))
    top_k = int(os.getenv("PHASE5_NDCG_K", "20"))

    matrix_df = load_matrix(spark)
    matrix_df = matrix_df.select("user_id", "article_id", col("rating").cast("double")).na.drop()

    user_indexer = StringIndexer(
        inputCol="user_id",
        outputCol="user_idx",
        handleInvalid="skip",
    )
    article_indexer = StringIndexer(
        inputCol="article_id",
        outputCol="article_idx",
        handleInvalid="skip",
    )

    user_indexer_model = user_indexer.fit(matrix_df)
    indexed = user_indexer_model.transform(matrix_df)
    article_indexer_model = article_indexer.fit(indexed)
    indexed = article_indexer_model.transform(indexed)

    train_df, validation_df = indexed.randomSplit([0.8, 0.2], seed=42)

    als = ALS(
        userCol="user_idx",
        itemCol="article_idx",
        ratingCol="rating",
        rank=rank,
        maxIter=max_iter,
        regParam=reg_param,
        coldStartStrategy="drop",
        nonnegative=True,
    )
    model = als.fit(train_df)

    predictions = model.transform(validation_df).na.drop(subset=["prediction"])
    rmse = float(
        RegressionEvaluator(
            metricName="rmse",
            labelCol="rating",
            predictionCol="prediction",
        ).evaluate(predictions)
    )
    ndcg_at_k = evaluate_ndcg(model, validation_df, k=top_k)

    user_lookup = _index_lookup(
        spark,
        user_indexer_model.labels,
        index_col="user_idx",
        value_col="user_id",
    )
    article_lookup = _index_lookup(
        spark,
        article_indexer_model.labels,
        index_col="article_idx",
        value_col="article_id",
    )

    user_recs = model.recommendForAllUsers(top_k)
    exploded = (
        user_recs.select(col("user_idx"), explode("recommendations").alias("rec"))
        .select(
            col("user_idx"),
            col("rec.article_idx").alias("article_idx"),
            col("rec.rating").alias("score"),
        )
        .join(user_lookup, on="user_idx", how="left")
        .join(article_lookup, on="article_idx", how="left")
        .select("user_id", "article_id", col("score").cast("double"))
        .where(col("user_id").isNotNull() & col("article_id").isNotNull())
    )

    tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
    experiment = os.getenv("PHASE5_MLFLOW_EXPERIMENT", "phase5-als")
    registered_model = os.getenv("PHASE5_ALS_REGISTERED_MODEL", "newsvine-als")

    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(experiment)

    with mlflow.start_run(run_name="als-batch-training") as run:
        run_id = run.info.run_id
        mlflow.log_param("rank", rank)
        mlflow.log_param("max_iter", max_iter)
        mlflow.log_param("reg_param", reg_param)
        mlflow.log_param("top_k", top_k)
        mlflow.log_metric("rmse", rmse)
        mlflow.log_metric("ndcg_at_k", ndcg_at_k)

        model_artifact_path = os.getenv(
            "PHASE5_ALS_MODEL_PATH",
            f"/tmp/models/als/{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",
        )
        model.write().overwrite().save(model_artifact_path)
        mlflow.log_param("model_path", model_artifact_path)

        mlflow.spark.log_model(model, artifact_path="als_model")

        model_uri = f"runs:/{run_id}/als_model"
        registered = mlflow.register_model(model_uri=model_uri, name=registered_model)

        client = MlflowClient(tracking_uri=tracking_uri)
        production_best = _best_production_metric(client, registered_model, "ndcg_at_k")
        if production_best is None or ndcg_at_k > production_best:
            client.transition_model_version_stage(
                name=registered_model,
                version=registered.version,
                stage="Production",
                archive_existing_versions=True,
            )

        output_table = os.getenv(
            "PHASE5_ALS_RECOMMENDATIONS_TABLE",
            "news_features.als_user_recommendations",
        )
        jdbc_url = os.getenv("PHASE5_POSTGRES_JDBC_URL", "jdbc:postgresql://postgres:5432/users")
        jdbc_user = os.getenv("PHASE5_POSTGRES_USER", "newsvine")
        jdbc_password = os.getenv("PHASE5_POSTGRES_PASSWORD", "newsvine")

        export_df = exploded.withColumn("model_run_id", lit(run_id)).withColumn(
            "refreshed_at",
            current_timestamp(),
        )

        (
            export_df.write.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", output_table)
            .option("user", jdbc_user)
            .option("password", jdbc_password)
            .option("driver", "org.postgresql.Driver")
            .option("truncate", "true")
            .mode("overwrite")
            .save()
        )

    print(f"ALS training completed: rmse={rmse:.4f}, ndcg@{top_k}={ndcg_at_k:.4f}")


if __name__ == "__main__":
    main()
