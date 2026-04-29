from datetime import datetime, timedelta, timezone
import logging

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.utils.email import send_email

LOGGER = logging.getLogger(__name__)


def _notify_failure(context) -> None:
    dag_id = context.get("dag").dag_id if context.get("dag") else "unknown"
    task_id = context.get("task_instance").task_id if context.get("task_instance") else "unknown"
    run_id = context.get("run_id", "unknown")

    subject = f"[newsvine] DAG failure: {dag_id}.{task_id}"
    body = (
        f"DAG: {dag_id}\n"
        f"Task: {task_id}\n"
        f"Run: {run_id}\n"
        f"Log URL: {context.get('task_instance').log_url if context.get('task_instance') else ''}\n"
    )

    email_targets_raw = Variable.get("BATCH_ALERT_EMAILS", default_var="")
    email_targets = [item.strip() for item in email_targets_raw.split(",") if item.strip()]
    if email_targets:
        send_email(to=email_targets, subject=subject, html_content=body.replace("\n", "<br/>"))

    slack_webhook = Variable.get("BATCH_SLACK_WEBHOOK", default_var="")
    if slack_webhook:
        try:
            import requests

            requests.post(
                slack_webhook,
                json={"text": f"{subject}\n```{body}```"},
                timeout=10,
            ).raise_for_status()
        except Exception:
            LOGGER.exception("Failed to send Slack alert")


def _validate_refresh() -> None:
    import redis

    redis_url = Variable.get("REDIS_URL", default_var="redis://redis:6379/0")
    client = redis.from_url(redis_url, decode_responses=True)

    required_keys = [
        "phase5:last_refresh:user_vectors",
        "phase5:last_refresh:article_embeddings",
        "phase5:last_refresh:als_scores",
    ]
    missing = [key for key in required_keys if not client.get(key)]
    if missing:
        raise RuntimeError(f"Redis refresh markers missing: {missing}")


def _eval_report() -> None:
    import mlflow

    tracking_uri = Variable.get("MLFLOW_TRACKING_URI", default_var="http://mlflow:5000")
    experiment_name = Variable.get("PHASE5_MLFLOW_EXPERIMENT", default_var="phase5-als")

    mlflow.set_tracking_uri(tracking_uri)
    experiment = mlflow.get_experiment_by_name(experiment_name)
    if experiment is None:
        raise RuntimeError(f"MLflow experiment not found: {experiment_name}")

    runs = mlflow.search_runs(
        experiment_ids=[experiment.experiment_id],
        max_results=1,
        order_by=["start_time DESC"],
    )
    if runs.empty:
        raise RuntimeError("No MLflow runs found for evaluation report")

    latest = runs.iloc[0]
    ndcg = latest.get("metrics.ndcg_at_k", None)
    rmse = latest.get("metrics.rmse", None)
    LOGGER.info("Latest phase5 metrics: ndcg_at_k=%s rmse=%s", ndcg, rmse)


def _pod_task(task_id: str, image_var: str, command: str, timeout_minutes: int = 50) -> KubernetesPodOperator:
    return KubernetesPodOperator(
        task_id=task_id,
        name=task_id.replace("_", "-"),
        namespace=Variable.get("BATCH_NAMESPACE", default_var="batch"),
        image=Variable.get(image_var, default_var="ghcr.io/newsvine/placeholder:latest"),
        cmds=["bash", "-lc"],
        arguments=[command],
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
        execution_timeout=timedelta(minutes=timeout_minutes),
    )


default_args = {
    "owner": "newsvine",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
    "on_failure_callback": _notify_failure,
    "sla": timedelta(hours=2),
}

with DAG(
    dag_id="nightly_batch",
    description="Phase 5 nightly local-first batch pipeline",
    schedule="0 2 * * *",
    start_date=datetime(2026, 4, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    default_args=default_args,
    tags=["phase5", "batch"],
) as dag:
    dbt_run = _pod_task(
        task_id="dbt_run",
        image_var="DBT_IMAGE",
        command=(
            "dbt run --project-dir /opt/dbt/newsvine --target ${DBT_TARGET} "
            "&& dbt test --project-dir /opt/dbt/newsvine --target ${DBT_TARGET}"
        ),
    )

    als_training = _pod_task(
        task_id="als_training",
        image_var="SPARK_BATCH_IMAGE",
        command="spark-submit /opt/spark/work-dir/streaming/spark_als_training_batch.py",
        timeout_minutes=70,
    )

    embedding_refresh = _pod_task(
        task_id="embedding_refresh",
        image_var="SPARK_BATCH_IMAGE",
        command="spark-submit /opt/spark/work-dir/streaming/spark_embedding_refresh_batch.py",
        timeout_minutes=70,
    )

    redis_push = _pod_task(
        task_id="redis_push",
        image_var="API_WORKER_IMAGE",
        command=(
            "python -m newsvine_pipeline.phase5_refresh_user_vectors "
            "&& python -m newsvine_pipeline.phase5_refresh_article_embeddings "
            "&& python -m newsvine_pipeline.phase5_refresh_als_scores"
        ),
        timeout_minutes=30,
    )

    verify_redis = PythonOperator(
        task_id="verify_redis",
        python_callable=_validate_refresh,
        execution_timeout=timedelta(minutes=10),
    )

    eval_report = PythonOperator(
        task_id="eval_report",
        python_callable=_eval_report,
        execution_timeout=timedelta(minutes=10),
    )

    dbt_run >> als_training >> embedding_refresh >> redis_push >> verify_redis >> eval_report
