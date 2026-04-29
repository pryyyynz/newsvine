# Phase 6 Monitoring Stack (Local-First, K8s Migration-Ready)

This folder contains migration-oriented monitoring assets for Kubernetes deployment.

## Helm deployment targets

- Chart: `kube-prometheus-stack`
- Namespace: `monitoring`
- Values file: `orchestration/monitoring/kube-prometheus-values.yaml`

### Example install/upgrade

```bash
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update
helm upgrade --install newsvine-monitoring prometheus-community/kube-prometheus-stack \
  --namespace monitoring --create-namespace \
  -f orchestration/monitoring/kube-prometheus-values.yaml
```

## Dashboards

Import JSON dashboards from `orchestration/monitoring/grafana/`:

- `api-dashboard.json`
- `redis-dashboard.json`
- `kafka-dashboard.json`

## Alert rules

Apply `orchestration/monitoring/prometheus-alert-rules.yaml` as a `PrometheusRule` custom resource.

## Migration notes

- Local: instrumented FastAPI exposes `/metrics`.
- K8s: route metrics through ServiceMonitor and Prometheus scrape config.
- Redis/Kafka metrics assume exporters are deployed in cluster.
- Keep metric names stable between local and cloud to avoid dashboard rewrites.
