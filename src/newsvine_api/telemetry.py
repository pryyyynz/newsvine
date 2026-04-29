import logging
import importlib

from fastapi import FastAPI

from newsvine_api.config import get_settings

LOGGER = logging.getLogger("newsvine.telemetry")


def configure_telemetry(app: FastAPI) -> None:
    settings = get_settings()
    if not settings.otel_enabled:
        return

    try:
        trace = importlib.import_module("opentelemetry.trace")
        OTLPSpanExporter = getattr(
            importlib.import_module("opentelemetry.exporter.otlp.proto.http.trace_exporter"),
            "OTLPSpanExporter",
        )
        FastAPIInstrumentor = getattr(
            importlib.import_module("opentelemetry.instrumentation.fastapi"),
            "FastAPIInstrumentor",
        )
        RequestsInstrumentor = getattr(
            importlib.import_module("opentelemetry.instrumentation.requests"),
            "RequestsInstrumentor",
        )
        Resource = getattr(importlib.import_module("opentelemetry.sdk.resources"), "Resource")
        TracerProvider = getattr(importlib.import_module("opentelemetry.sdk.trace"), "TracerProvider")
        BatchSpanProcessor = getattr(
            importlib.import_module("opentelemetry.sdk.trace.export"),
            "BatchSpanProcessor",
        )
    except Exception:
        LOGGER.warning("OpenTelemetry dependencies are not installed; skipping tracing setup")
        return

    service_name = settings.otel_service_name
    otlp_endpoint = settings.otel_exporter_otlp_endpoint

    resource = Resource.create({"service.name": service_name})
    provider = TracerProvider(resource=resource)
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=otlp_endpoint)))
    trace.set_tracer_provider(provider)

    FastAPIInstrumentor.instrument_app(app)
    RequestsInstrumentor().instrument()
