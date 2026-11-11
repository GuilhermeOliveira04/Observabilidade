import hashlib
import logging
import os
import random
import time
from contextlib import contextmanager

import httpx
import pyroscope
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from opentelemetry import metrics, trace
from opentelemetry._logs import set_logger_provider
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.instrumentation.logging import LoggingInstrumentor
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.view import ExplicitBucketHistogramAggregation, View
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import Status, StatusCode
from prometheus_client import start_http_server
from pyroscope.otel import PyroscopeSpanProcessor

SCENARIOS = ("fast", "io", "db", "cpu")
SCENARIO_WEIGHTS = (0.4, 0.2, 0.2, 0.2)
FRAUD_SLA_SECONDS = 0.8
SERVICE_NAME = os.getenv("OTEL_SERVICE_NAME", "checkout")
PAYMENT_URL = os.getenv("PAYMENT_URL", "http://payment:8000/payment")
TRACES_ENDPOINT = os.getenv(
    "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "http://tempo:4318/v1/traces"
)
LOGS_ENDPOINT = os.getenv(
    "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", "http://loki:3100/otlp/v1/logs"
)
PYROSCOPE_SERVER = os.getenv("PYROSCOPE_SERVER_ADDRESS", "http://pyroscope:4040")
ENVIRONMENT = os.getenv("DEPLOYMENT_ENVIRONMENT", "demo")
METRICS_PORT = int(os.getenv("METRICS_PORT", "9464"))

logger = logging.getLogger("shop")


class TraceContextFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        trace_id = getattr(record, "otelTraceID", "")
        span_id = getattr(record, "otelSpanID", "")
        span_context = trace.get_current_span().get_span_context()
        if span_context.is_valid:
            trace_id = format(span_context.trace_id, "032x")
            span_id = format(span_context.span_id, "016x")
            record.otelTraceID = trace_id
            record.otelSpanID = span_id
        if trace_id and set(str(trace_id)) != {"0"} and "trace_id=" not in record.getMessage():
            record.msg = f"{record.getMessage()} trace_id={trace_id} span_id={span_id}"
            record.args = ()
        return True


def configure_logging(logger_provider: LoggerProvider) -> None:
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers.clear()
    root.addFilter(TraceContextFilter())
    logging.getLogger("shop").addFilter(TraceContextFilter())

    stream = logging.StreamHandler()
    stream.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    root.addHandler(stream)
    root.addHandler(LoggingHandler(level=logging.INFO, logger_provider=logger_provider))

    otel_logger = logging.getLogger("opentelemetry")
    otel_logger.setLevel(logging.WARNING)
    otel_logger.propagate = False
    if not otel_logger.handlers:
        otel_logger.addHandler(logging.StreamHandler())


def setup_telemetry() -> None:
    resource = Resource.create(
        {
            "service.name": SERVICE_NAME,
            "deployment.environment": ENVIRONMENT,
        }
    )

    logger_provider = LoggerProvider(resource=resource)
    logger_provider.add_log_record_processor(
        BatchLogRecordProcessor(
            OTLPLogExporter(endpoint=LOGS_ENDPOINT),
            schedule_delay_millis=1000,
        )
    )
    set_logger_provider(logger_provider)
    configure_logging(logger_provider)
    LoggingInstrumentor().instrument(set_logging_format=False)

    try:
        started = pyroscope.configure(
            application_name=SERVICE_NAME,
            server_address=PYROSCOPE_SERVER,
            oncpu=True,
            gil_only=True,
            sample_rate=100,
            tags={"environment": ENVIRONMENT},
        )
        if started is False:
            logger.error("Pyroscope agent did not start")
    except Exception:
        logger.exception("Pyroscope configuration failed")

    tracer_provider = TracerProvider(resource=resource)
    tracer_provider.add_span_processor(PyroscopeSpanProcessor())
    tracer_provider.add_span_processor(
        BatchSpanProcessor(
            OTLPSpanExporter(endpoint=TRACES_ENDPOINT),
            schedule_delay_millis=1000,
        )
    )
    trace.set_tracer_provider(tracer_provider)

    start_http_server(port=METRICS_PORT, addr="0.0.0.0")
    meter_provider = MeterProvider(
        resource=resource,
        metric_readers=[PrometheusMetricReader()],
        views=[
            View(
                instrument_name="http.server.request.duration",
                aggregation=ExplicitBucketHistogramAggregation(
                    boundaries=(0.05, 0.1, 0.2, 0.5, 0.8, 1.0, 1.5, 2.0, 3.0, 5.0)
                ),
            )
        ],
    )
    metrics.set_meter_provider(meter_provider)
    HTTPXClientInstrumentor().instrument()


setup_telemetry()
meter = metrics.get_meter("shop")
request_counter = meter.create_counter(
    name="http.server.requests",
    unit="1",
    description="HTTP requests processed by the shop API",
)
duration_histogram = meter.create_histogram(
    name="http.server.request.duration",
    unit="s",
    description="HTTP request duration",
)
payment_client = httpx.Client(timeout=httpx.Timeout(10.0, connect=2.0))
app = FastAPI(title="Shop observability demo")


def resolve_scenario(requested: str | None) -> str:
    if requested in SCENARIOS:
        return requested
    return random.choices(SCENARIOS, weights=SCENARIO_WEIGHTS, k=1)[0]


def record_request(
    method: str, route: str, status_code: int, scenario: str, duration_s: float
) -> None:
    attributes = {
        "method": method,
        "route": route,
        "status_code": str(status_code),
        "scenario": scenario,
    }
    request_counter.add(1, attributes)
    duration_histogram.record(duration_s, attributes)


@contextmanager
def bind_worker_profile(span=None):
    """Tag the worker thread that actually runs the sync endpoint.

    The server span starts on the event loop, while the route body runs in a
    threadpool. Pyroscope samples that worker, so the span id has to be
    attached there for trace-to-profile links.
    """
    current = span or trace.get_current_span()
    span_context = current.get_span_context()
    if not span_context.is_valid:
        yield current
        return
    span_id = format(span_context.span_id, "016x")
    trace_id = format(span_context.trace_id, "032x")
    current.set_attribute("pyroscope.profile.id", span_id)
    pyroscope.add_thread_tag("span_id", span_id)
    pyroscope.add_thread_tag("trace_id", trace_id)
    try:
        yield current
    finally:
        pyroscope.remove_thread_tag("span_id", span_id)
        pyroscope.remove_thread_tag("trace_id", trace_id)


@contextmanager
def profile_scenario(scenario: str):
    pyroscope.add_thread_tag("scenario", scenario)
    try:
        yield
    finally:
        pyroscope.remove_thread_tag("scenario", scenario)


def run_heavy_fraud_check(duration_s: float) -> None:
    deadline = time.perf_counter() + duration_s
    state = 0x12345678
    while time.perf_counter() < deadline:
        for _ in range(4000):
            state = (state * 1664525 + 1013904223) & 0xFFFFFFFF
            state ^= (state << 13) & 0xFFFFFFFF
            state ^= (state >> 17) & 0xFFFFFFFF
            state ^= (state << 5) & 0xFFFFFFFF
        hashlib.sha256(state.to_bytes(4, "little")).digest()


def run_payment(scenario: str) -> tuple[int, dict]:
    tracer = trace.get_tracer("shop")
    log_extra = {"scenario": scenario}
    if scenario == "fast":
        with tracer.start_as_current_span("payment.fast") as span:
            span.set_attribute("payment.scenario", scenario)
            time.sleep(random.uniform(0.05, 0.2))
        logger.info("Payment processed successfully", extra=log_extra)
        return 200, {"status": "paid", "scenario": scenario}

    if scenario == "io":
        with tracer.start_as_current_span("payment.io") as span:
            span.set_attribute("payment.scenario", scenario)
            time.sleep(random.uniform(1.5, 3.0))
        logger.warning("Payment gateway high latency detected", extra=log_extra)
        return 200, {"status": "paid", "scenario": scenario}

    if scenario == "db":
        with tracer.start_as_current_span("payment.db") as span:
            span.set_attribute("payment.scenario", scenario)
            time.sleep(random.uniform(0.5, 1.5))
            error = TimeoutError("Database connection timeout - pool exhausted")
            span.record_exception(error)
            span.set_status(Status(StatusCode.ERROR, str(error)))
        logger.error("Database connection timeout - pool exhausted", extra=log_extra)
        raise HTTPException(
            status_code=500,
            detail="Database connection timeout - pool exhausted",
        )

    budget = random.uniform(0.5, 1.0)
    with tracer.start_as_current_span("run_heavy_fraud_check") as span:
        span.set_attribute("payment.scenario", scenario)
        with bind_worker_profile(span):
            started = time.perf_counter()
            run_heavy_fraud_check(budget)
            elapsed = time.perf_counter() - started
        span.set_attribute("fraud.cpu_seconds", elapsed)
        if elapsed > FRAUD_SLA_SECONDS:
            span.set_status(Status(StatusCode.ERROR, "Fraud check exceeded CPU budget"))
            logger.error("Fraud check exceeded CPU budget", extra=log_extra)
            raise HTTPException(status_code=500, detail="Fraud check exceeded CPU budget")
    logger.info("Fraud check completed", extra=log_extra)
    return 200, {
        "status": "paid",
        "scenario": scenario,
        "elapsed_seconds": round(elapsed, 3),
    }


def error_detail(response: httpx.Response) -> str:
    try:
        body = response.json()
    except ValueError:
        return response.text or "Payment failed"
    detail = body.get("detail", "Payment failed")
    if isinstance(detail, str):
        return detail
    return str(detail)


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.post("/payment")
def payment(scenario: str | None = Query(default=None)) -> JSONResponse:
    started = time.perf_counter()
    status_code = 200
    chosen = "unknown"
    try:
        with bind_worker_profile():
            chosen = resolve_scenario(scenario)
            current = trace.get_current_span()
            current.set_attribute("payment.scenario", chosen)
            with profile_scenario(chosen):
                status_code, body = run_payment(chosen)
            return JSONResponse(
                content=body,
                headers={"X-Payment-Scenario": chosen},
            )
    except HTTPException as exc:
        status_code = exc.status_code
        headers = dict(exc.headers or {})
        headers["X-Payment-Scenario"] = chosen
        exc.headers = headers
        raise
    finally:
        record_request(
            "POST", "/payment", status_code, chosen, time.perf_counter() - started
        )


@app.post("/checkout")
def checkout(scenario: str | None = Query(default=None)) -> dict:
    started = time.perf_counter()
    status_code = 200
    chosen = scenario if scenario in SCENARIOS else "random"
    try:
        with bind_worker_profile():
            current = trace.get_current_span()
            current.set_attribute("payment.scenario", chosen)
            logger.info("Checkout started")
            params = {"scenario": scenario} if scenario in SCENARIOS else None
            try:
                response = payment_client.post(PAYMENT_URL, params=params)
            except httpx.HTTPError as exc:
                status_code = 502
                current.set_status(Status(StatusCode.ERROR, "Payment service unavailable"))
                logger.error("Payment failed: %s", exc)
                raise HTTPException(
                    status_code=502, detail="Payment service unavailable"
                ) from exc

            current.set_attribute("payment.http.status_code", response.status_code)
            chosen = response.headers.get("X-Payment-Scenario", chosen)
            current.set_attribute("payment.scenario", chosen)
            if response.status_code >= 500:
                detail = error_detail(response)
                status_code = 502
                current.set_status(Status(StatusCode.ERROR, detail))
                logger.error("Payment failed: %s", detail, extra={"scenario": chosen})
                raise HTTPException(status_code=502, detail=detail)

            body = response.json()
            chosen = str(body.get("scenario", chosen))
            current.set_attribute("payment.scenario", chosen)
            logger.info("Checkout completed scenario=%s", chosen, extra={"scenario": chosen})
            return {"status": "accepted", "payment": body}
    except HTTPException as exc:
        status_code = exc.status_code
        raise
    finally:
        record_request(
            "POST", "/checkout", status_code, chosen, time.perf_counter() - started
        )


FastAPIInstrumentor.instrument_app(app, excluded_urls="health")
