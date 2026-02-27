import os
import logging
import uptrace

_initialized = False


# must be initialized before any logging configuration or logger usage
def setup_otel_logging() -> None:
    global _initialized
    if _initialized:
        return

    dsn_value: str | None = os.getenv("OTEL")
    if not dsn_value:
        logging.warning("OTEL not found in environment. Skipping remote.")
        return

    log_level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_name, logging.INFO)  # INFO by default

    uptrace.configure_opentelemetry(
        dsn=dsn_value,
        service_name="qctapi",
        service_version="0.1.0",
        logging_level=log_level,
    )

    _initialized = True
