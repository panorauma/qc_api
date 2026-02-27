# QC Tool API

QC Tool API server + test client.

## Server

Run with `python3 app.py`

### Environment variables

Create .env file with following content.

```env
LOG_LEVEL="DEBUG"
OTEL="123"
```

LOG_LEVEL accepts: DEBUG, INFO, WARNING, ERROR. I use DEBUG for development and testing. INFO when deployed.

OTEL is OpenTelemetry logging DSN. Use obvious placeholder like 123 if you don't need to store logs. Accepts any string except empty string.

## Client

Run checks without server. `python3 local_client.py`

Run checks with server. `python3 api_client.py` Server needs to be running.

JavaScript client example code in `checks_client/js_example`

Test dataset is csv file in tidy format.

## Note

Logging with opentelemetry is optional. I use it mostly for debugging. Package still needs to be installed. Removing all logging is not a priority at the moment.
