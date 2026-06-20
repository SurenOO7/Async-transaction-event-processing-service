from prometheus_client import Counter, Gauge, start_http_server

# Counter("events_processed") exposes the sample "events_processed_total".
events_processed = Counter("events_processed", "Events processed by a worker", ["status"])
events_dead_lettered = Counter("events_dead_lettered", "Events sent to the dead-letter queue")
retry_queue_depth = Gauge("retry_queue_depth", "Current depth of the retry ZSET")


def record_processed(status: str) -> None:
    events_processed.labels(status=status).inc()


def record_dead_lettered() -> None:
    events_dead_lettered.inc()


def set_retry_depth(depth: int) -> None:
    retry_queue_depth.set(depth)


def serve_metrics(port: int) -> None:
    # For the worker processes, which have no HTTP server of their own.
    start_http_server(port)
