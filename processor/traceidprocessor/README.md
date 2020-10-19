# TraceID processor

Replaces the high order 32 bits of the `TraceID` to contain the epoch value in seconds based on the start time of the span if available, otherwise takes the current Unix epoch time. The computed epoch value of the first span with a `TraceID` is cached for subsequent spans with the same `TraceID` to make sure they end up in the same trace.

The following settings are required:

- `cache_endpoint`: contains the endpoint of the cache implementation. Cache provider uses the URL scheme to switch between local map imeplementation or Redis cache. For local map use `local://` as the endpoint (**_For local testing only. Not for production use._**). For Redis cache use `redis://<host>:<port>`.
- `ttl` : contains the time to live in seconds for the computed epoch value. Only used for Redis provider.

Example:

```yaml
receivers:
  zipkin:

processors:
  traceid:
    cache_endpoint: redis://localhost:6379
    ttl: 604800

exporters:
  awsxray:

service:
  pipelines:
    traces:  # a pipeline of “traces” type
      receivers: [zipkin]
      processors: [traceid]
      exporters: [awsxray]
```

The full list of settings exposed for this processor are documented [here](./config.go) with detailed sample configuration [here](./testdata/config.yaml).
