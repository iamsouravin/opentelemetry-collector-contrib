# Name processor

Replaces special character in span name.

The following settings are required:

- `from_char`: contains the character to replace.
- `to_char` : contains the substitution character.

Example:

```yaml
receivers:
  zipkin:

processors:
  name:
    from_char: '*'
    to_char: '+'

exporters:
  awsxray:

service:
  pipelines:
    traces:  # a pipeline of “traces” type
      receivers: [zipkin]
      processors: [name]
      exporters: [awsxray]
```

The full list of settings exposed for this processor are documented [here](./config.go) with detailed sample configuration [here](./testdata/config.yaml).
