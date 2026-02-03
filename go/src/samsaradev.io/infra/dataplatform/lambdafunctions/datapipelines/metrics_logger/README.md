The `metrics_logger` lambda is now deprecated. It was responsible for logging data pipeline success/failure metrics 
to datadog. This is now being done in databricksjobmetricsworker.

Currently, the metrics_logger remains as a no-op lambda function and the work to completely remove the 
`metrics_logger` from the data pipelines framework will be done in the future.
