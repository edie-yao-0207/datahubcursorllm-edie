package sparktypes

type SparkExecutionEnvironment struct {
	SparkVersion       string            `json:"spark_version"`
	SparkConfOverrides map[string]string `json:"spark_conf_overrides"`
	Libraries          []string          `json:"libraries"`
	S3InitScripts      []string          `json:"s3_init_scripts"`
}

func IsValidSparkExecutionEnvironment(s SparkExecutionEnvironment) (bool, error) {
	// TODO: Add Validation for fields
	return true, nil
}
