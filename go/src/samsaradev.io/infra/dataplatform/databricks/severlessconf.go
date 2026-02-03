package databricks

// ClientVersion is the client version for the serverless compute.
// This controls what version of DBR is used by the serverless compute.
// Refer to the following documentation for more details: https://docs.databricks.com/aws/en/release-notes/serverless/environment-version/
type ClientVersion string

const (
	ClientVersion1 ClientVersion = "1"
	ClientVersion2 ClientVersion = "2"
	ClientVersion3 ClientVersion = "3"
	ClientVersion4 ClientVersion = "4"
)

const (
	DefaultServerlessEnvironmentKey                  = "DEFAULT_ENVIRONMENT"
	StandardSeverlessJobPerformanceTarget            = "STANDARD"
	PerformaceOptimizedSeverlessJobPerformanceTarget = "PERFORMANCE_OPTIMIZED"
)

type ServerlessEnvironmentSpec struct {
	Client       string   `json:"client"`
	Dependencies []string `json:"dependencies,omitempty"`
}

type ServerlessEnvironment struct {
	EnvironmentKey string                     `json:"environment_key"`
	Spec           *ServerlessEnvironmentSpec `json:"spec,omitempty"`
}

type ServerlessConfig struct {
	PerformanceTarget string
	// Environment is the environment for the table.
	Environments []ServerlessEnvironment

	// EnvironmentKey is the environment key for the table.
	EnvironmentKey string

	// ClientVersionByRegion is the client version for the serverless compute by region.
	// This overrides the default client version for the serverless compute by region.
	ClientVersionByRegionOverride map[string]ClientVersion

	// BudgetPolicyId is the budget policy id for the serverless compute.
	BudgetPolicyId string
}

type ServerlessJobTags struct {
	Key   string
	Value string
}

func (sc *ServerlessConfig) GetEnvironmentKey() string {
	if sc.EnvironmentKey == "" {
		return DefaultServerlessEnvironmentKey
	}
	return sc.EnvironmentKey
}

func (sc *ServerlessConfig) GetPerformanceTarget() string {
	if sc.PerformanceTarget == "" {
		return StandardSeverlessJobPerformanceTarget
	}
	return sc.PerformanceTarget
}

func (sc *ServerlessConfig) GetEnvironments() []*ServerlessEnvironment {
	environments := []*ServerlessEnvironment{}

	for _, env := range sc.Environments {
		environments = append(environments, &ServerlessEnvironment{
			EnvironmentKey: env.EnvironmentKey,
			Spec: &ServerlessEnvironmentSpec{
				Client:       env.Spec.Client,
				Dependencies: env.Spec.Dependencies,
			},
		})
	}

	return environments
}
