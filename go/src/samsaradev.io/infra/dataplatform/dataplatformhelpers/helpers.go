package dataplatformhelpers

import (
	"fmt"
	"hash/fnv"
	"os"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/libs/ni/infraconsts"
)

// Contains helper checks if the string is present in a slice.
func Contains(items []string, target string) bool {
	for _, key := range items {
		if key == target {
			return true
		}
	}
	return false
}

// GetSupportedAzs returns valid az accepted by VPC define given a region.
func GetSupportedAzs(azs []string) ([]string, error) {
	validAZs := []string{"a", "b", "c", "d"}
	supportedAzs := []string{}

	for _, az := range azs {
		if az == "" || !Contains(validAZs, az[len(az)-1:]) {
			return nil, fmt.Errorf("invalid az option: %v", az)
		}
		supportedAzs = append(supportedAzs, az[len(az)-1:])
	}

	return supportedAzs, nil
}

// GetAWSSession gets an aws session for the dataplatformadmin role
func GetAWSSession(region string) *session.Session {
	profile := "dataplatformadmin"
	if region == infraconsts.SamsaraAWSEURegion {
		profile = "eu-dataplatformadmin"
	} else if region == infraconsts.SamsaraAWSCARegion {
		profile = "ca-dataplatformadmin"
	}
	awsSession := session.Must(session.NewSessionWithOptions(session.Options{
		Profile: profile,
	}))

	return awsSession
}

// GetDatabricksClient gets a awssession in the Databricks account
func GetDatabricksAWSSession(region string) *session.Session {
	profile := "databricks-superadmin"
	if region == infraconsts.SamsaraAWSEURegion {
		profile = "eu-databricks-superadmin"
	} else if region == infraconsts.SamsaraAWSCARegion {
		profile = "ca-databricks-superadmin"
	}
	awsSession := session.Must(session.NewSessionWithOptions(session.Options{
		Profile: profile,
	}))

	return awsSession
}

// GetDatabricksE2Client gets a databricks client in the E2 workspace
func GetDatabricksE2Client(region string) (*databricks.Client, error) {
	url := ""
	tokenVar := ""

	switch region {
	case infraconsts.SamsaraAWSDefaultRegion:
		url = infraconsts.SamsaraDevDatabricksWorkspaceURLBase
		tokenVar = "DATABRICKS_E2_TOKEN"
	case infraconsts.SamsaraAWSEURegion:
		url = infraconsts.SamsaraEUDevDatabricksWorkspaceURLBase
		tokenVar = "DATABRICKS_EU_E2_TOKEN"
	case infraconsts.SamsaraAWSCARegion:
		url = infraconsts.SamsaraCADevDatabricksWorkspaceURLBase
		tokenVar = "DATABRICKS_CA_E2_TOKEN"
	default:
		return nil, oops.Errorf("region %s not supported yet", region)
	}

	databricksToken := os.Getenv(tokenVar)
	if databricksToken == "" {
		return nil, oops.Errorf("$%s does not exist. Please configure.", tokenVar)
	}

	client, err := databricks.New(url, databricksToken)
	if err != nil {
		return nil, oops.Wrapf(err, "Error creating Databricks client")
	}

	return client, nil
}

func Hash(s string) uint32 {
	h := fnv.New32a()
	_, err := h.Write([]byte(s))
	if err != nil {
		return 0
	}
	return h.Sum32()
}

// Rollout changes to clusters/jobs by hashing the name
// maxStage: number of steps for a full roll out
// currStage: current step in the roll out, starts at 0
func Rollout(name string, currStage int, maxStage int) bool {
	val := int(Hash(name)) % maxStage
	return val <= currStage
}

func CombineInterfaceMaps(m1 map[string]interface{}, m2 map[string]interface{}) map[string]interface{} {
	resultMap := make(map[string]interface{}, len(m1)+len(m2))
	for k, v := range m1 {
		resultMap[k] = v
	}

	for k, v := range m2 {
		resultMap[k] = v
	}

	return resultMap
}
