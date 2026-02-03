package datapipelineerrors

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetErrorClassification(t *testing.T) {
	testCases := map[string]struct {
		errorMsg               string
		expectedClassification string
	}{
		"null-primary-keys": {
			errorMsg:               "NULL primary keys found",
			expectedClassification: "null_primary_keys",
		},
		"null-primary-keys-uppercase": {
			errorMsg:               "NULL PRIMARY KEYS FOUND",
			expectedClassification: "null_primary_keys",
		},
		"insufficient-instance-capacity-1": {
			errorMsg:               "DATABRICKS ERROR -->  AWS_INSUFFICIENT_INSTANCE_CAPACITY_FAILURE",
			expectedClassification: "insufficient_instance_capacity",
		},
		"error-that-does-not-exist": {
			errorMsg:               "none classified error",
			expectedClassification: "unknown",
		},
		"bootstrap-failure": {
			errorMsg:               "Unexpected failure while waiting for the cluster (0222-031506-q4ne89qv) to be ready.Cause Unexpected state for cluster (0222-031506-q4ne89qv): SELF_BOOTSTRAP_FAILURE(SUCCESS): databricks_error_message:Self-bootstrap failure during launch. Please try again later and contact Databricks if the problem persists. VM self bootstrap failed [GetRunbook] Failure message (may be truncated): [Failed to get runbook (may retry). Status code: 400. Content: {\"error_code\":\"INVALID_STATE\",\"message\":\"[workerEnv=WorkerEnvId(workerenv-5924096274798303-12facf0b-488c-442e-ae98-7ba18dcbde4b)]Instance InstanceId(i-0bf74df215c44d0a1) is not ready for generating runbook, [id: InstanceId(i-0bf74df215c44d0a1), status: INSTANCE_LAUNCHING, workerEnvId:WorkerEnvId(workerenv-5924096274798303-12facf0b-488c-442e-ae98-7ba18dcbde4b), lastStatusChangeTime: 1645499759009, groupIdOpt Some(-7812120757689538746]",
			expectedClassification: "bootstrap_failure",
		},
		"bootstrap-timeout": {
			errorMsg:               "Unexpected failure while waiting for the cluster (0228-033947-sse45up7) to be ready.Cause Unexpected state for cluster (0228-033947-sse45up7): BOOTSTRAP_TIMEOUT(SUCCESS): databricks_error_message:[id: InstanceId(i-0d43132ac4b1ce991), status: INSTANCE_INITIALIZING, workerEnvId:WorkerEnvId(workerenv-5924096274798303-12facf0b-488c-442e-ae98-7ba18dcbde4b), lastStatusChangeTime: 1646019697575, groupIdOpt Some(-8002829507207647788),requestIdOpt Some(0228-033947-sse45up7-9c3f784f-4860-4c0f-8),version 0] with threshold 700 seconds timed out after 703532 milliseconds. Please check network connectivity from the data plane to the control plane.,instance_id:i-0d43132ac4b1ce991",
			expectedClassification: "bootstrap_timeout",
		},
		"cluster-startup-catch-all": {
			errorMsg:               "Unexpected failure while waiting for the cluster (0427-011727-a9iba2kb) to be ready.Cause Unexpected state for cluster (0427-011727-a9iba2kb): INSTANCE_POOL_CLUSTER_FAILURE(CLIENT_ERROR): instance_id:i-0b0a4a23bde4eaebe,databricks_error_message:Failed to update InstanceProfile for instance i-0b0a4a23bde4eaebe. Please verify that the AWS account used to set up your Databricks workspace includes the `ec2:AssociateIamInstanceProfile`, `ec2:DescribeIamInstanceProfileAssociations`, `ec2:DisassociateIamInstanceProfile` and `ec2:ReplaceIamInstanceProfileAssociation` actions in its access policy.",
			expectedClassification: "cluster_startup_error_catch_all",
		},
		"schema-mismatch": {
			errorMsg:               "AnalysisException: A schema mismatch detected when writing to the Delta table (Table ID: 6994ced1-86b7-47ba-9562-69008bc604c4)",
			expectedClassification: "schema_mismatch",
		},
		"datadog-outage": {
			errorMsg:               "Datadog returned a bad HTTP response code: 503 - Service Unavailable. Please try again later. If the problem persists, please contact support@datadoghq.com",
			expectedClassification: "datadog_unavailable",
		},
	}

	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, testCase.expectedClassification, GetErrorClassification(testCase.errorMsg).ErrorType)
		})
	}
}
