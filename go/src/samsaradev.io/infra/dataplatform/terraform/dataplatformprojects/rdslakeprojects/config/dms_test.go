package config_test

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"samsaradev.io/infra/app/generate_terraform/awsresource"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/rdslakeprojects/config"
)

func TestTaskSettingsRemarshal(t *testing.T) {
	marshalled, err := json.Marshal(config.DesiredDMSReplicationTaskSettings)
	require.NoError(t, err)

	decoder := json.NewDecoder(bytes.NewReader(marshalled))
	decoder.DisallowUnknownFields()

	var awsresTaskSettings awsresource.DMSReplicationTaskSettings
	err = decoder.Decode(&awsresTaskSettings)
	require.NoError(t, err)
}
