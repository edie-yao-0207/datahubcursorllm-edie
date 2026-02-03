package mock_databricks

import (
	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/fxregistry"
)

func init() {
	fxregistry.MustRegisterOverrideTestConstructor(fxregistry.Override{
		Constructor: NewMockAPI,
		InterfaceConstructor: func(client *MockAPI) databricks.API {
			return client
		},
	})
}
