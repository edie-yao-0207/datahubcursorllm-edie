package mock_godatalakeclient

import (
	"samsaradev.io/infra/dataplatform/godatalake"
	"samsaradev.io/infra/fxregistry"
)

func init() {
	fxregistry.MustRegisterOverrideTestConstructor(fxregistry.Override{
		Constructor: NewMockClient,
		InterfaceConstructor: func(c *MockClient) godatalake.Client {
			return c
		},
	})

	fxregistry.MustRegisterOverrideTestConstructor(fxregistry.Override{
		Constructor: NewMockResult,
		InterfaceConstructor: func(r *MockResult) godatalake.Result {
			return r
		},
	})
}
