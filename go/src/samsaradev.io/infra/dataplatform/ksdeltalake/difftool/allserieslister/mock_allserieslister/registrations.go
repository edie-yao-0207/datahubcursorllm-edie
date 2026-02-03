package mock_allserieslister

import (
	"samsaradev.io/infra/dataplatform/ksdeltalake/difftool/allserieslister"
	"samsaradev.io/infra/fxregistry"
)

func init() {
	fxregistry.MustRegisterOverrideTestConstructor(fxregistry.Override{
		Constructor: NewMockAllSeriesListerClient,
		InterfaceConstructor: func(mock *MockAllSeriesListerClient) allserieslister.AllSeriesListerClient {
			return mock
		},
	})
}
