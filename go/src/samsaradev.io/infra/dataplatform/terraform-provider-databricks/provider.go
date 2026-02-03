package main

import (
	"github.com/hashicorp/terraform-plugin-sdk/plugin"

	"samsaradev.io/infra/dataplatform/terraform-provider-databricks/databricks"
)

func main() {
	plugin.Serve(&plugin.ServeOpts{ProviderFunc: databricks.Provider})
}
