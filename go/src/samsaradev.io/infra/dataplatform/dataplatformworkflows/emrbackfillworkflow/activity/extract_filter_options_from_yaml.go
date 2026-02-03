package activity

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/samsarahq/go/oops"
	"go.uber.org/fx"
	"gopkg.in/yaml.v3"

	"samsaradev.io/helpers/ni/filepathhelpers"
	"samsaradev.io/infra/dataplatform/dataplatformworkflows/emrbackfillworkflow/helpers"
	"samsaradev.io/infra/workflows/workflowregistry"
)

func init() {
	workflowregistry.MustRegisterActivityConstructor(NewExtractFilterOptionsFromYamlActivity)
}

type ExtractFilterOptionsFromYamlActivityArgs struct {
	Path    string
	Filters helpers.FilterFieldToValueMap
}

type ExtractFilterOptionsFromYamlActivity struct{}

type ExtractFilterOptionsFromYamlActivityParams struct {
	fx.In
}

type ExtractFilterOptionsFromYamlActivityResult struct {
	FilterOptions []helpers.FilterOptions
}

func NewExtractFilterOptionsFromYamlActivity(p ExtractFilterOptionsFromYamlActivityParams) *ExtractFilterOptionsFromYamlActivity {
	return &ExtractFilterOptionsFromYamlActivity{}
}

func (a ExtractFilterOptionsFromYamlActivity) Name() string {
	return "ExtractFilterOptionsFromYamlActivity"
}

func (a ExtractFilterOptionsFromYamlActivity) Execute(ctx context.Context, args *ExtractFilterOptionsFromYamlActivityArgs) (ExtractFilterOptionsFromYamlActivityResult, error) {
	// Read the YAML file
	filePath := filepath.Join(filepathhelpers.BackendRoot, args.Path)
	data, err := os.ReadFile(filePath)
	if err != nil {
		return ExtractFilterOptionsFromYamlActivityResult{}, oops.Wrapf(err, "failed to read YAML file")
	}

	// Define a struct to unmarshal the YAML
	type YamlConfig struct {
		APIEndpoints []struct {
			Type            string `yaml:"type"`
			ListRecordsSpec struct {
				FilterOptions []struct {
					Field      string `yaml:"field"`
					Comparator string `yaml:"comparator"`
				} `yaml:"filterOptions"`
			} `yaml:"listRecordsSpec"`
		} `yaml:"apiEndpoints"`
	}

	var config YamlConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		return ExtractFilterOptionsFromYamlActivityResult{}, oops.Wrapf(err, "failed to unmarshal YAML")
	}

	// Find the ListRecords endpoint and extract filter options.
	for _, endpoint := range config.APIEndpoints {
		if endpoint.Type == "ListRecords" {
			filterOptions := make([]helpers.FilterOptions, 0)
			for _, opt := range endpoint.ListRecordsSpec.FilterOptions {
				// Check if the comparator is in the allowed list
				isAllowed := false
				for _, allowed := range helpers.FilterAllowedComparators {
					if opt.Comparator == allowed {
						isAllowed = true
						break
					}
				}
				if !isAllowed {
					continue
				}
				filterOptions = append(filterOptions, helpers.FilterOptions{
					Field:      opt.Field,
					Comparator: opt.Comparator,
				})
			}

			// Validate the input filters against the filter options.
			for fieldName := range args.Filters {
				var isValid bool
				for _, option := range filterOptions {
					completeFieldName := fmt.Sprintf("%s%s", option.Field, option.Comparator)
					if completeFieldName == fieldName {
						isValid = true
						break
					}
				}
				if !isValid {
					return ExtractFilterOptionsFromYamlActivityResult{}, oops.Errorf("invalid filter field named %s", fieldName)
				}
			}

			return ExtractFilterOptionsFromYamlActivityResult{
				FilterOptions: filterOptions,
			}, nil
		}
	}

	return ExtractFilterOptionsFromYamlActivityResult{}, oops.Errorf("no ListRecords endpoint found in YAML")
}
