package unitycatalog

import (
	"sort"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/databricksresource_official"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
)

func unityCatalogArtifactAllowlist(providerGroup string) (map[string][]tf.Resource, error) {
	resourceGroups := map[string][]tf.Resource{}

	config, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "retrieving provider config")
	}

	// Exit early in case we're not in the dev workspaces. Artifact allowlists exist at a metastore level,
	// but need to be created from a workspace (a requirement of the terraform provider).
	// We also start by testing this only in the EU
	if !dataplatformresource.IsE2ProviderGroup(config.DatabricksProviderGroup) {
		return nil, nil
	}

	resourceGroups["allowlist_artifacts"] = []tf.Resource{
		&databricksresource_official.DatabricksArtifactAllowlist{
			ResourceName: "init_scripts",
			ArtifactType: databricksresource_official.ArtifactTypeInitScript,
			ArtifactMatchers: []databricksresource_official.ArtifactMatcher{
				{
					Artifact:  "/Volumes/s3/dataplatform-deployed-artifacts/init_scripts/",
					MatchType: databricksresource_official.ArtifactMatchTypePrefix,
				},
			},
		},
		&databricksresource_official.DatabricksArtifactAllowlist{
			ResourceName: "jars",
			ArtifactType: databricksresource_official.ArtifactTypeJar,
			ArtifactMatchers: []databricksresource_official.ArtifactMatcher{
				{
					Artifact:  "/Volumes/s3/dataplatform-deployed-artifacts/jars/",
					MatchType: databricksresource_official.ArtifactMatchTypePrefix,
				},
			},
		},
	}

	var mavenArtifactMatchers []databricksresource_official.ArtifactMatcher
	mavenArtifactMatchersKeys := make([]string, 0, len(dataplatformresource.MavenAllowlistedLibraries))

	// Collect the keys and sort them to keep the orders same.
	for k := range dataplatformresource.MavenAllowlistedLibraries {
		mavenArtifactMatchersKeys = append(mavenArtifactMatchersKeys, k)
	}
	sort.Strings(mavenArtifactMatchersKeys)

	for _, coordinate := range mavenArtifactMatchersKeys {
		mavenArtifactMatchers = append(mavenArtifactMatchers, databricksresource_official.ArtifactMatcher{
			Artifact:  string(dataplatformresource.MavenAllowlistedLibraries[coordinate]),
			MatchType: databricksresource_official.ArtifactMatchTypePrefix,
		})
	}
	resourceGroups["allowlist_artifacts"] = append(resourceGroups["allowlist_artifacts"], &databricksresource_official.DatabricksArtifactAllowlist{
		ResourceName:     "maven",
		ArtifactType:     databricksresource_official.ArtifactTypeMaven,
		ArtifactMatchers: mavenArtifactMatchers,
	})

	return resourceGroups, nil
}
