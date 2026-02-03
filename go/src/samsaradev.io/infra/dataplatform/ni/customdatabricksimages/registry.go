package customdatabricksimages

type ECRRepoName string

const (
	Firmware = ECRRepoName("samsara-python-firmware")
)

type DatabricksECRRepo struct {
	name ECRRepoName
}

var RepoDefinitions = []DatabricksECRRepo{
	{
		name: Firmware,
	},
}
