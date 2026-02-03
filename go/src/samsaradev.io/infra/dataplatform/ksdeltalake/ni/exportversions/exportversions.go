package exportversions

const ExportVersionsTableName = "kinesisstats-deltalake-export-versions"

type TableVersionEntry struct {
	Table          string `dynamodbav:"table"`
	CurrentVersion int    `dynamodbav:"current_version"`
}
