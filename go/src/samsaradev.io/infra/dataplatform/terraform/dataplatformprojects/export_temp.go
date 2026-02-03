package dataplatformprojects

var StandardDevReadBuckets = standardDevReadBuckets
var DataPipelineEUReplicatedBucketsForUSRegion = dataPipelineEUReplicatedBucketsForUSRegion
var GetReadOnlyDevDbNames = getReadOnlyDevDbNames
var GetDataModelDatabaseNames = getDataModelDatabaseNames
var AllRawClusterProfiles = allRawClusterProfiles
var InteractiveClusterInstanceProfileName = interactiveClusterInstanceProfileName

var GetPolicyAttachments = func(c ClusterProfile) []string {
	return c.policyAttachments
}

var GetReadDatabases = func(c ClusterProfile) []string {
	return c.readDatabases
}

var GetReadWriteDatabases = func(c ClusterProfile) []string {
	return c.readwriteDatabases
}

var GetInstanceProfileName = func(c ClusterProfile) string {
	return c.instanceProfileName
}

var GetRegionSpecificReadBuckets = func(c ClusterProfile) map[string][]string {
	return c.regionSpecificReadBuckets
}

var GetReadBuckets = func(c ClusterProfile) []string {
	return c.readBuckets
}
