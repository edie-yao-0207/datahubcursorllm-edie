package dataplatformresource

import (
	"hash/fnv"

	"samsaradev.io/libs/ni/infraconsts"
)

func Hash(s string) uint32 {
	h := fnv.New32a()
	_, err := h.Write([]byte(s))
	if err != nil {
		return 0
	}
	return h.Sum32()
}

func JobNameToAZ(region string, jobName string) string {
	azs := infraconsts.GetDatabricksAvailabilityZones(region)
	azIndex := Hash(jobName) % uint32(len(azs))
	return azs[azIndex]
}
