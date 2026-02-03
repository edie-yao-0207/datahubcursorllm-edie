package metadatagenerator

import (
	"fmt"

	"samsaradev.io/infra/dataplatform/rdsdeltalake"
)

func getAllTableNames(db rdsdeltalake.RegistryDatabase, table rdsdeltalake.RegistryTable, region string) []string {
	var deltaTableNames []string

	for _, shard := range db.RegionToShards[region] {
		// Base view that is the most up to date version
		deltaTableNames = append(deltaTableNames, fmt.Sprintf("%s.%s", rdsdeltalake.GetSparkFriendlyRdsDBName(shard, db.Sharded, false), table.TableName))

		// All versions of the table
		for i := 0; i <= int(table.VersionInfo.SparkVersionParquet()); i++ {
			deltaTableNames = append(deltaTableNames, fmt.Sprintf("%s.%s", rdsdeltalake.GetSparkFriendlyRdsDBName(shard, db.Sharded, false), fmt.Sprintf("%s_v%d", table.TableName, i)))
		}
	}

	// If the db is sharded then there is a combined shards view with the db suffix as 'shards'
	if db.Sharded {
		deltaTableNames = append(deltaTableNames, fmt.Sprintf("%s_shards.%s", db.Name, table.TableName))
	}

	return deltaTableNames
}
