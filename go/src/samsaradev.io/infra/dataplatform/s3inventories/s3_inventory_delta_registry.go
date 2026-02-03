package s3inventories

import "samsaradev.io/libs/ni/infraconsts"

// NOTE: This was created during hackathon. Please don't add something here without contacting us in
// #ask-data-platform
const (
	databricksAccountInventory = "samsara-databricks-s3-inventory"
	mainAccountInventory       = "samsara-s3-inventory"

	inventoryTypeDaily  = "daily"
	inventoryTypeWeekly = "weekly"
)

type S3InventoryDeltaRegistry struct {
	InventoryBucket string `json:"inventory_bucket"`
	SourceBucket    string `json:"source_bucket"`
	InventoryType   string `json:"inventory_type"`
	// What region the inventory & source bucket are in.
	Region string `json:"-"`

	// Optionally, you may set a retention on the amount of s3inventories we keep.
	// The default for now is 180.
	Retention int `json:"retention,omitempty"`
}

func GetInventoriesForRegion(region string) []S3InventoryDeltaRegistry {
	var inventories []S3InventoryDeltaRegistry

	for _, inventory := range allInventories {
		if inventory.Region == region {
			inventories = append(inventories, inventory)
		}
	}

	return inventories
}

func getEMRExportBucketInventories() []S3InventoryDeltaRegistry {
	cells := append(infraconsts.USProdUSCells, infraconsts.USProdStagingCells...)
	var inventories []S3InventoryDeltaRegistry
	for _, cell := range cells {
		inventories = append(inventories, S3InventoryDeltaRegistry{
			InventoryBucket: databricksAccountInventory,
			SourceBucket:    "samsara-emr-replication-export-" + cell,
			InventoryType:   inventoryTypeDaily,
			Region:          infraconsts.SamsaraAWSDefaultRegion,
		})
	}
	return inventories
}

func getEMRDeltaLakeBucketInventories() []S3InventoryDeltaRegistry {
	cells := append(infraconsts.USProdUSCells, infraconsts.USProdStagingCells...)
	var inventories []S3InventoryDeltaRegistry
	for _, cell := range cells {
		inventories = append(inventories, S3InventoryDeltaRegistry{
			InventoryBucket: databricksAccountInventory,
			SourceBucket:    "samsara-emr-replication-delta-lake-" + cell,
			InventoryType:   inventoryTypeDaily,
			Region:          infraconsts.SamsaraAWSDefaultRegion,
		})
	}
	return inventories
}

func getInventories() []S3InventoryDeltaRegistry {
	var inventories = []S3InventoryDeltaRegistry{
		// Dataplatform buckets
		{
			InventoryBucket: databricksAccountInventory,
			SourceBucket:    "samsara-data-pipelines-delta-lake",
			InventoryType:   inventoryTypeDaily,
			Region:          infraconsts.SamsaraAWSDefaultRegion,
		},
		{
			InventoryBucket: databricksAccountInventory,
			SourceBucket:    "samsara-rds-delta-lake",
			InventoryType:   inventoryTypeDaily,
			Region:          infraconsts.SamsaraAWSDefaultRegion,
		},
		{
			InventoryBucket: databricksAccountInventory,
			SourceBucket:    "samsara-kinesisstats-delta-lake",
			InventoryType:   inventoryTypeDaily,
			Region:          infraconsts.SamsaraAWSDefaultRegion,
		},
		{
			InventoryBucket: databricksAccountInventory,
			SourceBucket:    "samsara-databricks-playground",
			InventoryType:   inventoryTypeDaily,
			Region:          infraconsts.SamsaraAWSDefaultRegion,
		},
		{
			InventoryBucket: mainAccountInventory,
			SourceBucket:    "samsara-databricks-workspace",
			InventoryType:   inventoryTypeDaily,
			Region:          infraconsts.SamsaraAWSDefaultRegion,
		},
		{
			InventoryBucket: databricksAccountInventory,
			SourceBucket:    "samsara-databricks-warehouse",
			InventoryType:   inventoryTypeDaily,
			Region:          infraconsts.SamsaraAWSDefaultRegion,
		},
		{
			InventoryBucket: databricksAccountInventory,
			SourceBucket:    "samsara-s3bigstats-delta-lake",
			InventoryType:   inventoryTypeDaily,
			Region:          infraconsts.SamsaraAWSDefaultRegion,
		},
		{
			InventoryBucket: databricksAccountInventory,
			SourceBucket:    "samsara-data-streams-delta-lake",
			InventoryType:   inventoryTypeDaily,
			Region:          infraconsts.SamsaraAWSDefaultRegion,
		},
		{
			InventoryBucket: databricksAccountInventory,
			SourceBucket:    "samsara-report-staging-tables",
			InventoryType:   inventoryTypeDaily,
			Region:          infraconsts.SamsaraAWSDefaultRegion,
		},

		// Sites
		{
			InventoryBucket: mainAccountInventory,
			SourceBucket:    "samsara-workforce-video-assets",
			InventoryType:   inventoryTypeDaily,
			Region:          infraconsts.SamsaraAWSDefaultRegion,
			Retention:       14, // this bucket is very large and so we have a 2 week retention on inventories
		},
		{
			InventoryBucket: mainAccountInventory,
			SourceBucket:    "samsara-sites-media-backup",
			InventoryType:   inventoryTypeDaily,
			Region:          infraconsts.SamsaraAWSDefaultRegion,
		},

		// KinesisStats
		// Retention: These inventory bucketes are larger than Data Plat buckets (in terms of # of files/objects)
		// but smaller than workforce-video-assets. However, we don't see a use case for too much history,
		// so set the retention to 1 week for now
		{
			InventoryBucket: mainAccountInventory,
			SourceBucket:    "samsara-ks0-kinesisstats",
			InventoryType:   inventoryTypeDaily,
			Retention:       7,
			Region:          infraconsts.SamsaraAWSDefaultRegion,
		},
		{
			InventoryBucket: mainAccountInventory,
			SourceBucket:    "samsara-ks1-kinesisstats",
			InventoryType:   inventoryTypeDaily,
			Retention:       7,
			Region:          infraconsts.SamsaraAWSDefaultRegion,
		},
		{
			InventoryBucket: mainAccountInventory,
			SourceBucket:    "samsara-ks2-kinesisstats",
			InventoryType:   inventoryTypeDaily,
			Retention:       7,
			Region:          infraconsts.SamsaraAWSDefaultRegion,
		},
		{
			InventoryBucket: mainAccountInventory,
			SourceBucket:    "samsara-ks3-kinesisstats",
			InventoryType:   inventoryTypeDaily,
			Retention:       7,
			Region:          infraconsts.SamsaraAWSDefaultRegion,
		},
		{
			InventoryBucket: mainAccountInventory,
			SourceBucket:    "samsara-ks4-kinesisstats",
			InventoryType:   inventoryTypeDaily,
			Retention:       7,
			Region:          infraconsts.SamsaraAWSDefaultRegion,
		},
		{
			InventoryBucket: mainAccountInventory,
			SourceBucket:    "samsara-ks5-kinesisstats",
			InventoryType:   inventoryTypeDaily,
			Retention:       7,
			Region:          infraconsts.SamsaraAWSDefaultRegion,
		},
	}
	// EMR buckets
	inventories = append(inventories, getEMRExportBucketInventories()...)
	inventories = append(inventories, getEMRDeltaLakeBucketInventories()...)

	return inventories
}

var allInventories = getInventories()
