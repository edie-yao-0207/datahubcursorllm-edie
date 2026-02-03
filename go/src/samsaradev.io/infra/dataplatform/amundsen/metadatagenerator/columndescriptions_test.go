package metadatagenerator_test

import (
	"testing"

	"github.com/samsarahq/go/snapshotter"

	"samsaradev.io/hubproto/objectstatproto"
	"samsaradev.io/infra/dataplatform/amundsen/metadatagenerator"
	"samsaradev.io/infra/dataplatform/datastreamlake"
	"samsaradev.io/infra/dataplatform/ksdeltalake"
	"samsaradev.io/infra/dataplatform/sparktypes"
)

func TestParsingBigStatsFields(t *testing.T) {
	snapper := snapshotter.New(t)
	defer snapper.Verify()
	stat := ksdeltalake.NewTable(ksdeltalake.Stat{
		StatType:                 objectstatproto.ObjectStatEnum_osDAccelerometer,
		BinaryMessageField:       "AccelerometerEvent",
		S3BinaryMessageField:     "AccelEventData",
		Kind:                     ksdeltalake.StatKindObjectStat,
		DoNotSetConversionParams: sparktypes.ConversionParams{LegacyUint64AsBigint: true},
		MetadataInfo: &ksdeltalake.MetadataInfo{
			ColumnDescriptions: map[string]string{
				"value.proto_value.accelerometer_event.last_gps.ts":               "test description for value.proto_value.accelerometer_event.last_gps.ts",               // Only on ks schema
				"s3_proto_value.accel_event_data.oriented_gyro_lp_filtered.z_dps": "test description for s3_proto_value.accel_event_data.oriented_gyro_lp_filtered.z_dps", // Only on bigstats schema
			},
		},
	})

	// Get all the bigstat field names from the schema
	bigStatColNames := metadatagenerator.GetAllBigStatFieldNames()

	// Get all the ks field names for ks tables that are also bigstats using the schema
	KSColumnNamesForBigStat := metadatagenerator.GetAllKSFieldNamesForBigStats()

	// Get the columns for eacch of the three tables
	bigStatColumnDescriptions, ksColumnDescriptions, kinesisWithBigStatColumnDescriptions := metadatagenerator.SortBigStatsColumnNamesToCorrectTable(stat, bigStatColNames, KSColumnNamesForBigStat)
	snapper.Snapshot("TestParsingBigStatsFields", bigStatColumnDescriptions, ksColumnDescriptions, kinesisWithBigStatColumnDescriptions)
}

func TestPipelineErrorsColDescriptions(t *testing.T) {
	snapper := snapshotter.New(t)
	defer snapper.Verify()
	columnDescriptions := make(map[string]map[string]string)
	pipelineNames := map[string]struct{}{
		"testdbname": {},
	}
	columnDescriptions = metadatagenerator.PipelineErrorsTableColumnDescriptions(columnDescriptions, pipelineNames)
	snapper.Snapshot("TestParsingPipelineErrorsColumnDescriptions", columnDescriptions)

}

func TestParsingDataStreamTables(t *testing.T) {
	snapper := snapshotter.New(t)
	defer snapper.Verify()

	partitionStragy := datastreamlake.PartitionStrategy{
		PartitionColumns: []string{"date"},
	}
	columnDescriptions := metadatagenerator.DataStreamDateDescription(&partitionStragy)
	snapper.Snapshot("TestPartitionStrategyDataStreamsFirehoseIngestionDate", columnDescriptions)

	partitionStragy = datastreamlake.PartitionStrategy{
		PartitionColumns:   []string{"date"},
		DateColumnOverride: "timestamp",
	}
	columnDescriptions = metadatagenerator.DataStreamDateDescription(&partitionStragy)
	snapper.Snapshot("TestPartitionStrategyDataStreamsDateOverride", columnDescriptions)

	partitionStragy = datastreamlake.PartitionStrategy{
		PartitionColumns:             []string{"date"},
		DateColumnOverrideExpression: "date(from_unixtime(event_ms / 1e3))",
	}
	columnDescriptions = metadatagenerator.DataStreamDateDescription(&partitionStragy)
	snapper.Snapshot("TestPartitionStrategyDataStreamsDateOverrideExpression", columnDescriptions)
}
