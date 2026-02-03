package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/boltdb/bolt"
	"github.com/samsarahq/go/oops"
	"go.uber.org/fx"
	"golang.org/x/time/rate"

	"samsaradev.io/infra/config"
	"samsaradev.io/infra/dataplatform/ksdeltalake/backfiller"
	"samsaradev.io/infra/grpc/grpchelpers/grpcclient"
	"samsaradev.io/infra/releasemanagement/launchdarkly"
	"samsaradev.io/infra/samsaraaws/awshelpers/awssessions"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/stats/kinesisstats/jsonexporter"
	"samsaradev.io/system"
)

// This tool can be used to backfill kinesisstats data for a particular table. This is useful in case there is data
// missing in databricks for a particular stat, or if you add a proto value after the table is already being ingested
// and need to re-ingest all data.
// See the usage information for more info about each parameter, but general usage might look like:
//
//	go run . -table osdthermallimiterstate -state thermal_limiter_backfill.txt
func main() {
	var table, cluster, stateFile string
	var streamFilesOnly, dryRun bool
	var startMs, endMs int64
	flag.StringVar(&table, "table", "", "The table name you'd like to backfill, e.g. location, osdthermallimiterstate, etc.")
	flag.StringVar(&cluster, "cluster", "", "KS cluster it reads from. You should not generally need to provide this.")
	flag.StringVar(&stateFile, "state", "", "A file locally that will keep state as this backfill runs, allowing to re-run this command and pick up where it left off in case of failures.")
	flag.BoolVar(&streamFilesOnly, "streamonly", false, "Only backfill stream files (two days of recently ingested data)")
	flag.BoolVar(&dryRun, "dryrun", false, "Only print files to use in backfill and exit")
	flag.Int64Var(&startMs, "startms", 0, "If doing a partial backfill, provide the startms (unixtime) to filter datapoint event timestamps on")
	flag.Int64Var(&endMs, "endms", math.MaxInt64, "If doing a partial backfill, provide the endms (unixtime) to filter datapoint event timestamps on")
	flag.Parse()

	if stateFile == "" {
		log.Panicln("-state missing")
	}

	if table == "" {
		log.Panicln("-table missing")
	}

	if endMs < startMs {
		log.Panicln("Please specify a startms less than or equal to the endms")
	}

	confirmBackfill(startMs, endMs)

	sess := session.Must(session.NewSession())
	if sess.Config.Region == nil {
		panic("Cannot determine AWS region")
	}
	region := *sess.Config.Region

	var clusters []string
	if cluster == "" {
		var err error
		clusters, err = infraconsts.RegionKinesisStatsClusters(region)
		if err != nil {
			log.Panic(err)
		}
	} else {
		clusters = []string{cluster}
	}

	db, err := bolt.Open(stateFile, 0600, nil)
	if err != nil {
		log.Panic(err)
	}
	defer db.Close()

	// Initialize state file.
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("files"))
		return err
	}); err != nil {
		log.Panicln(err)
	}

	var appConfig *config.AppConfig
	var grpcDial grpcclient.DialFunc
	var launchDarklyBridge *launchdarkly.SDKBridge
	app := system.NewFx(&config.ConfigParams{}, fx.Populate(&appConfig, &grpcDial, &launchDarklyBridge))
	if err := app.Start(context.Background()); err != nil {
		log.Fatalln(err)
	}
	defer app.Stop(context.Background())

	sqsClient := sqs.New(sess)
	dynamodbClient := dynamodb.New(sess)
	session := awssessions.NewInstrumentedAWSSession()
	s3Client := s3.New(session)

	var partialBackfill *jsonexporter.PartialBackfillArgs
	if startMs != 0 || endMs != math.MaxInt64 {
		partialBackfill = &jsonexporter.PartialBackfillArgs{
			StartMs: startMs,
			EndMs:   endMs,
		}
	}

	if err := backfiller.BackfillKsClusters(context.Background(), appConfig, grpcDial, clusters, sqsClient, dynamodbClient, s3Client, db, backfiller.BackfillInput{
		Region:                  region,
		Table:                   table,
		ArchiveListingRateLimit: rate.Limit(500),
		StreamFilesOnly:         streamFilesOnly,
		DryRun:                  dryRun,
		ProgressBar:             true,
		PartialBackfill:         partialBackfill,
	}, launchDarklyBridge); err != nil {
		log.Fatalln(oops.Wrapf(err, "backfill: cluster=%s, table=%s", cluster, table))
	}
}

func confirmBackfill(startMs int64, endMs int64) {
	var response string
	if startMs == 0 && endMs == math.MaxInt64 {
		log.Println("You have specified to do a full KS backfill. This means every data point in the KS will be processed. \n If you know what date ranges you need to process, please specify a startms and endms to do a partial backfill to save cost/time. \n If not, and you would like to proceed, please type 'yes'")
	} else {
		startDate := time.Unix(startMs/1000, startMs%1000).UTC()
		endDate := time.Unix(endMs/1000, endMs%1000).UTC()
		log.Printf("You have chosen to partial backfill from %s to %s. To continue, please type 'yes'", startDate, endDate)
	}

	_, err := fmt.Scanln(&response)
	if err != nil {
		log.Fatal(err)
	}

	if strings.ToLower(response) != "yes" {
		log.Println("Exiting ksbackfill script. Backfill not confirmed.")
		os.Exit(0)
	}
}
