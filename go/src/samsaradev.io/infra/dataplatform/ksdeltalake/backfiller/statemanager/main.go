// This script allows people with permissions to manipulate the state of kinesis stats backfills
// Runbook here: https://paper.dropbox.com/doc/Runbook-KS-Delta-Lake-Backfill-Automation--A7p9Zh6QgnY~afIs8kNQj~ofAg-KK7L0RtX0XpaLGVHfT3CY
package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"

	"samsaradev.io/infra/dataplatform/dataplatformhelpers"
	"samsaradev.io/infra/dataplatform/ksdeltalake"
	"samsaradev.io/infra/samsaraaws/awsregionconsts"
	"samsaradev.io/infra/samsaraaws/s3iface"
	"samsaradev.io/libs/ni/infraconsts"
)

func main() {
	var table, region string
	var attemptsCount int
	var allTables, completed, clear bool
	flag.StringVar(&table, "table", "", "name of object stat")
	flag.StringVar(&region, "region", infraconsts.SamsaraAWSDefaultRegion, "aws region")
	flag.BoolVar(&allTables, "allTables", false, "print out the status of all tables. note that you cannot mutate the state of all tables with the other commands")
	flag.BoolVar(&completed, "completed", false, "if provided will set the backfill status to completed")
	flag.BoolVar(&clear, "clear", false, "if provided will clear the backfill status")
	flag.IntVar(&attemptsCount, "attempts", -1, "number of attempts to put in attempts file")
	flag.Parse()

	switch region {
	case infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion:
		break
	default:
		log.Panicf("Invalid region %s. Please specify %s, %s, or %s", region, infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion)
	}
	fmt.Printf("Running in region %s\n", region)

	sess := dataplatformhelpers.GetAWSSession(region)
	prefix := awsregionconsts.RegionPrefix[region]
	s3Client := s3.New(sess)
	bucket := prefix + "dataplatform-metadata"

	if allTables {
		allStats := ksdeltalake.AllTables()
		sort.Slice(allStats, func(i, j int) bool {
			return allStats[i].Name < allStats[j].Name
		})
		printAllTables(s3Client, allStats, bucket)
		return
	}

	if table == "" {
		log.Panicln("-table missing")
	}

	if completed {
		setCompleted(s3Client, table, bucket)
	} else if clear {
		clearFiles(s3Client, table, bucket)
	}

	flag.Visit(func(f *flag.Flag) {
		// checks if flag exists, if so, set attempts value
		if f.Name == "attempts" {
			if attemptsCount < 0 {
				log.Panicln("please add integer >= 0 to attempts flag")
			}
			setAttempts(s3Client, table, bucket, attemptsCount)
		}
	})

	getState(s3Client, table, bucket)
}

func printAllTables(s3Client s3iface.S3API, tables []ksdeltalake.Table, bucket string) {
	var backfills []backfillState
	for _, table := range tables {
		backfills = append(backfills, getStateInternal(s3Client, table.Name, bucket))
	}
	sort.Slice(backfills, func(i, j int) bool {
		// Sort by completed, putting incomplete stuff at the top
		if backfills[i].completed == backfills[j].completed {
			// Sort by completed time descending
			if backfills[i].completedAt != nil && backfills[j].completedAt != nil {
				return !backfills[i].completedAt.Before(*backfills[j].completedAt)
			}

			if backfills[i].completedAt == nil {
				return false
			}
			return true
		}

		if !backfills[i].completed {
			return true
		}
		return false
	})
	for _, bf := range backfills {
		fmt.Printf("[%s]. Completed: %t. CompletedAt: %v. Attempts: %d.\n", bf.table, bf.completed, bf.completedAt, bf.attemptsCount)
	}
}

func setCompleted(s3Client s3iface.S3API, table string, bucket string) {
	if _, err := s3Client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(fmt.Sprintf("kinesisstats-backfill/%s/completed", table)),
	}); err != nil {
		log.Panicln(err, "")
	}
	log.Println(fmt.Sprintf("Success! Completed file has been placed in S3 at %s/kinesisstats-backfill/%s", bucket, table))
	os.Exit(0)
}

func setAttempts(s3Client s3iface.S3API, table string, bucket string, attemptsCount int) {
	if _, err := s3Client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(fmt.Sprintf("kinesisstats-backfill/%s/attempts", table)),
		Body:   strings.NewReader(strconv.FormatInt(int64(attemptsCount), 10)),
	}); err != nil {
		log.Panicln(err, "")
	}
	log.Println(fmt.Sprintf("Success! Attempts file has been placed in S3 at %s/kinesisstats-backfill/%s", bucket, table))
	os.Exit(0)
}

type backfillState struct {
	table         string
	completed     bool
	completedAt   *time.Time
	attemptsCount int
}

func getStateInternal(s3Client s3iface.S3API, table string, bucket string) backfillState {
	state := backfillState{table: table}
	// Get completed file
	out, err := s3Client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(fmt.Sprintf("kinesisstats-backfill/%s/completed", table)),
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); !(ok && aerr.Code() == s3.ErrCodeNoSuchKey) {
			log.Panicln(err, "Could not get completed file")
		}
	} else {
		state.completed = true
		state.completedAt = out.LastModified
	}

	// Get and parse attempts file
	s3out, err := s3Client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(fmt.Sprintf("kinesisstats-backfill/%s/attempts", table)),
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); !(ok && aerr.Code() == s3.ErrCodeNoSuchKey) {
			log.Panicln(err, "Could not get attempts file")
		}
	} else {
		defer s3out.Body.Close()
		// If attempts file exists, read body of file, convert to int and set current attempts count
		body, err := ioutil.ReadAll(s3out.Body)
		if err != nil {
			log.Panicln(err, "Error reading contents of attempts file")
		}
		value, err := strconv.ParseInt(string(body), 10, 64)
		if err != nil {
			log.Panicln(err, "Could not parse contents of attempts file into integer: ", string(body))
		}
		state.attemptsCount = int(value)
	}
	return state
}

func getState(s3Client s3iface.S3API, table string, bucket string) {
	state := getStateInternal(s3Client, table, bucket)
	fmt.Printf("[%s]. Completed: %t. CompletedAt: %v. Attempts: %d.\n", table, state.completed, state.completedAt, state.attemptsCount)
}

func clearFiles(s3Client s3iface.S3API, table string, bucket string) {
	if _, err := s3Client.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(fmt.Sprintf("kinesisstats-backfill/%s/completed", table)),
	}); err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() != s3.ErrCodeNoSuchKey {
			log.Panicln(err, "")
		}
	}

	if _, err := s3Client.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(fmt.Sprintf("kinesisstats-backfill/%s/attempts", table)),
	}); err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() != s3.ErrCodeNoSuchKey {
			log.Panicln(err, "")
		}
	}

	log.Println(fmt.Sprintf("Success! %s/kinesisstats-backfill/%s has been cleared", bucket, table))
	os.Exit(0)
}
