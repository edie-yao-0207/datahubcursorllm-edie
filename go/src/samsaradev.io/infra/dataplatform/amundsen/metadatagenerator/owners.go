package metadatagenerator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/samsarahq/go/oops"

	"samsaradev.io/helpers/slog"
	"samsaradev.io/infra/dataplatform/amundsen/metadataregistry"
	"samsaradev.io/infra/dataplatform/datastreamlake"
	"samsaradev.io/infra/dataplatform/dynamodbdeltalake"
	"samsaradev.io/infra/dataplatform/ksdeltalake"
	"samsaradev.io/infra/dataplatform/rdsdeltalake"
	"samsaradev.io/infra/fxregistry"
	"samsaradev.io/infra/objectstatownership"
	"samsaradev.io/infra/samsaraaws/s3iface"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/service/dbregistry"
	"samsaradev.io/team"
)

type OwnersGenerator struct {
	s3Client s3iface.S3API
}

func newOwnersGenerator(s3Client s3iface.S3API) (*OwnersGenerator, error) {
	return &OwnersGenerator{
		s3Client: s3Client,
	}, nil
}

func init() {
	fxregistry.MustRegisterDefaultConstructor(newOwnersGenerator)
}

func (a *OwnersGenerator) Name() string {
	return "amundsen-owners-generator"
}

func (a *OwnersGenerator) GenerateMetadata(metadata *Metadata) error {
	ctx := context.Background()

	tableOwners := make(map[string]string)

	// Unmanaged Data Assets
	for tableName, owner := range metadataregistry.Owners {
		tableOwners[tableName] = getAmundsenTeamDisplay(owner.TeamName)
	}

	// Kinesis Stats tables
	for _, stat := range ksdeltalake.AllTables() {
		team, shared := objectstatownership.GetTeamOwnerNameForStatName(stat.Name)
		if shared {
			// Shared stats don't have a clear owner, continue
			// we should also look in dataplatform/ksdeltalake/registry.go for AdditionalAdminTeams
			// and add them to the list of teams that should own the table
			additionalTeams := stat.AdditionalAdminTeams
			if len(additionalTeams) > 0 {
				team = additionalTeams[0].TeamName
			} else {
				continue
			}
		}

		tableOwners[strings.ToLower(stat.QualifiedName())] = getAmundsenTeamDisplay(team)
	}

	// RDS tables
	for _, db := range rdsdeltalake.AllDatabases() {
		teamDisplay := getAmundsenTeamDisplay(dbregistry.DbToTeam[db.RegistryName].TeamName)
		for _, table := range db.TablesInRegion(infraconsts.SamsaraAWSDefaultRegion) {
			for _, tableName := range getAllTableNames(db, table, infraconsts.SamsaraAWSDefaultRegion) {
				tableOwners[tableName] = teamDisplay
			}
		}
	}

	// Dynamodb tables
	for _, table := range dynamodbdeltalake.AllTables() {
		tableName := fmt.Sprintf("dynamodb.%s", strings.ReplaceAll(table.TableName, "-", "_"))
		tableOwners[tableName] = getAmundsenTeamDisplay(table.TeamOwner.TeamName)
	}

	// Data Pipelines
	for _, node := range metadata.DataPipelineNodes {
		tableOwners[node.Name] = getAmundsenTeamDisplay(node.Owner.TeamName)
	}

	// Data Streams
	for _, stream := range datastreamlake.Registry {
		tableOwners[fmt.Sprintf("datastreams.%s", stream.StreamName)] = getAmundsenTeamDisplay(stream.Owner.TeamName)
		tableOwners[fmt.Sprintf("datastreams_errors.%s", stream.StreamName)] = getAmundsenTeamDisplay(stream.Owner.TeamName)
		tableOwners[fmt.Sprintf("datastreams_schema.%s", stream.StreamName)] = getAmundsenTeamDisplay(stream.Owner.TeamName)
	}

	// S3 Big Stats
	for _, bigStat := range ksdeltalake.AllS3BigStatTables() {
		team, shared := objectstatownership.GetTeamOwnerNameForStatName(bigStat.Name)
		if shared {
			// Shared stats don't have a clear owner, continue
			continue
		}

		teamDisplay := getAmundsenTeamDisplay(team)
		tableOwners[strings.ToLower(bigStat.S3BigStatsName())] = teamDisplay
		tableOwners[fmt.Sprintf("kinesisstats.%s_with_s3_big_stat", strings.ToLower(bigStat.Name))] = teamDisplay
	}

	// S3 Tables
	for tableName, s3TableMetadata := range metadata.S3Tables {
		tableOwners[tableName] = getAmundsenTeamDisplay(s3TableMetadata.Owner)
	}

	// SQL Views
	for viewName, sqlViewMetadata := range metadata.SQLViews {
		tableOwners[viewName] = getAmundsenTeamDisplay(sqlViewMetadata.Owner)
	}

	// Dagster tables - these are last because we want the ownership in Dagster to take
	// precedence in the event of a name collision.
	output, err := a.s3Client.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String("samsara-amundsen-metadata"),
		Key:    aws.String("staging/owners/dagster.json"),
	})
	if err != nil {
		return oops.Wrapf(err, "failed to read Dagster metadata")
	}

	fileBytes, err := ioutil.ReadAll(output.Body)

	var dagsterOwnerData map[string]string

	err = json.Unmarshal(fileBytes, &dagsterOwnerData)
	if err != nil {
		slog.Infow(ctx, fmt.Sprintf("Error unmarshalling JSON: %s", err))
	}

	for table, tableOwner := range dagsterOwnerData {
		tableOwners[table] = tableOwner
	}

	rawOwners, err := json.Marshal(tableOwners)
	if err != nil {
		return oops.Wrapf(err, "error marshalling owners map to json")
	}

	_, err = a.s3Client.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket: aws.String("samsara-amundsen-metadata"),
		Key:    aws.String("delta/table_owners.json"),
		Body:   bytes.NewReader(rawOwners),
		ACL:    aws.String(s3.ObjectCannedACLBucketOwnerFullControl),
	})
	if err != nil {
		return oops.Wrapf(err, "error uploading owners file to S3")
	}

	return nil
}

func getAmundsenTeamDisplay(teamName string) string {
	slackChannel := team.TeamByName[teamName].SlackContactChannel

	if slackChannel == "" {
		return teamName
	}

	return fmt.Sprintf("%s - #%s", teamName, slackChannel)
}

var _ MetadataGenerator = &OwnersGenerator{}
