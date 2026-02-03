package glueversiondeleter

import (
	"context"
	"sort"
	"strconv"

	"samsaradev.io/libs/ni/infraconsts"

	"samsaradev.io/infra/samsaraaws"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/glue"
	"github.com/aws/aws-sdk-go/service/glue/glueiface"
	"github.com/samsarahq/go/oops"

	"samsaradev.io/helpers/slog"
	"samsaradev.io/infra/fxregistry"
)

const numberOfTableVersionsToSave = 5

type GlueVersionDeleter struct {
	catalogId  *string
	glueClient glueiface.GlueAPI
}

func New(glueClient glueiface.GlueAPI) *GlueVersionDeleter {
	region := samsaraaws.GetAWSRegion()
	accountId := infraconsts.GetDatabricksAccountIdForRegion(region)
	return &GlueVersionDeleter{
		glueClient: glueClient,
		catalogId:  aws.String(strconv.Itoa(accountId)),
	}
}

func init() {
	fxregistry.MustRegisterDefaultConstructor(New)
}

func (g *GlueVersionDeleter) Run() error {
	ctx := context.Background()
	return g.DeleteOldGlueTableVersions(ctx)
}

func (g *GlueVersionDeleter) DeleteOldGlueTableVersions(ctx context.Context) error {
	databases, err := g.getGlueDatabases(ctx)
	if err != nil {
		return oops.Wrapf(err, "error getting databases from glue")
	}

	for _, database := range databases {
		if err := g.deleteTableVersionsFromDatabase(ctx, database); err != nil {
			slog.Errorw(ctx, oops.Wrapf(err, "error deleting glue table versions for %s db", database), slog.Tag{
				"database": database,
			})
		}
	}

	return nil
}

func (g *GlueVersionDeleter) getGlueDatabases(ctx context.Context) ([]string, error) {
	databases := make([]string, 0)
	err := g.glueClient.GetDatabasesPagesWithContext(ctx, &glue.GetDatabasesInput{
		CatalogId: g.catalogId,
	}, func(output *glue.GetDatabasesOutput, b bool) bool {
		for _, database := range output.DatabaseList {
			databases = append(databases, aws.StringValue(database.Name))
		}
		return true
	})

	return databases, err
}

func (g *GlueVersionDeleter) deleteTableVersionsFromDatabase(ctx context.Context, database string) error {
	return g.glueClient.GetTablesPagesWithContext(ctx, &glue.GetTablesInput{
		CatalogId:    g.catalogId,
		DatabaseName: aws.String(database),
	}, func(output *glue.GetTablesOutput, b bool) bool {
		for _, table := range output.TableList {
			if deleteTableErr := g.deleteTableVersions(ctx, aws.StringValue(table.DatabaseName), aws.StringValue(table.Name)); deleteTableErr != nil {
				slog.Errorw(ctx, oops.Wrapf(deleteTableErr, "error deleting glue table versions for %s.%s", database, aws.StringValue(table.Name)), slog.Tag{
					"database": database,
					"table":    aws.StringValue(table.Name),
				})
			}
		}
		return true
	})
}

func (g *GlueVersionDeleter) deleteTableVersions(ctx context.Context, database string, tableName string) error {
	tableVersions := make([]*glue.TableVersion, 0)
	err := g.glueClient.GetTableVersionsPagesWithContext(ctx, &glue.GetTableVersionsInput{
		CatalogId:    g.catalogId,
		DatabaseName: aws.String(database),
		TableName:    aws.String(tableName),
	}, func(output *glue.GetTableVersionsOutput, b bool) bool {
		for _, tableVersion := range output.TableVersions {
			tableVersions = append(tableVersions, tableVersion)
		}
		return true
	})
	if err != nil {
		return oops.Wrapf(err, "error glueClient.GetTableVersionsPagesWithContext")
	}

	if len(tableVersions) < numberOfTableVersionsToSave {
		slog.Infow(ctx, "less than 5 table versions. early exiting...", "database", database, "table", tableName)
		return nil
	}

	tableVersionsToDelete := g.getTableVersionsToDelete(tableVersions)
	if len(tableVersions) == 0 {
		slog.Infow(ctx, "no table versions to delete. early exiting...", "database", database, "table", tableName)
		return nil
	}

	slog.Infow(ctx, "Deleting glue versions", "database", database, "table", tableName)
	maxVersionsPerBatchRequest := 100
	for i := 0; i < len(tableVersionsToDelete); i += maxVersionsPerBatchRequest {
		currEnd := i + maxVersionsPerBatchRequest
		if currEnd > len(tableVersionsToDelete) {
			currEnd = len(tableVersionsToDelete)
		}

		_, err = g.glueClient.BatchDeleteTableVersionWithContext(ctx, &glue.BatchDeleteTableVersionInput{
			CatalogId:    g.catalogId,
			DatabaseName: aws.String(database),
			TableName:    aws.String(tableName),
			VersionIds:   tableVersionsToDelete[i:currEnd],
		})
		if err != nil {
			return oops.Wrapf(err, "error glueClient.BatchDeleteTableVersionWithContext")
		}
	}

	return err
}

func (g *GlueVersionDeleter) getTableVersionsToDelete(tableVersions []*glue.TableVersion) []*string {
	sort.Slice(tableVersions, func(i, j int) bool {
		iIntVal, _ := strconv.Atoi(aws.StringValue(tableVersions[i].VersionId))
		jIntVal, _ := strconv.Atoi(aws.StringValue(tableVersions[j].VersionId))
		return iIntVal < jIntVal
	})

	// Add all versions to be deleted that aren't in the most recent 'numberOfTableVersionsToSave' versions
	versionIdsToDelete := make([]*string, 0)
	for _, tableVersion := range tableVersions[0 : len(tableVersions)-numberOfTableVersionsToSave] {
		versionIdsToDelete = append(versionIdsToDelete, tableVersion.VersionId)
	}

	return versionIdsToDelete
}
