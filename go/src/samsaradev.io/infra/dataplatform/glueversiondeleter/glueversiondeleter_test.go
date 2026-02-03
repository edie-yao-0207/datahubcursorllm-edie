package glueversiondeleter

import (
	"context"
	"strconv"
	"testing"
	"time"

	"samsaradev.io/libs/ni/infraconsts"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/glue"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"samsaradev.io/libs/ni/pointer"
	"samsaradev.io/vendormocks/mock_glueiface"
)

func TestGetGlueDatabases(t *testing.T) {
	ctx := context.Background()

	mockGlueClient := mock_glueiface.NewMockGlueAPI(gomock.NewController(t))
	mockGlueClient.EXPECT().GetDatabasesPagesWithContext(
		ctx,
		gomock.Any(),
		gomock.Any(),
	).Return(nil).Do(func(ctx context.Context, input *glue.GetDatabasesInput, fn func(p *glue.GetDatabasesOutput, lastPage bool) bool) {
		fn(&glue.GetDatabasesOutput{
			DatabaseList: []*glue.Database{
				{
					Name: aws.String("database1"),
				},
				{
					Name: aws.String("database2"),
				},
				{
					Name: aws.String("database3"),
				},
				{
					Name: aws.String("database4"),
				},
				{
					Name: aws.String("database5"),
				},
			},
		}, true)
	})

	g := New(mockGlueClient)
	databases, err := g.getGlueDatabases(ctx)
	require.Nil(t, err)
	require.Equal(t,
		[]string{
			"database1",
			"database2",
			"database3",
			"database4",
			"database5",
		}, databases)

}

func TestDeleteTableVersions(t *testing.T) {
	ctx := context.Background()
	database := "db1"
	tableName := "table1"

	mockGlueClient := mock_glueiface.NewMockGlueAPI(gomock.NewController(t))
	mockGlueClient.EXPECT().GetTableVersionsPagesWithContext(
		ctx,
		gomock.Any(),
		gomock.Any(),
		gomock.Any(),
	).Return(nil).Do(func(ctx context.Context, input *glue.GetTableVersionsInput, fn func(p *glue.GetTableVersionsOutput, lastPage bool) bool) {
		fn(&glue.GetTableVersionsOutput{
			TableVersions: generateTableVersions(10),
		}, true)
	})

	mockGlueClient.EXPECT().BatchDeleteTableVersionWithContext(ctx, &glue.BatchDeleteTableVersionInput{
		CatalogId:    aws.String(strconv.Itoa(infraconsts.SamsaraAWSDatabricksAccountID)),
		DatabaseName: aws.String(database),
		TableName:    aws.String(tableName),
		VersionIds: []*string{
			aws.String("0"),
			aws.String("1"),
			aws.String("2"),
			aws.String("3"),
			aws.String("4"),
		},
	})

	g := New(mockGlueClient)
	require.NoError(t, g.deleteTableVersions(ctx, database, tableName))
}

func TestGetTableVersionsToDelete(t *testing.T) {
	testCases := map[string]struct {
		tableVersionsInput         []*glue.TableVersion
		expectedVersionIdsToDelete []*string
	}{
		"10-different-version": {
			tableVersionsInput: generateTableVersions(10),
			expectedVersionIdsToDelete: []*string{
				aws.String("0"),
				aws.String("1"),
				aws.String("2"),
				aws.String("3"),
				aws.String("4"),
			},
		},
		"20-different-version": {
			tableVersionsInput: generateTableVersions(20),
			expectedVersionIdsToDelete: []*string{
				aws.String("0"),
				aws.String("1"),
				aws.String("2"),
				aws.String("3"),
				aws.String("4"),
				aws.String("5"),
				aws.String("6"),
				aws.String("7"),
				aws.String("8"),
				aws.String("9"),
				aws.String("10"),
				aws.String("11"),
				aws.String("12"),
				aws.String("13"),
				aws.String("14"),
			},
		},
	}

	g := New(nil)
	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			versionIdsToDelete := g.getTableVersionsToDelete(testCase.tableVersionsInput)
			require.Equal(t, testCase.expectedVersionIdsToDelete, versionIdsToDelete)
		})
	}
}

func generateTableVersions(numToGen int) []*glue.TableVersion {
	var tableVersions []*glue.TableVersion
	for idx := 0; idx < numToGen; idx++ {
		tableVersions = append(tableVersions, &glue.TableVersion{
			VersionId: aws.String(strconv.Itoa(idx)),
			Table: &glue.TableData{
				LastAccessTime: pointer.TimePtr(time.Now().AddDate(0, 0, -(numToGen - idx))),
			},
		})
	}
	return tableVersions
}
