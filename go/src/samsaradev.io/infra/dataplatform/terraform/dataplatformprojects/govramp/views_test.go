package govramp

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"samsaradev.io/infra/dataplatform/govramp"
)

func TestGetSqlString(t *testing.T) {
	testCases := []struct {
		name        string
		table       govramp.Table
		expectedSQL string
		expectError bool
	}{
		{
			name: "regular table",
			table: govramp.Table{
				SourceCatalog: "default",
				SourceSchema:  "dbname",
				SourceTable:   "tablename",
				TableType:     govramp.SourceTableTypeTable,
			},
			expectedSQL: `SELECT t.*
FROM default.dbname.tablename AS t
LEFT ANTI JOIN default.stateramp.stateramp_orgs AS s ON t.org_id = s.org_id`,
		},
		{
			name: "regular table with custom select query",
			table: govramp.Table{
				SourceCatalog:       "default",
				SourceSchema:        "dbname",
				SourceTable:         "tablename",
				TableType:           govramp.SourceTableTypeTable,
				CustomSelectColumns: []string{"custom_column_1", "custom_column_2"},
			},
			expectedSQL: `SELECT t.custom_column_1, t.custom_column_2
FROM default.dbname.tablename AS t
LEFT ANTI JOIN default.stateramp.stateramp_orgs AS s ON t.org_id = s.org_id`,
		},
		{
			name: "table with custom org_id column and no join",
			table: govramp.Table{
				SourceCatalog:       "default",
				SourceSchema:        "clouddb",
				SourceTable:         "groups",
				TableType:           govramp.SourceTableTypeTable,
				TableCategory:       govramp.SourceTableCategoryRds,
				OrgIdColumnOverride: "organization_id",
			},
			expectedSQL: `SELECT t.*
FROM default.clouddb.groups AS t
LEFT ANTI JOIN default.stateramp.stateramp_orgs AS s ON t.organization_id = s.org_id`,
		},
		{
			name: "table with join to clouddb.groups",
			table: govramp.Table{
				SourceCatalog:       "default",
				SourceSchema:        "clouddb",
				SourceTable:         "addresses",
				TableType:           govramp.SourceTableTypeTable,
				TableCategory:       govramp.SourceTableCategoryRds,
				OrgIdColumnOverride: "organization_id",
				JoinClause:          "JOIN non_govramp_customer_data.clouddb.groups AS g ON t.group_id = g.id",
			},
			expectedSQL: `SELECT t.*
FROM default.clouddb.addresses AS t
JOIN non_govramp_customer_data.clouddb.groups AS g ON t.group_id = g.id
LEFT ANTI JOIN default.stateramp.stateramp_orgs AS s ON g.organization_id = s.org_id`,
		},
		{
			name: "table with join clause but not to clouddb.groups",
			table: govramp.Table{
				SourceCatalog:       "default",
				SourceSchema:        "somedb",
				SourceTable:         "some_table",
				TableType:           govramp.SourceTableTypeTable,
				TableCategory:       govramp.SourceTableCategoryRds,
				OrgIdColumnOverride: "custom_org_id",
				JoinClause:          "JOIN default.someotherdb.other_table AS alias ON t.other_id = alias.id",
			},
			expectedSQL: `SELECT t.*
FROM default.somedb.some_table AS t
JOIN default.someotherdb.other_table AS alias ON t.other_id = alias.id
LEFT ANTI JOIN default.stateramp.stateramp_orgs AS s ON alias.custom_org_id = s.org_id`,
		},
		{
			name: "table with join clause but no custom org_id column",
			table: govramp.Table{
				SourceCatalog: "default",
				SourceSchema:  "somedb",
				SourceTable:   "some_table",
				TableType:     govramp.SourceTableTypeTable,
				TableCategory: govramp.SourceTableCategoryRds,
				JoinClause:    "JOIN default.someotherdb.other_table AS alias ON t.other_id = alias.id",
			},
			expectedSQL: `SELECT t.*
FROM default.somedb.some_table AS t
JOIN default.someotherdb.other_table AS alias ON t.other_id = alias.id
LEFT ANTI JOIN default.stateramp.stateramp_orgs AS s ON alias.org_id = s.org_id`,
		},
		{
			name: "rds combined shards view",
			table: govramp.Table{
				SourceCatalog: "default",
				SourceSchema:  "workforcevideodb_shards",
				SourceTable:   "areas_of_interest",
				TableType:     govramp.SourceTableTypeView,
				TableCategory: govramp.SourceTableCategoryRdsSharded,
			},
			expectedSQL: `
(
  SELECT
    *
  FROM
    (
      SELECT
        /*+ BROADCAST(os) */
        shards.*
      FROM
        (
          SELECT
            'workforcevideo_shard_0' AS shard_name,
            *
          FROM
            non_govramp_customer_data.workforcevideo_shard_0db.areas_of_interest
          UNION ALL
          SELECT
            'workforcevideo_shard_1' AS shard_name,
            *
          FROM
            non_govramp_customer_data.workforcevideo_shard_1db.areas_of_interest
          UNION ALL
          SELECT
            'workforcevideo_shard_2' AS shard_name,
            *
          FROM
            non_govramp_customer_data.workforcevideo_shard_2db.areas_of_interest
          UNION ALL
          SELECT
            'workforcevideo_shard_3' AS shard_name,
            *
          FROM
            non_govramp_customer_data.workforcevideo_shard_3db.areas_of_interest
          UNION ALL
          SELECT
            'workforcevideo_shard_4' AS shard_name,
            *
          FROM
            non_govramp_customer_data.workforcevideo_shard_4db.areas_of_interest
          UNION ALL
          SELECT
            'workforcevideo_shard_5' AS shard_name,
            *
          FROM
            non_govramp_customer_data.workforcevideo_shard_5db.areas_of_interest
        ) shards
        LEFT OUTER JOIN non_govramp_customer_data.appconfigs.org_shards_v2 os ON shards.org_id = os.org_id
      WHERE
        os.shard_type = 1
        AND os.data_type = 'workforcevideo'
    )
)`,
		},
		{
			name: "non-existent RDS Shardedview",
			table: govramp.Table{
				SourceCatalog: "default",
				SourceSchema:  "fakedbnamedb_shards",
				SourceTable:   "faketable",
				TableType:     govramp.SourceTableTypeView,
				TableCategory: govramp.SourceTableCategoryRdsSharded,
			},
			expectError: true,
		},
		{
			name: "kinesisstats history table",
			table: govramp.Table{
				SourceCatalog: "default",
				SourceSchema:  "kinesisstats_history",
				SourceTable:   "some_table",
				TableType:     govramp.SourceTableTypeTable,
				TableCategory: govramp.SourceTableCategoryKinesisstats,
			},
			expectedSQL: `SELECT t.*
FROM default.kinesisstats_history.some_table AS t
LEFT ANTI JOIN default.stateramp.stateramp_orgs AS s ON t.org_id = s.org_id`,
		},
		{
			name: "kinesisstats date filtered view",
			table: govramp.Table{
				SourceCatalog: "default",
				SourceSchema:  "kinesisstats",
				SourceTable:   "some_table",
				TableType:     govramp.SourceTableTypeView,
				TableCategory: govramp.SourceTableCategoryKinesisstats,
			},
			expectedSQL: `SELECT t.*
FROM default.kinesisstats_history.some_table AS t
LEFT ANTI JOIN default.stateramp.stateramp_orgs AS s ON t.org_id = s.org_id
WHERE date > date_sub(current_date(), 365)`,
		},
		{
			name: "kinesisstats date filtered view with custom select query",
			table: govramp.Table{
				SourceCatalog:       "default",
				SourceSchema:        "kinesisstats",
				SourceTable:         "some_table",
				TableType:           govramp.SourceTableTypeView,
				TableCategory:       govramp.SourceTableCategoryKinesisstats,
				CustomSelectColumns: []string{"custom_column_1", "custom_column_2"},
			},
			expectedSQL: `SELECT t.custom_column_1, t.custom_column_2
FROM default.kinesisstats_history.some_table AS t
LEFT ANTI JOIN default.stateramp.stateramp_orgs AS s ON t.org_id = s.org_id
WHERE date > date_sub(current_date(), 365)`,
		},
		{
			name: "clouddb splitting table",
			table: govramp.Table{
				SourceCatalog: "default",
				SourceSchema:  "clouddb",
				SourceTable:   "gateways",
				TableType:     govramp.SourceTableTypeView,
				TableCategory: govramp.SourceTableCategoryRdsClouddbSplitting,
			},
			expectedSQL: `SELECT t.*
FROM default.productsdb.gateways AS t
LEFT ANTI JOIN default.stateramp.stateramp_orgs AS s ON t.org_id = s.org_id`,
		},
		{
			name: "not clouddb splitting table",
			table: govramp.Table{
				SourceCatalog: "default",
				SourceSchema:  "clouddb",
				SourceTable:   "non_splitting_table",
				TableType:     govramp.SourceTableTypeView,
				TableCategory: govramp.SourceTableCategoryRdsClouddbSplitting,
			},
			expectError: true,
		},
		{
			name: "clouddb splitting table with wrong schema",
			table: govramp.Table{
				SourceCatalog: "default",
				SourceSchema:  "someotherdb",
				SourceTable:   "gateways",
				TableType:     govramp.SourceTableTypeView,
				TableCategory: govramp.SourceTableCategoryRdsClouddbSplitting,
			},
			expectError: true,
		},
		{
			name: "data streams table",
			table: govramp.Table{
				SourceCatalog: "default",
				SourceSchema:  "datastreams",
				SourceTable:   "some_table",
				TableType:     govramp.SourceTableTypeView,
				TableCategory: govramp.SourceTableCategoryDataStreams,
			},
			expectedSQL: `SELECT t.*
FROM default.datastreams_history.some_table AS t
LEFT ANTI JOIN default.stateramp.stateramp_orgs AS s ON t.org_id = s.org_id
WHERE date > date_sub(current_date(), 365)`,
		},
		{
			name: "data streams table with no org specific column",
			table: govramp.Table{
				SourceCatalog:       "default",
				SourceSchema:        "datastreams",
				SourceTable:         "some_table",
				TableType:           govramp.SourceTableTypeView,
				TableCategory:       govramp.SourceTableCategoryDataStreams,
				NoOrgSpecificColumn: true,
			},
			expectedSQL: `SELECT t.*
FROM default.datastreams_history.some_table AS t
WHERE date > date_sub(current_date(), 365)`,
		},
		{
			name: "data streams table with custom select query",
			table: govramp.Table{
				SourceCatalog:       "default",
				SourceSchema:        "datastreams",
				SourceTable:         "some_table",
				TableType:           govramp.SourceTableTypeView,
				TableCategory:       govramp.SourceTableCategoryDataStreams,
				CustomSelectColumns: []string{"custom_column_1", "custom_column_2"},
			},
			expectedSQL: `SELECT t.custom_column_1, t.custom_column_2
FROM default.datastreams_history.some_table AS t
LEFT ANTI JOIN default.stateramp.stateramp_orgs AS s ON t.org_id = s.org_id
WHERE date > date_sub(current_date(), 365)`,
		},
		{
			name: "no org specific column",
			table: govramp.Table{
				SourceCatalog:       "default",
				SourceSchema:        "somedb",
				SourceTable:         "some_table",
				TableType:           govramp.SourceTableTypeTable,
				NoOrgSpecificColumn: true,
			},
			expectedSQL: `SELECT t.*
FROM default.somedb.some_table AS t`,
		},
		{
			name: "table with sam_number column",
			table: govramp.Table{
				SourceCatalog:   "default",
				SourceSchema:    "somedb",
				SourceTable:     "some_table",
				TableType:       govramp.SourceTableTypeTable,
				SamNumberColumn: "sam_number",
			},
			expectedSQL: `SELECT t.*
FROM default.somedb.some_table AS t
LEFT ANTI JOIN default.stateramp.stateramp_orgs AS s ON t.sam_number = s.sam_number`,
		},
		{
			name: "table with both org_id and sam_number columns",
			table: govramp.Table{
				SourceCatalog:       "default",
				SourceSchema:        "somedb",
				SourceTable:         "some_table",
				TableType:           govramp.SourceTableTypeTable,
				OrgIdColumnOverride: "orgId",
				SamNumberColumn:     "saaam_number", // unused column name since orgIdColumn is specified.
			},
			expectedSQL: `SELECT t.*
FROM default.somedb.some_table AS t
LEFT ANTI JOIN default.stateramp.stateramp_orgs AS s ON t.orgId = s.org_id`,
		},
		{
			name: "table with custom sam_number column name",
			table: govramp.Table{
				SourceCatalog:   "default",
				SourceSchema:    "somedb",
				SourceTable:     "some_table",
				TableType:       govramp.SourceTableTypeTable,
				SamNumberColumn: "sam_number_undecorated",
			},
			expectedSQL: `SELECT t.*
FROM default.somedb.some_table AS t
LEFT ANTI JOIN default.stateramp.stateramp_orgs AS s ON t.sam_number_undecorated = s.sam_number`,
		},
		{
			name: "view with 1 underlying table reference",
			table: govramp.Table{
				SourceCatalog: "default",
				SourceSchema:  "testdb",
				SourceTable:   "testview",
				TableType:     govramp.SourceTableTypeView,
				TableReferences: []string{
					"anotherdb.underlying_table",
				},
				SqlFile: &govramp.SqlFile{
					SqlFilePath: "go/src/samsaradev.io/infra/dataplatform/govramp/viewdefinitions/testdb.testview.sql",
				},
			},
			expectedSQL: `SELECT * FROM non_govramp_customer_data.anotherdb.underlying_table`,
		},
		{
			name: "view with 2 table references",
			table: govramp.Table{
				SourceCatalog: "default",
				SourceSchema:  "testdb",
				SourceTable:   "testview2",
				TableType:     govramp.SourceTableTypeView,
				TableReferences: []string{
					"anotherdb.underlying_table",
					"yetanotherdb.underlying_table2",
				},
				SqlFile: &govramp.SqlFile{
					SqlFilePath: "go/src/samsaradev.io/infra/dataplatform/govramp/viewdefinitions/testdb.testview2.sql",
				},
			},
			expectedSQL: `SELECT device, COUNT(*) FROM non_govramp_customer_data.anotherdb.underlying_table
WHERE column = 'a'
GROUP BY device
UNION All
SELECT device, COUNT(*) FROM non_govramp_customer_data.anotherdb.underlying_table
WHERE column = 'b'
GROUP BY device
UNION All
SELECT device, COUNT(*) FROM non_govramp_customer_data.yetanotherdb.underlying_table2
GROUP BY device`,
		},
		{
			name: "view with 2 table references and sql file with variables",
			table: govramp.Table{
				SourceCatalog: "default",
				SourceSchema:  "testdb",
				SourceTable:   "testview3",
				TableType:     govramp.SourceTableTypeView,
				TableReferences: []string{
					"anotherdb.underlying_table",
					"yetanotherdb.underlying_table2",
				},
				SqlFile: &govramp.SqlFile{
					SqlFilePath: "go/src/samsaradev.io/infra/dataplatform/govramp/viewdefinitions/testdb.testview3.sql",
					VariableMap: map[string]string{
						"SOME_TABLE":    "anotherdb.underlying_table",
						"another_table": "yetanotherdb.underlying_table2",
					},
				},
			},
			expectedSQL: `SELECT device, COUNT(*) FROM non_govramp_customer_data.anotherdb.underlying_table
WHERE column = 'a'
GROUP BY device
UNION All
SELECT device, COUNT(*) FROM non_govramp_customer_data.yetanotherdb.underlying_table2
GROUP BY device`,
		},
		{
			name: "table with org_id in array",
			table: govramp.Table{
				SourceCatalog:       "default",
				SourceSchema:        "somedb",
				SourceTable:         "some_table",
				TableType:           govramp.SourceTableTypeTable,
				OrgIdColumnOverride: "org_ids",
				OrgIdInArray:        true,
			},
			expectedSQL: `SELECT t.*
FROM default.somedb.some_table AS t
WHERE NOT arrays_overlap(t.org_ids, (SELECT collect_set(org_id) FROM default.stateramp.stateramp_orgs))`,
		},
		{
			name: "clouddb splitting table with join clause",
			table: govramp.Table{
				SourceCatalog: "default",
				SourceSchema:  "clouddb",
				SourceTable:   "tag_devices",
				TableType:     govramp.SourceTableTypeView,
				TableCategory: govramp.SourceTableCategoryRdsClouddbSplitting,
				JoinClause:    "JOIN non_govramp_customer_data.productsdb.devices AS d ON t.device_id = d.id",
			},
			expectedSQL: `SELECT t.*
FROM default.productsdb.tag_devices AS t
JOIN non_govramp_customer_data.productsdb.devices AS d ON t.device_id = d.id
LEFT ANTI JOIN default.stateramp.stateramp_orgs AS s ON d.org_id = s.org_id`,
		},
		{
			name: "table with custom view definition",
			table: govramp.Table{
				SourceCatalog: "default",
				SourceSchema:  "clouddb",
				SourceTable:   "users",
				TableType:     govramp.SourceTableTypeTable,
				SqlFile: &govramp.SqlFile{
					CustomViewDefinition: true,
					SqlFilePath:          "go/src/samsaradev.io/infra/dataplatform/govramp/viewdefinitions/clouddb.users.sql",
				},
			},
			expectedSQL: `SELECT u.*
FROM default.clouddb.users u
LEFT ANTI JOIN (
SELECT DISTINCT uo.user_id
FROM default.clouddb.users_organizations uo
INNER JOIN default.stateramp.stateramp_orgs so
ON uo.organization_id = so.org_id
) stateramp_users
ON u.id = stateramp_users.user_id`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := getSqlString(tc.table)
			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedSQL, result)
			}
		})
	}
}

func TestGetSelectColumns(t *testing.T) {
	testCases := []struct {
		name        string
		table       govramp.Table
		expectedSQL string
		expectError bool
	}{
		{
			name: "table with custom select query",
			table: govramp.Table{
				SourceCatalog:       "default",
				SourceSchema:        "somedb",
				SourceTable:         "some_table",
				TableType:           govramp.SourceTableTypeTable,
				CustomSelectColumns: []string{"custom_column_1", "custom_column_2"},
			},
			expectedSQL: "t.custom_column_1, t.custom_column_2",
		},
		{
			name: "table with no custom select query",
			table: govramp.Table{
				SourceCatalog:       "default",
				SourceSchema:        "somedb",
				SourceTable:         "some_table",
				TableType:           govramp.SourceTableTypeTable,
				CustomSelectColumns: []string{},
			},
			expectedSQL: "t.*",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := getSelectColumns(tc.table)
			assert.Equal(t, tc.expectedSQL, result)
		})
	}
}
