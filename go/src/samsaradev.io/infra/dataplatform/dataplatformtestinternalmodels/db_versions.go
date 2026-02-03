package dataplatformtestinternalmodels

import "samsaradev.io/infra/dbtools/dbhelpers"

var Versions = dbhelpers.DBVersions{
	Versions: map[int]dbhelpers.Migration{
		1: dbhelpers.StatementsMigration(`
	CREATE TABLE notes (
		id BIGINT(20) NOT NULL,
		note VARCHAR(255),
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		PRIMARY KEY (id)
	);
	`),
		2: dbhelpers.StatementsMigration(`CALL mysql.rds_set_configuration('binlog retention hours', 48);`),
		3: dbhelpers.StatementsMigration(`
    ALTER TABLE notes
      ADD COLUMN org_id BIGINT(20) NOT NULL DEFAULT 1,
      DROP PRIMARY KEY,
      ADD PRIMARY KEY(org_id, id);
		`),
		4: dbhelpers.StatementsMigration(`
    CREATE TABLE json_notes (
			id BIGINT(20) NOT NULL,
			note JSON NOT NULL,
			nullable_note JSON DEFAULT NULL,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (id)
		);
		`),
		5: dbhelpers.StatementsMigration(`
    ALTER TABLE json_notes
      ADD COLUMN org_id BIGINT(20) NOT NULL DEFAULT 1,
      DROP PRIMARY KEY,
      ADD PRIMARY KEY(org_id, id);
		`),
	},
	// Very important: ensure the DB has utf8mb4 support enabled and character
	// sets for server and DB are set to utf8mb4 by default for strings.
	// Very important: comment out DROP TABLE statements and all earlier
	// references to the recreated object 24 hours after they have been
	// pushed to avoid accidentally losing production data later.
	Applied: 5,
}
