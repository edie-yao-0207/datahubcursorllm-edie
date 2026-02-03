package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/samsarahq/go/oops"
	"go.uber.org/fx"

	"samsaradev.io/infra/config"
	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/dataplatform/databricksdb"
	"samsaradev.io/infra/security/secrets"
	"samsaradev.io/libs/ni/pointer"
	"samsaradev.io/system"
)

func main() {
	var appConfig *config.AppConfig
	var db *databricksdb.DB
	app := system.NewFx(
		&config.ConfigParams{},
		fx.Populate(&appConfig, &db),
	)
	if err := app.Start(context.Background()); err != nil {
		log.Fatalln(oops.Wrapf(err, "Start"))
	}
	defer app.Stop(context.Background())

	if appConfig.DatabricksJdbcHttpPath == "" {
		log.Fatalln("databricks_jdbc_http_path is not set in config. it should look like 'sql/1.0/endpoints/abcdefghijk' for Databricks SQL endpoints or 'sql/protocolv1/o/5924096274798303/contentmarketing' for clusters.")
	}
	if appConfig.DatabricksE2UrlBase == "" {
		log.Fatalln("databricks_e2_url_base is not set in config. it should look like 'https://samsara-dev-us-west-2.cloud.databricks.com'.")
	}

	databricksGqlUserApiTokenResp, err := secrets.DefaultService().GetSecretValueFromKey(secrets.DatabricksGqlUserApiToken)
	if err != nil {
		log.Fatalln("Could not get DatabricksGqlUserApiToken")
	}

	// Let's validate the token value set for gqluserapi token works
	client, err := databricks.New(appConfig.DatabricksE2UrlBase, databricksGqlUserApiTokenResp.StringValue)
	if err != nil {
		log.Fatalln(oops.Wrapf(err, "Failed to instantiate databricks client."))
	}

	_, err = client.Me(context.Background(), &databricks.MeInput{})
	if err != nil {
		log.Fatalln(oops.Wrapf(err, "Failed to make sentinel API command to databricks. Check the error and that your token is not expired."))
	}

	if err := db.Ping(); err != nil {
		log.Fatalln(oops.Wrapf(err, "Ping"))
	}

	var unusedKey string
	var clusterName string
	if err := db.QueryRow("SET spark.databricks.clusterUsageTags.clusterName").Scan(&unusedKey, &clusterName); err != nil && !strings.Contains(err.Error(), "Configuration spark.databricks.clusterUsageTags.clusterName is not available.") {
		log.Fatalln(oops.Wrapf(err, "select cluster name"))
	}
	var clusterId string
	if err := db.QueryRow("SET spark.databricks.clusterUsageTags.clusterId").Scan(&unusedKey, &clusterId); err != nil && !strings.Contains(err.Error(), "Configuration spark.databricks.clusterUsageTags.clusterId is not available.") {
		log.Fatalln(oops.Wrapf(err, "select cluster id"))
	}
	var user string
	if err := db.QueryRow("select current_user()").Scan(&user); err != nil {
		log.Fatalln(oops.Wrapf(err, "select user"))
	}
	fmt.Fprintf(os.Stderr, "connected to cluster %s (%s) as %s\n", clusterName, clusterId, user)

	rows, err := db.Query(`
		SELECT
			org_id,
			device_id,
			value.latitude,
			value.longitude,
			value.revgeo_state
		FROM kinesisstats.location
		WHERE date = date_sub(current_date(), 1)
		LIMIT 1000000
	`)
	if err != nil {
		log.Fatalln(oops.Wrapf(err, "read locations"))
	}
	defer rows.Close()

	for rows.Next() {
		var orgId, deviceId int64
		var latitude, longitude float64
		var state *string
		if err := rows.Scan(&orgId, &deviceId, &latitude, &longitude, &state); err != nil {
			log.Fatalln(oops.Wrapf(err, "scan"))
		}

		fmt.Printf("%d %d %f %f %s\n", orgId, deviceId, latitude, longitude, pointer.StringValOr(state, ""))
	}

	if err := rows.Err(); err != nil {
		log.Fatalln(oops.Wrapf(err, ""))
	}
}
