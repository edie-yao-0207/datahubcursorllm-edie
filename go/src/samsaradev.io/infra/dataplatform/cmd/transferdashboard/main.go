package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/dataplatform/dataplatformhelpers"
)

// This script can be used to transfer *ownership* of a databricks SQL dashboard, and all of its underlying queries
// to a new user. This can be useful when the original author of a dashboard has offboarded or is OOO
// and we want to move it over to a new person. Normally this takes a lot of manual work, so hopefully
// this script can help in those situations.
// USAGE:
// go run . -region us-west-2 -dashboardId <> -newOwner someone@samsara.com
func main() {

	var region, dashboardId, newOwner string
	flag.StringVar(&region, "region", "", "The region to execute the reload in (us-west-2 for US or eu-west-2 for EU)")
	flag.StringVar(&dashboardId, "dashboardId", "", "The dashboard ID for which you want the dashboard and all queries to be moved")
	flag.StringVar(&newOwner, "newOwner", "", "The email address of the new owner")
	flag.Parse()

	ctx := context.Background()

	if region == "" || dashboardId == "" || newOwner == "" || !strings.HasSuffix(newOwner, "@samsara.com") {
		log.Fatalln("please provide region, dashboard id, and new owner (has to be a samsara email)")
	}

	dbxApi, err := dataplatformhelpers.GetDatabricksE2Client(region)
	if err != nil {
		log.Fatalln(err)
	}

	err = transferItem(ctx, dbxApi, databricks.ObjectTypeSqlDashboard, dashboardId, newOwner)
	if err != nil {
		log.Fatal(err)
	}

	resp, err := dbxApi.GetSqlDashboard(ctx, dashboardId)
	if err != nil {
		log.Fatal(err)
	}

	// Our current implementation of the databricks client API does not parse back the widgets section of the
	// API response, and I had trouble finding the full spec in the docs. For this script, i'm just parsing out
	// what we need from the response.
	type query struct {
		Id   string `json:"id"`
		Name string `json:"name"`
	}
	type visualization struct {
		Query *query `json:"query"`
	}
	type widget struct {
		Id            string         `json:"id"`
		Visualization *visualization `json:"visualization"`
	}

	var widgets []widget
	for _, w := range resp.Widgets {
		var parsed widget
		err := json.Unmarshal(w, &parsed)
		if err != nil {
			log.Fatalln(err)
		}
		widgets = append(widgets, parsed)
	}

	fmt.Printf("widgets %v\n", widgets)

	var queries []string
	for _, w := range widgets {
		if w.Visualization != nil && w.Visualization.Query != nil && w.Visualization.Query.Id != "" {
			queries = append(queries, w.Visualization.Query.Id)
		}
	}

	for _, query := range queries {
		err = transferItem(ctx, dbxApi, databricks.ObjectTypeSqlQuery, query, newOwner)
		if err != nil {
			log.Fatal(err)
		}
	}

}

func transferItem(ctx context.Context, dbxApi *databricks.Client, objectType databricks.ObjectType, objectId string, newOwner string) error {
	_, err := dbxApi.TransferObject(ctx, &databricks.TransferObjectInput{
		ObjectType: objectType,
		ObjectId:   objectId,
		NewOwner:   newOwner,
	})

	if err != nil {
		if strings.Contains(err.Error(), "object already belongs to") {
			fmt.Printf("%s %s already owned by %s. continuing...\n", objectType, objectId, newOwner)
			return nil
		} else {
			return oops.Wrapf(err, "")
		}
	}

	fmt.Printf("successfully transferred ownership of %s %s to %s.\n", objectType, objectId, newOwner)
	return nil
}
