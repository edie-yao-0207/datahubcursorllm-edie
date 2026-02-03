package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/dataplatform/dataplatformhelpers"
	"samsaradev.io/libs/ni/infraconsts"
)

// Note: This is a legacy script that was used to create and manage cloud credentials
// before Databricks had properly supported it via terraform. It is kept here for
// posterity and in case we need to use it again.
func main() {
	var region string
	flag.StringVar(&region, "region", "", "The region to execute the reload in (us-west-2 for US or eu-west-2 for EU)")
	flag.Parse()

	ctx := context.Background()

	switch region {
	case infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion:
	default:
		log.Fatalf("invalid region %s. Please provide a valid region (us-west-2 for US or eu-west-1 for EU)", region)
	}

	dbxApi, err := dataplatformhelpers.GetDatabricksE2Client(region)
	if err != nil {
		log.Fatalln(err)
	}

	// Just mess with the API and see that things are working.
	// Create a test credential
	created, err := dbxApi.CreateCloudCredential(ctx, &databricks.CreateCredentialInput{
		Name:    "Junk",
		Purpose: databricks.CredentialPurposeService,
		AwsIamRole: databricks.CloudCredentialAwsIamRole{
			RoleArn: "arn:aws:iam::353964698255:role/ec2-instance/dataplatform-cluster",
		},
	})
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Printf("Created: %v\n", created)

	// List credentials, both using list and get
	output, err := dbxApi.ListCloudCredentials(ctx)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Printf("Cloud Credentials: %v\n", *output)

	for _, credential := range output.Credentials {
		fmt.Printf("name: %s\n", credential.Name)
		cred, err := dbxApi.GetCloudCredential(ctx, &databricks.GetCredentialInput{Name: credential.Name})
		if err != nil {
			log.Fatalln(err)
		}
		fmt.Printf("Credential: %v\n", cred)
	}

	// Mess with permissions
	permissions, err := dbxApi.ListCredentialPermissions(ctx, &databricks.ListCredentialPermissionsInput{Name: "junk"})
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Printf("Permissions on junk: %v\n", permissions)

	// Change permissions
	changes := []databricks.Change{
		{
			Principal: "tim.kim@samsara.com",
			Add:       []string{databricks.CredentialAccess},
		},
	}
	changedPermissions, err := dbxApi.ChangeCredentialPermissions(ctx, &databricks.PatchCredentialPermissionsInput{
		Name:    "junk",
		Changes: changes,
	})
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Printf("Changed Permissions: %v\n", changedPermissions)

	changes = []databricks.Change{
		{
			Principal: "tim.kim@samsara.com",
			Remove:    []string{databricks.CredentialAccess},
		},
	}
	changedPermissions, err = dbxApi.ChangeCredentialPermissions(ctx, &databricks.PatchCredentialPermissionsInput{
		Name:    "junk",
		Changes: changes,
	})
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Printf("Changed Permissions: %v\n", changedPermissions)

	// Delete credential
	deletionOutput, err := dbxApi.DeleteCloudCredential(ctx, &databricks.DeleteCredentialInput{Name: "junk"})
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Printf("Deletion Output: %v\n", deletionOutput)

}
