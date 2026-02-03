package databricks

import (
	"context"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/dataplatform/databricks"
)

// This is a really simplified version of the total output. We only put the elements we care about
// in terraform state, and ignore the rest, as the API returns a lot of other info. This is to
// simplify this provider implementation.
var cloudCredentialSchema = map[string]*schema.Schema{
	"name": {
		Type:     schema.TypeString,
		Required: true,
		ForceNew: true,
	},
	"role_arn": &schema.Schema{
		Type:     schema.TypeString,
		Required: true,
		ForceNew: true,
	},
	"purpose": {
		Type:     schema.TypeString,
		Required: true,
		ForceNew: true,
	},
}

// We don't implement update; it's fine to recreate credentials
// upon any changes, since they're referenced by name whenever used.
func resourceCloudCredential() *schema.Resource {
	return &schema.Resource{
		Create: resourceCloudCredentialCreate,
		Read:   resourceCloudCredentialRead,
		Delete: resourceCloudCredentialDelete,
		Schema: cloudCredentialSchema,
	}
}

func resourceCloudCredentialCreate(d *schema.ResourceData, m interface{}) error {
	client := m.(databricks.API)

	out, err := client.CreateCloudCredential(context.Background(), &databricks.CreateCredentialInput{
		Name:    d.Get("name").(string),
		Purpose: d.Get("purpose").(string),
		AwsIamRole: databricks.CloudCredentialAwsIamRole{
			RoleArn: d.Get("role_arn").(string),
		},
	})

	if err != nil {
		return oops.Wrapf(err, "")
	}

	// We use the `name` as the ID; these have to be unique anyways.
	d.SetId(out.Name)
	return nil
}

func resourceCloudCredentialRead(d *schema.ResourceData, m interface{}) error {
	client := m.(databricks.API)
	out, err := client.GetCloudCredential(context.Background(), &databricks.GetCredentialInput{
		Name: d.Get("name").(string),
	})
	if err != nil {
		if apiErr, ok := oops.Cause(err).(*databricks.APIError); ok {
			if strings.Contains(apiErr.Message, "does not exist") {
				// Indicate that we should remove this resource.
				d.SetId("")
				return nil
			}
		}
		return oops.Wrapf(err, "")
	}

	if err = d.Set("role_arn", out.CredentialInfo.AwsIamRole.RoleArn); err != nil {
		return oops.Wrapf(err, "failed to set role_arn")
	}
	if err = d.Set("purpose", out.CredentialInfo.Purpose); err != nil {
		return oops.Wrapf(err, "failed to set purpose")
	}
	if err = d.Set("name", out.CredentialInfo.Name); err != nil {
		return oops.Wrapf(err, "failed to set name")
	}
	return nil
}

func resourceCloudCredentialDelete(d *schema.ResourceData, m interface{}) error {
	client := m.(databricks.API)

	if _, err := client.DeleteCloudCredential(context.Background(), &databricks.DeleteCredentialInput{
		Name: d.Id(),
	}); err != nil {
		return oops.Wrapf(err, "")
	}
	return nil
}
