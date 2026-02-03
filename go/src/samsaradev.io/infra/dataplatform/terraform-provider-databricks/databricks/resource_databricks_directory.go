package databricks

import (
	"context"

	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/dataplatform/databricks"
)

var directorySchema = map[string]*schema.Schema{
	"path": &schema.Schema{
		Type:     schema.TypeString,
		ForceNew: true,
		Required: true,
	},
	"object_id": &schema.Schema{
		Type:     schema.TypeInt,
		Computed: true,
	},
}

func resourceDirectory() *schema.Resource {
	return &schema.Resource{
		Create: resourceDirectoryCreate,
		Read:   resourceDirectoryRead,
		Delete: resourceDirectoryDelete,
		Importer: &schema.ResourceImporter{
			State: resourceDirectoryImport,
		},
		Schema: directorySchema,
	}
}

func resourceDirectoryCreate(d *schema.ResourceData, m interface{}) error {
	client := m.(databricks.API)
	path := d.Get("path").(string)
	if _, err := client.WorkspaceMkdirs(context.Background(), &databricks.WorkspaceMkdirsInput{
		Path: path,
	}); err != nil {
		return oops.Wrapf(err, "")
	}
	d.SetId(path)

	// Retrieve object_id by reading.
	if err := resourceDirectoryRead(d, m); err != nil {
		return oops.Wrapf(err, "")
	}
	return nil
}

func resourceDirectoryRead(d *schema.ResourceData, m interface{}) error {
	client := m.(databricks.API)
	path := d.Get("path").(string)
	status, err := client.WorkspaceGetStatus(context.Background(), &databricks.WorkspaceGetStatusInput{
		Path: path,
	})
	if err != nil {
		if aerr, ok := oops.Cause(err).(*databricks.APIError); ok {
			if aerr.ErrorCode == databricks.ErrorCodeResourceDoesNotExist {
				d.SetId("")
				return nil
			}
		}
		return oops.Wrapf(err, "")
	}
	if status.ObjectType != databricks.WorkspaceObjectTypeDirectory {
		return oops.Errorf("%s is not a directory", path)
	}
	d.SetId(path)
	if err := d.Set("object_id", status.ObjectId); err != nil {
		return oops.Wrapf(err, "object_id")
	}
	return nil
}

func resourceDirectoryDelete(d *schema.ResourceData, m interface{}) error {
	client := m.(databricks.API)
	path := d.Get("path").(string)
	if _, err := client.WorkspaceDelete(context.Background(), &databricks.WorkspaceDeleteInput{
		Path: path,
	}); err != nil {
		if aerr, ok := oops.Cause(err).(*databricks.APIError); ok {
			if aerr.ErrorCode == databricks.ErrorCodeResourceDoesNotExist {
				d.SetId("")
				return nil
			} else if aerr.ErrorCode == databricks.ErrorCodeDirectoryNotEmpty {
				// This 'else if' is slightly hacky and a temporary mitigation to an issue with how we create databricks directories as tf resource
				// More info in JIRA: https://samsara.atlassian-us-gov-mod.net/browse/DATAPLAT-265
				d.SetId("")
				return nil
			}
		}
		return oops.Wrapf(err, "")
	}
	return nil
}

func resourceDirectoryImport(d *schema.ResourceData, m interface{}) ([]*schema.ResourceData, error) {
	if err := resourceDirectoryRead(d, m); err != nil {
		return nil, err
	}
	return []*schema.ResourceData{d}, nil
}
