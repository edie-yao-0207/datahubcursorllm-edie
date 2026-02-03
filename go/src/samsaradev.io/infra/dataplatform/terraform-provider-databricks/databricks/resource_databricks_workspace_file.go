package databricks

import (
	"context"

	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/dataplatform/databricks"
)

var fileSchema = map[string]*schema.Schema{
	"path": &schema.Schema{
		Type:     schema.TypeString,
		ForceNew: true,
		Required: true,
	},
	"language": &schema.Schema{
		Type:     schema.TypeString,
		Optional: true,
	},
	"content": &schema.Schema{
		Type:     schema.TypeString,
		Required: true,
	},
	"object_id": &schema.Schema{
		Type:     schema.TypeInt,
		Computed: true,
	},
	"format": &schema.Schema{
		Type:     schema.TypeString,
		Optional: true,
	},
}

func resourceFile() *schema.Resource {
	return &schema.Resource{
		Create: resourceFileCreate,
		Read:   resourceFileRead,
		Update: resourceFileUpdate,
		Delete: resourceFileDelete,
		Importer: &schema.ResourceImporter{
			State: resourceFileImport,
		},
		Schema: fileSchema,
	}
}

func importFile(d *schema.ResourceData, m interface{}, overwrite bool) error {
	client := m.(databricks.API)
	path := d.Get("path").(string)
	if _, err := client.WorkspaceImport(context.Background(), &databricks.WorkspaceImportInput{
		Path:      path,
		Format:    databricks.ExportFormat(d.Get("format").(string)),
		Language:  databricks.NotebookLanguage(d.Get("language").(string)),
		Content:   []byte(d.Get("content").(string)),
		Overwrite: overwrite,
	}); err != nil {
		return oops.Wrapf(err, "")
	}
	d.SetId(path)
	return nil
}

func resourceFileCreate(d *schema.ResourceData, m interface{}) error {
	if err := importFile(d, m, false); err != nil {
		return oops.Wrapf(err, "")
	}
	if err := resourceFileRead(d, m); err != nil {
		return oops.Wrapf(err, "")
	}
	return nil
}

func resourceFileUpdate(d *schema.ResourceData, m interface{}) error {
	if err := importFile(d, m, true); err != nil {
		return oops.Wrapf(err, "")
	}
	if err := resourceFileRead(d, m); err != nil {
		return oops.Wrapf(err, "")
	}
	return nil
}

func resourceFileRead(d *schema.ResourceData, m interface{}) error {
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
	if status.ObjectType != databricks.WorkspaceObjectTypeFile {
		return oops.Errorf("%s is not a file", path)
	}
	d.SetId(path)
	if err := d.Set("language", status.Language); err != nil {
		return oops.Wrapf(err, "")
	}
	if err := d.Set("object_id", status.ObjectId); err != nil {
		return oops.Wrapf(err, "")
	}

	export, err := client.WorkspaceExport(context.Background(), &databricks.WorkspaceExportInput{
		Path:   path,
		Format: databricks.ExportFormatAuto,
	})
	if err != nil {
		return oops.Wrapf(err, "")
	}
	if err := d.Set("content", string(export.Content)); err != nil {
		return oops.Wrapf(err, "")
	}
	return nil
}

func resourceFileDelete(d *schema.ResourceData, m interface{}) error {
	client := m.(databricks.API)
	path := d.Get("path").(string)
	if _, err := client.WorkspaceDelete(context.Background(), &databricks.WorkspaceDeleteInput{
		Path: path,
	}); err != nil {
		if aerr, ok := oops.Cause(err).(*databricks.APIError); ok {
			if aerr.ErrorCode == databricks.ErrorCodeResourceDoesNotExist {
				d.SetId("")
				return nil
			}
		}
		return oops.Wrapf(err, "")
	}
	return nil
}

func resourceFileImport(d *schema.ResourceData, m interface{}) ([]*schema.ResourceData, error) {
	if err := resourceFileRead(d, m); err != nil {
		return nil, err
	}
	return []*schema.ResourceData{d}, nil
}
