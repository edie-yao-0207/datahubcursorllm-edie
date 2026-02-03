package databricks

import (
	"context"
	"strconv"

	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/dataplatform/databricks"
)

var workspaceConfSchema = map[string]*schema.Schema{
	"custom_config": {
		Type:     schema.TypeMap,
		Required: true,
		Elem: &schema.Schema{
			Type: schema.TypeBool,
		},
	},
}

func resourceWorkspaceConf() *schema.Resource {
	return &schema.Resource{
		Create: resourceWorkspaceConfCreate,
		Read:   resourceWorkspaceConfRead,
		Update: resourceWorkspaceConfUpdate,
		Delete: resourceWorkspaceConfDelete,
		Importer: &schema.ResourceImporter{
			State: resourceWorkspaceConfImport,
		},
		Schema: workspaceConfSchema,
	}
}

func resourceWorkspaceConfCreate(d *schema.ResourceData, m interface{}) error {
	// Databricks Workspace Conf is not created. It exists with each workspace.
	// When we define it in terraform for the first time we want to update it in Databricks.
	// Before we update it though we want to make sure we can properly read it so we don't create a resource we can't read.
	client := m.(databricks.API)
	_, err := client.GetWorkspaceConf(context.Background())
	if err != nil {
		return oops.Wrapf(err, "Unable to read workspace-conf inside CREATE for WorkspaceConf resource")
	}
	return resourceWorkspaceConfUpdate(d, m)
}

func resourceWorkspaceConfRead(d *schema.ResourceData, m interface{}) error {
	client := m.(databricks.API)
	out, err := client.GetWorkspaceConf(context.Background())
	if err != nil {
		return oops.Wrapf(err, "")
	}

	enableIPAccessLists, _ := strconv.ParseBool(out.EnableIpAccessLists)
	customConfig := map[databricks.Conf]bool{
		databricks.EnableIPAccessListsConf: enableIPAccessLists,
	}

	// There can only be a single workspace_conf per workspace
	// There is no unique id so set id to workspace_conf
	d.SetId("workspace_conf")

	if err := d.Set("custom_config", customConfig); err != nil {
		return oops.Wrapf(err, "")
	}

	return nil
}

func resourceWorkspaceConfUpdate(d *schema.ResourceData, m interface{}) error {
	spec, err := expandCustomConfigSpec(d.Get("custom_config"))
	if err != nil {
		return oops.Wrapf(err, "")
	}

	client := m.(databricks.API)
	err = client.EditWorkspaceConf(context.Background(), &databricks.EditWorkspaceConfInput{
		WorkspaceConf: *spec,
	})
	if err != nil {
		return oops.Wrapf(err, "")
	}

	// There can only be a single workspace_conf per workspace
	// There is no unique id so set id to workspace_conf
	d.SetId("workspace_conf")

	if err := d.Set("custom_config", d.Get("custom_config")); err != nil {
		return oops.Wrapf(err, "")
	}
	return nil
}

func resourceWorkspaceConfDelete(d *schema.ResourceData, m interface{}) error {
	// Databricks Workspace Configurations don't get deleted. Return nil.
	return nil
}

func resourceWorkspaceConfImport(d *schema.ResourceData, m interface{}) ([]*schema.ResourceData, error) {
	if err := resourceWorkspaceConfRead(d, m); err != nil {
		return nil, err
	}
	return []*schema.ResourceData{d}, nil
}

func expandCustomConfigSpec(d interface{}) (*databricks.WorkspaceConf, error) {
	m, _ := d.(map[string]interface{})
	var spec databricks.WorkspaceConf
	spec.CustomConfig = make(map[databricks.Conf]bool, len(m))
	if v, ok := m["enableIpAccessLists"]; ok {
		spec.CustomConfig[databricks.EnableIPAccessListsConf] = v.(bool)
	}
	return &spec, nil
}
