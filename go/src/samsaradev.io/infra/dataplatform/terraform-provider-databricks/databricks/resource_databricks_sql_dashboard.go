package databricks

import (
	"context"

	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/dataplatform/databricks"
)

var sqlDashboardSchema = map[string]*schema.Schema{
	"name": {
		Type:     schema.TypeString,
		Required: true,
	},
	"tags": {
		Type:     schema.TypeSet,
		Optional: true,
		Elem: &schema.Schema{
			Type: schema.TypeString,
		},
	},
}

func expandSqlDashboard(d map[string]interface{}) *databricks.SqlDashboard {
	var spec databricks.SqlDashboard
	if v, ok := d["name"]; ok {
		spec.Name = v.(string)
	}

	if v, ok := d["tags"]; ok {
		specTags := v.(*schema.Set)
		for _, tag := range specTags.List() {
			spec.Tags = append(spec.Tags, tag.(string))
		}
	}
	return &spec
}

func flattenSqlDashboard(dash *databricks.SqlDashboard) map[string]interface{} {
	m := make(map[string]interface{})
	m["name"] = dash.Name

	var tags []interface{}
	for _, t := range dash.Tags {
		tags = append(tags, string(t))
	}
	tagSet := schema.NewSet(stringHash, tags)
	m["tags"] = tagSet
	return m
}

func dashboardResourceDataToMap(d *schema.ResourceData) map[string]interface{} {
	m := make(map[string]interface{})
	for field := range sqlDashboardSchema {
		m[field] = d.Get(field)
	}
	return m
}

func resourceSqlDashboard() *schema.Resource {
	return &schema.Resource{
		Schema: sqlDashboardSchema,
		Create: resourceSqlDashboardCreate,
		Read:   resourceSqlDashboardRead,
		Update: resourceSqlDashboardUpdate,
		Delete: resourceSqlDashboardDelete,
		Importer: &schema.ResourceImporter{
			State: resourceSqlDashboardImport,
		},
	}
}

func resourceSqlDashboardCreate(d *schema.ResourceData, m interface{}) error {
	client := m.(databricks.API)
	dashboardMap := dashboardResourceDataToMap(d)
	sqlDashboard := expandSqlDashboard(dashboardMap)

	newDashboard, err := client.CreateSqlDashboard(context.Background(), sqlDashboard)
	if err != nil {
		return oops.Wrapf(err, "")
	}

	d.SetId(newDashboard.Id)
	return nil
}

func resourceSqlDashboardRead(d *schema.ResourceData, m interface{}) error {
	client := m.(databricks.API)
	out, err := client.GetSqlDashboard(context.Background(), d.Id())
	if err != nil {
		if apiErr, ok := oops.Cause(err).(*databricks.APIError); ok {
			if apiErr.ErrorCode == databricks.ErrorCodeResourceDoesNotExist {
				d.SetId("")
				return nil
			}
		}
		return oops.Wrapf(err, "")
	}

	kv := flattenSqlDashboard(out)
	for k, v := range kv {
		if err := d.Set(k, v); err != nil {
			return oops.Wrapf(err, "%+v: %+v", k, v)
		}
	}
	return nil
}

func resourceSqlDashboardUpdate(d *schema.ResourceData, m interface{}) error {
	client := m.(databricks.API)
	dashboardMap := dashboardResourceDataToMap(d)
	sqlDashboard := expandSqlDashboard(dashboardMap)

	if err := client.EditSqlDashboard(context.Background(), d.Id(), sqlDashboard); err != nil {
		return oops.Wrapf(err, "")
	}
	return nil
}

func resourceSqlDashboardDelete(d *schema.ResourceData, m interface{}) error {
	client := m.(databricks.API)
	if err := client.DeleteSqlDashboard(context.Background(), d.Id()); err != nil {
		return err
	}
	return nil
}

func resourceSqlDashboardImport(d *schema.ResourceData, m interface{}) ([]*schema.ResourceData, error) {
	if err := resourceSqlDashboardRead(d, m); err != nil {
		return nil, err
	}
	return []*schema.ResourceData{d}, nil
}
