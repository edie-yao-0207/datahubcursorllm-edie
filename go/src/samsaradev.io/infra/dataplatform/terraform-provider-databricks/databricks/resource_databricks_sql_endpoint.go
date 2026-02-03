package databricks

import (
	"context"

	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/dataplatform/databricks"
)

var sqlEndpointSchema = map[string]*schema.Schema{
	"name": &schema.Schema{
		Type:     schema.TypeString,
		Required: true,
	},
	"cluster_size": &schema.Schema{
		Type:     schema.TypeString,
		Required: true,
	},
	"min_num_clusters": &schema.Schema{
		Type:     schema.TypeInt,
		Required: true,
	},
	"max_num_clusters": &schema.Schema{
		Type:     schema.TypeInt,
		Required: true,
	},
	"auto_stop_mins": &schema.Schema{
		Type:     schema.TypeInt,
		Required: true,
	},
	"custom_tag": &schema.Schema{
		Type:     schema.TypeSet,
		Optional: true,
		MaxItems: 45,
		Elem: &schema.Resource{
			Schema: map[string]*schema.Schema{
				"key": &schema.Schema{
					Type:     schema.TypeString,
					Required: true,
				},
				"value": &schema.Schema{
					Type:     schema.TypeString,
					Required: true,
				},
			},
		},
	},
	"spot_instance_policy": &schema.Schema{
		Type:     schema.TypeString,
		Required: true,
	},
	"enable_photon": &schema.Schema{
		Type:     schema.TypeBool,
		Required: true,
	},
}

func resourceSqlEndpoint() *schema.Resource {
	return &schema.Resource{
		Create: resourceSqlEndpointCreate,
		Read:   resourceSqlEndpointRead,
		Update: resourceSqlEndpointUpdate,
		Delete: resourceSqlEndpointDelete,
		Importer: &schema.ResourceImporter{
			State: resourceSqlEndpointImport,
		},
		Schema: sqlEndpointSchema,
	}
}

func expandSqlEndpointSpec(d *schema.ResourceData) databricks.SqlEndpointSpec {
	var spec databricks.SqlEndpointSpec
	spec.Name = d.Get("name").(string)
	spec.ClusterSize = databricks.ClusterSize(d.Get("cluster_size").(string))
	spec.MinNumClusters = d.Get("min_num_clusters").(int)
	spec.MaxNumClusters = d.Get("max_num_clusters").(int)
	spec.AutoStopMins = d.Get("auto_stop_mins").(int)
	spec.SpotInstancePolicy = databricks.EndpointSpotInstancePolicy(d.Get("spot_instance_policy").(string))
	spec.EnablePhoton = d.Get("enable_photon").(bool)

	if v, ok := d.GetOk("custom_tag"); ok {
		for _, v := range v.(*schema.Set).List() {
			entry := v.(map[string]interface{})
			spec.Tags.CustomTags = append(spec.Tags.CustomTags, databricks.ClusterTag{
				Key:   entry["key"].(string),
				Value: entry["value"].(string),
			})
		}
	}
	return spec
}

func flattenSqlEndpointSpec(spec databricks.SqlEndpointSpec) map[string]interface{} {
	m := make(map[string]interface{})
	m["name"] = spec.Name
	m["cluster_size"] = spec.ClusterSize
	m["min_num_clusters"] = spec.MinNumClusters
	m["max_num_clusters"] = spec.MaxNumClusters
	m["auto_stop_mins"] = spec.AutoStopMins
	m["spot_instance_policy"] = spec.SpotInstancePolicy
	m["enable_photon"] = spec.EnablePhoton

	var tags []interface{}
	for _, tag := range spec.Tags.CustomTags {
		tags = append(tags, map[string]interface{}{
			"key":   tag.Key,
			"value": tag.Value,
		})
	}
	m["custom_tag"] = tags
	return m
}

func resourceSqlEndpointCreate(d *schema.ResourceData, m interface{}) error {
	client := m.(databricks.API)
	spec := expandSqlEndpointSpec(d)
	out, err := client.CreateSqlEndpoint(context.Background(), &databricks.CreateSqlEndpointInput{
		SqlEndpointSpec: spec,
	})
	if err != nil {
		return oops.Wrapf(err, "")
	}

	d.SetId(out.Id)
	return nil
}

func resourceSqlEndpointRead(d *schema.ResourceData, m interface{}) error {
	client := m.(databricks.API)
	out, err := client.GetSqlEndpoint(context.Background(), &databricks.GetSqlEndpointInput{
		Id: d.Id(),
	})
	if err != nil {
		if apiErr, ok := oops.Cause(err).(*databricks.APIError); ok {
			if apiErr.ErrorCode == databricks.ErrorCodeResourceDoesNotExist {
				d.SetId("")
				return nil
			}
		}
		return oops.Wrapf(err, "")
	}

	kv := flattenSqlEndpointSpec(out.SqlEndpointSpec)
	for k, v := range kv {
		if err := d.Set(k, v); err != nil {
			return oops.Wrapf(err, "%+v: %+v", k, v)
		}
	}
	return nil
}

func resourceSqlEndpointUpdate(d *schema.ResourceData, m interface{}) error {
	client := m.(databricks.API)
	spec := expandSqlEndpointSpec(d)
	input := &databricks.EditSqlEndpointInput{
		Id:              d.Id(),
		SqlEndpointSpec: spec,
	}
	if _, err := client.EditSqlEndpoint(context.Background(), input); err != nil {
		return oops.Wrapf(err, "")
	}
	return nil
}

func resourceSqlEndpointDelete(d *schema.ResourceData, m interface{}) error {
	client := m.(databricks.API)
	if _, err := client.DeleteSqlEndpoint(context.Background(), &databricks.DeleteSqlEndpointInput{
		Id: d.Id(),
	}); err != nil {
		return err
	}
	return nil
}

func resourceSqlEndpointImport(d *schema.ResourceData, m interface{}) ([]*schema.ResourceData, error) {
	if err := resourceSqlEndpointRead(d, m); err != nil {
		return nil, err
	}
	return []*schema.ResourceData{d}, nil
}
