package databricks

import (
	"context"
	"fmt"

	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/dataplatform/ni/sparkversion"
)

var instancePoolSchema = map[string]*schema.Schema{
	"instance_pool_name": &schema.Schema{
		Type:     schema.TypeString,
		Required: true,
	},
	"min_idle_instances": &schema.Schema{
		Type:     schema.TypeInt,
		Required: true,
	},
	"max_capacity": &schema.Schema{
		Type:     schema.TypeInt,
		Optional: true,
	},
	"node_type_id": &schema.Schema{
		Type:     schema.TypeString,
		Optional: true,
		ForceNew: true,
	},
	"custom_tag": &schema.Schema{
		Type:     schema.TypeSet,
		Optional: true,
		MaxItems: 45,
		ForceNew: true,
		Elem: &schema.Resource{
			Schema: map[string]*schema.Schema{
				"key": &schema.Schema{
					Type:     schema.TypeString,
					ForceNew: true,
					Required: true,
				},
				"value": &schema.Schema{
					Type:     schema.TypeString,
					ForceNew: true,
					Required: true,
				},
			},
		},
	},
	"idle_instance_autotermination_minutes": &schema.Schema{
		Type:     schema.TypeInt,
		Required: true,
	},
	"enable_elastic_disk": &schema.Schema{
		Type:     schema.TypeBool,
		Required: true,
		ForceNew: true,
	},
	"disk_spec": &schema.Schema{
		Type:     schema.TypeList,
		Optional: true,
		MinItems: 0,
		MaxItems: 1,
		ForceNew: true,
		Elem: &schema.Resource{
			Schema: map[string]*schema.Schema{
				"disk_type": &schema.Schema{
					Type:     schema.TypeList,
					Required: true,
					MinItems: 0,
					MaxItems: 1,
					Elem: &schema.Resource{
						Schema: map[string]*schema.Schema{
							"ebs_volume_type": &schema.Schema{
								Type:     schema.TypeString,
								Required: true,
							},
						},
					},
				},
				"disk_count": &schema.Schema{
					Type:     schema.TypeInt,
					Required: true,
				},
				"disk_size": &schema.Schema{
					Type:     schema.TypeInt,
					Required: true,
				},
			},
		},
	},
	"preloaded_spark_versions": &schema.Schema{
		Type:     schema.TypeSet,
		Optional: true,
		ForceNew: true,
		MaxItems: 1,
		Elem: &schema.Schema{
			Type: schema.TypeString,
		},
	},
	"aws_attributes": &schema.Schema{
		Type:     schema.TypeList,
		MinItems: 1,
		MaxItems: 1,
		Required: true,
		ForceNew: true,
		Elem: &schema.Resource{
			Schema: map[string]*schema.Schema{
				"availability": &schema.Schema{
					Type:     schema.TypeString,
					ForceNew: true,
					Required: true,
					// We should use the official helper
					// github.com/hashicorp/terraform/helper/validation.StringInSlice but
					// govendor has trouble pulling it down. Consider updating when we
					// switch to using go.mod.
					ValidateFunc: func(i interface{}, k string) ([]string, []error) {
						v, ok := i.(string)
						if !ok {
							return nil, []error{fmt.Errorf("expected type of %s to be string", k)}
						}
						if v != "ON_DEMAND" && v != "SPOT" {
							return nil, []error{fmt.Errorf("expected %s to be one of {ON_DEMAND, SPOT}, got %s", k, v)}
						}
						return nil, nil
					},
				},
				"zone_id": &schema.Schema{
					Type:     schema.TypeString,
					ForceNew: true,
					Optional: true,
					Computed: true,
				},
			},
		},
	},
}

func resourceInstancePool() *schema.Resource {
	return &schema.Resource{
		Create: resourceInstancePoolCreate,
		Read:   resourceInstancePoolRead,
		Update: resourceInstancePoolUpdate,
		Delete: resourceInstancePoolDelete,
		Importer: &schema.ResourceImporter{
			State: resourceInstancePoolImport,
		},
		Schema: instancePoolSchema,
	}
}

func expandDiskSpec(d map[string]interface{}) databricks.DiskSpec {
	return databricks.DiskSpec{
		DiskType: databricks.DiskType{
			EbsVolumeType: databricks.EbsVolumeType(d["disk_type"].([]interface{})[0].(map[string]interface{})["ebs_volume_type"].(string)),
		},
		DiskCount: d["disk_count"].(int),
		DiskSize:  d["disk_size"].(int),
	}
}

func flattenDiskSpec(spec *databricks.DiskSpec) map[string]interface{} {
	return map[string]interface{}{
		"disk_type": []interface{}{
			map[string]interface{}{
				"ebs_volume_type": spec.DiskType.EbsVolumeType,
			},
		},
		"disk_count": spec.DiskCount,
		"disk_size":  spec.DiskSize,
	}
}

func expandInstancePoolAwsAttributes(d map[string]interface{}) *databricks.InstancePoolAwsAttributes {
	return &databricks.InstancePoolAwsAttributes{
		Availability: databricks.AwsAvailability(d["availability"].(string)),
		ZoneId:       d["zone_id"].(string),
	}
}

func flattenInstancePoolAwsAttributes(spec *databricks.InstancePoolAwsAttributes) map[string]interface{} {
	return map[string]interface{}{
		"availability": spec.Availability,
		"zone_id":      spec.ZoneId,
	}
}

func expandInstancePoolSpec(d *schema.ResourceData) databricks.InstancePoolSpec {
	var spec databricks.InstancePoolSpec
	spec.InstancePoolName = d.Get("instance_pool_name").(string)
	spec.MinIdleInstances = d.Get("min_idle_instances").(int)
	spec.NodeTypeId = d.Get("node_type_id").(string)
	spec.IdleInstanceAutoterminationMinutes = d.Get("idle_instance_autotermination_minutes").(int)
	spec.EnableElasticDisk = d.Get("enable_elastic_disk").(bool)

	if v, ok := d.GetOk("max_capacity"); ok {
		maxCap := v.(int)
		spec.MaxCapacity = &maxCap
	}

	if v, ok := d.GetOk("disk_spec"); ok && len(v.([]interface{})) != 0 {
		diskSpec := expandDiskSpec(v.([]interface{})[0].(map[string]interface{}))
		spec.DiskSpec = &diskSpec
	}

	if v, ok := d.GetOk("aws_attributes"); ok && len(v.([]interface{})) != 0 {
		spec.AwsAttributes = expandInstancePoolAwsAttributes(v.([]interface{})[0].(map[string]interface{}))
	}

	if v, ok := d.GetOk("custom_tag"); ok {
		spec.CustomTags = make(map[string]string)
		for _, v := range v.(*schema.Set).List() {
			entry := v.(map[string]interface{})
			spec.CustomTags[entry["key"].(string)] = entry["value"].(string)
		}
	}
	if v, ok := d.GetOk("preloaded_spark_versions"); ok {
		for _, version := range v.(*schema.Set).List() {
			spec.PreloadedSparkVersions = append(spec.PreloadedSparkVersions, sparkversion.SparkVersion(version.(string)))
		}
	}

	return spec
}

func flattenInstancePoolSpec(spec databricks.InstancePoolSpec) map[string]interface{} {
	m := make(map[string]interface{})
	m["instance_pool_name"] = spec.InstancePoolName
	m["min_idle_instances"] = spec.MinIdleInstances
	m["node_type_id"] = spec.NodeTypeId
	m["idle_instance_autotermination_minutes"] = spec.IdleInstanceAutoterminationMinutes
	m["enable_elastic_disk"] = spec.EnableElasticDisk

	if spec.MaxCapacity != nil {
		m["max_capacity"] = *spec.MaxCapacity
	} else {
		m["max_capacity"] = nil
	}

	if spec.DiskSpec != nil {
		m["disk_spec"] = []interface{}{flattenDiskSpec(spec.DiskSpec)}
	} else {
		m["disk_spec"] = nil
	}

	if spec.AwsAttributes != nil {
		m["aws_attributes"] = []interface{}{flattenInstancePoolAwsAttributes(spec.AwsAttributes)}
	} else {
		m["aws_attributes"] = nil
	}

	var tags []interface{}
	for k, v := range spec.CustomTags {
		tags = append(tags, map[string]interface{}{
			"key":   k,
			"value": v,
		})
	}
	m["custom_tag"] = tags

	var versions []interface{}
	for _, v := range spec.PreloadedSparkVersions {
		versions = append(versions, string(v))
	}
	m["preloaded_spark_versions"] = versions

	return m
}

func resourceInstancePoolCreate(d *schema.ResourceData, m interface{}) error {
	client := m.(databricks.API)
	spec := expandInstancePoolSpec(d)
	out, err := client.CreateInstancePool(context.Background(), &databricks.CreateInstancePoolInput{
		InstancePoolSpec: spec,
	})
	if err != nil {
		return oops.Wrapf(err, "")
	}

	d.SetId(out.InstancePoolId)
	return nil
}

func resourceInstancePoolRead(d *schema.ResourceData, m interface{}) error {
	client := m.(databricks.API)
	out, err := client.GetInstancePool(context.Background(), &databricks.GetInstancePoolInput{
		InstancePoolId: d.Id(),
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

	kv := flattenInstancePoolSpec(out.InstancePoolSpec)
	for k, v := range kv {
		if err := d.Set(k, v); err != nil {
			return oops.Wrapf(err, "%+v: %+v", k, v)
		}
	}
	return nil
}

func resourceInstancePoolUpdate(d *schema.ResourceData, m interface{}) error {
	client := m.(databricks.API)
	input := &databricks.EditInstancePoolInput{
		InstancePoolId:                     d.Id(),
		InstancePoolName:                   d.Get("instance_pool_name").(string),
		MinIdleInstances:                   d.Get("min_idle_instances").(int),
		IdleInstanceAutoterminationMinutes: d.Get("idle_instance_autotermination_minutes").(int),
		NodeTypeId:                         d.Get("node_type_id").(string),
	}
	if v, ok := d.GetOk("max_capacity"); ok {
		maxCapacity := v.(int)
		input.MaxCapacity = &maxCapacity
	}
	if _, err := client.EditInstancePool(context.Background(), input); err != nil {
		return oops.Wrapf(err, "")
	}
	return nil
}

func resourceInstancePoolDelete(d *schema.ResourceData, m interface{}) error {
	client := m.(databricks.API)
	if _, err := client.DeleteInstancePool(context.Background(), &databricks.DeleteInstancePoolInput{
		InstancePoolId: d.Id(),
	}); err != nil {
		return err
	}
	return nil
}

func resourceInstancePoolImport(d *schema.ResourceData, m interface{}) ([]*schema.ResourceData, error) {
	if err := resourceInstancePoolRead(d, m); err != nil {
		return nil, err
	}
	return []*schema.ResourceData{d}, nil
}
