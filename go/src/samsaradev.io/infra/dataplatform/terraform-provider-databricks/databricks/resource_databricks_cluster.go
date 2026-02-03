package databricks

import (
	"context"
	"sort"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/dataplatform/ni/sparkversion"
	"samsaradev.io/libs/ni/pointer"
)

var clusterSchema = map[string]*schema.Schema{
	"cluster_name": &schema.Schema{
		Type:     schema.TypeString,
		Required: true,
	},
	"num_workers": &schema.Schema{
		Type:          schema.TypeInt,
		Optional:      true,
		ConflictsWith: []string{"autoscale"},
	},
	"autoscale": &schema.Schema{
		Type:     schema.TypeList,
		Optional: true,
		MinItems: 0,
		MaxItems: 1,
		Elem: &schema.Resource{
			Schema: map[string]*schema.Schema{
				"min_workers": &schema.Schema{
					Type:     schema.TypeInt,
					Required: true,
				},
				"max_workers": &schema.Schema{
					Type:     schema.TypeInt,
					Required: true,
				},
			},
		},
	},
	"spark_version": &schema.Schema{
		Type:     schema.TypeString,
		Required: true,
	},
	"spark_conf": &schema.Schema{
		Type:     schema.TypeMap,
		Optional: true,
		Elem: &schema.Schema{
			Type: schema.TypeString,
		},
	},
	"spark_env_vars": &schema.Schema{
		Type:     schema.TypeMap,
		Optional: true,
		Elem: &schema.Schema{
			Type: schema.TypeString,
		},
	},
	"aws_attributes": &schema.Schema{
		Type:     schema.TypeList,
		Optional: true,
		MinItems: 0,
		MaxItems: 1,
		Elem: &schema.Resource{
			Schema: map[string]*schema.Schema{
				"instance_profile_arn": &schema.Schema{
					Type:     schema.TypeString,
					Optional: true,
				},
				"first_on_demand": &schema.Schema{
					Type:     schema.TypeInt,
					Optional: true,
				},
				"zone_id": &schema.Schema{
					Type:     schema.TypeString,
					Optional: true,
					Computed: true,
				},
			},
		},
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
	"node_type_id": &schema.Schema{
		Type:          schema.TypeString,
		Optional:      true,
		ConflictsWith: []string{"instance_pool_id"},
	},
	"driver_node_type_id": &schema.Schema{
		Type:          schema.TypeString,
		Optional:      true,
		ConflictsWith: []string{"instance_pool_id"},
	},
	"cluster_log_conf": &schema.Schema{
		Type:     schema.TypeList,
		Optional: true,
		MinItems: 0,
		MaxItems: 1,
		Elem: &schema.Resource{
			Schema: map[string]*schema.Schema{
				"dbfs": &schema.Schema{
					Type:     schema.TypeList,
					Optional: true,
					MinItems: 0,
					MaxItems: 1,
					Elem: &schema.Resource{
						Schema: map[string]*schema.Schema{
							"destination": &schema.Schema{
								Type:     schema.TypeString,
								Required: true,
							},
						},
					},
				},
				"s3": &schema.Schema{
					Type:     schema.TypeList,
					Optional: true,
					MinItems: 0,
					MaxItems: 1,
					Elem: &schema.Resource{
						Schema: map[string]*schema.Schema{
							"destination": &schema.Schema{
								Type:     schema.TypeString,
								Required: true,
							},
							"region": &schema.Schema{
								Type:     schema.TypeString,
								Required: true,
							},
						},
					},
				},
			},
		},
	},
	"init_script": &schema.Schema{
		Type:     schema.TypeList,
		Optional: true,
		Elem: &schema.Resource{
			Schema: map[string]*schema.Schema{
				"dbfs": &schema.Schema{
					Type:     schema.TypeList,
					Optional: true,
					MinItems: 0,
					MaxItems: 1,
					Elem: &schema.Resource{
						Schema: map[string]*schema.Schema{
							"destination": &schema.Schema{
								Type:     schema.TypeString,
								Required: true,
							},
						},
					},
				},
				"volumes": &schema.Schema{
					Type:     schema.TypeList,
					Optional: true,
					MinItems: 0,
					MaxItems: 1,
					Elem: &schema.Resource{
						Schema: map[string]*schema.Schema{
							"destination": &schema.Schema{
								Type:     schema.TypeString,
								Required: true,
							},
						},
					},
				},
				"s3": &schema.Schema{
					Type:     schema.TypeList,
					Optional: true,
					MinItems: 0,
					MaxItems: 1,
					Elem: &schema.Resource{
						Schema: map[string]*schema.Schema{
							"destination": &schema.Schema{
								Type:     schema.TypeString,
								Required: true,
							},
							"region": &schema.Schema{
								Type:     schema.TypeString,
								Required: true,
							},
						},
					},
				},
			},
		},
	},
	"enable_elastic_disk": &schema.Schema{
		Type:          schema.TypeBool,
		Optional:      true,
		ConflictsWith: []string{"instance_pool_id"},
	},
	"instance_pool_id": &schema.Schema{
		Type:     schema.TypeString,
		Optional: true,
	},
	"driver_instance_pool_id": &schema.Schema{
		Type:     schema.TypeString,
		Optional: true,
	},
	"autotermination_minutes": &schema.Schema{
		Type:     schema.TypeInt,
		Optional: true,
	},
	"enable_local_disk_encryption": &schema.Schema{
		Type:     schema.TypeBool,
		Optional: true,
	},
	// Pin is not actually a field in the databricks cluster API
	// It is added here to signal update to call the pin API endpoint
	"pin": &schema.Schema{
		Type:     schema.TypeBool,
		Optional: true,
	},
	"workload_type": &schema.Schema{
		Type:     schema.TypeList,
		Optional: true,
		MinItems: 0,
		MaxItems: 1,
		Elem: &schema.Resource{
			Schema: map[string]*schema.Schema{
				"clients": &schema.Schema{
					Type:     schema.TypeList,
					Optional: true,
					MinItems: 0,
					MaxItems: 1,
					Elem: &schema.Resource{
						Schema: map[string]*schema.Schema{
							"notebooks": &schema.Schema{
								Type:     schema.TypeBool,
								Optional: true,
							},
							"jobs": &schema.Schema{
								Type:     schema.TypeBool,
								Optional: true,
							},
						},
					},
				},
			},
		},
	},
	"data_security_mode": &schema.Schema{
		Type:     schema.TypeString,
		Optional: true,
	},
	"runtime_engine": &schema.Schema{
		Type:     schema.TypeString,
		Optional: true,
	},
	"single_user_name": &schema.Schema{
		Type:     schema.TypeString,
		Optional: true,
	},
	"is_single_node": &schema.Schema{
		Type:          schema.TypeBool,
		Optional:      true,
		ConflictsWith: []string{"num_workers", "autoscale"},
	},
	"kind": &schema.Schema{
		Type:     schema.TypeString,
		Optional: true,
	},
}

func resourceCluster() *schema.Resource {
	return &schema.Resource{
		Create: resourceClusterCreate,
		Read:   resourceClusterRead,
		Update: resourceClusterUpdate,
		Delete: resourceClusterDelete,
		Importer: &schema.ResourceImporter{
			State: resourceClusterImport,
		},
		Schema: clusterSchema,
	}
}

func clusterResourceDataToMap(d *schema.ResourceData) map[string]interface{} {
	m := make(map[string]interface{})
	for field := range clusterSchema {
		m[field] = d.Get(field)
	}
	return m
}

func expandClusterSpec(d map[string]interface{}) *databricks.ClusterSpec {
	var spec databricks.ClusterSpec
	if v, ok := d["cluster_name"]; ok {
		spec.ClusterName = v.(string)
	}
	if v, ok := d["num_workers"]; ok {
		spec.NumWorkers = pointer.IntPtr(v.(int))
	}
	if v, ok := d["autoscale"]; ok && len(v.([]interface{})) == 1 {
		autoscale := v.([]interface{})[0].(map[string]interface{})
		spec.AutoScale = &databricks.ClusterAutoScale{
			MinWorkers: autoscale["min_workers"].(int),
			MaxWorkers: autoscale["max_workers"].(int),
		}
	}
	if v, ok := d["spark_version"]; ok {
		spec.SparkVersion = sparkversion.SparkVersion(v.(string))
	}
	if v, ok := d["spark_conf"]; ok && len(v.(map[string]interface{})) > 0 {
		spec.SparkConf = make(map[string]string)
		for k, v := range v.(map[string]interface{}) {
			spec.SparkConf[k] = v.(string)
		}
	}
	if v, ok := d["spark_env_vars"]; ok && len(v.(map[string]interface{})) > 0 {
		spec.SparkEnvVars = make(map[string]string)
		for k, v := range v.(map[string]interface{}) {
			spec.SparkEnvVars[k] = v.(string)
		}
	}
	if v, ok := d["aws_attributes"]; ok && len(v.([]interface{})) == 1 {
		attributes := v.([]interface{})[0].(map[string]interface{})
		spec.AwsAttributes = &databricks.ClusterAwsAttributes{
			InstanceProfileArn: attributes["instance_profile_arn"].(string),
			FirstOnDemand:      attributes["first_on_demand"].(int),
			ZoneId:             attributes["zone_id"].(string),
		}
	}
	if v, ok := d["custom_tag"]; ok {
		spec.CustomTags = make(map[string]string)
		for _, v := range v.(*schema.Set).List() {
			entry := v.(map[string]interface{})
			spec.CustomTags[entry["key"].(string)] = entry["value"].(string)
		}
	}
	if v, ok := d["node_type_id"]; ok {
		spec.NodeTypeId = v.(string)
	}
	if v, ok := d["driver_node_type_id"]; ok {
		spec.DriverNodeTypeId = v.(string)
	}
	if v, ok := d["cluster_log_conf"]; ok && len(v.([]interface{})) == 1 && v.([]interface{})[0] != nil {
		conf := v.([]interface{})[0].(map[string]interface{})

		if v, ok := conf["dbfs"]; ok && len(v.([]interface{})) == 1 {
			dbfs := v.([]interface{})[0].(map[string]interface{})

			spec.ClusterLogConf = &databricks.ClusterLogConf{
				DBFS: &databricks.DBFSStorageInfo{
					Destination: dbfs["destination"].(string),
				},
			}
		}

		if v, ok := conf["s3"]; ok && len(v.([]interface{})) == 1 {
			s3 := v.([]interface{})[0].(map[string]interface{})

			spec.ClusterLogConf = &databricks.ClusterLogConf{
				S3: &databricks.S3StorageInfo{
					Destination: s3["destination"].(string),
					Region:      s3["region"].(string),
				},
			}
		}
	}
	if v, ok := d["init_script"]; ok {
		for _, scriptIface := range v.([]interface{}) {
			script := scriptIface.(map[string]interface{})

			if v, ok := script["dbfs"]; ok && len(v.([]interface{})) == 1 {
				dbfs := v.([]interface{})[0].(map[string]interface{})
				spec.InitScripts = append(spec.InitScripts, &databricks.InitScript{
					DBFS: &databricks.DBFSStorageInfo{
						Destination: dbfs["destination"].(string),
					},
				})
			}
			if v, ok := script["s3"]; ok && len(v.([]interface{})) == 1 {
				s3 := v.([]interface{})[0].(map[string]interface{})
				spec.InitScripts = append(spec.InitScripts, &databricks.InitScript{
					S3: &databricks.S3StorageInfo{
						Destination: s3["destination"].(string),
						Region:      s3["region"].(string),
					},
				})
			}
			if v, ok := script["volumes"]; ok && len(v.([]interface{})) == 1 {
				s3 := v.([]interface{})[0].(map[string]interface{})
				spec.InitScripts = append(spec.InitScripts, &databricks.InitScript{
					Volumes: &databricks.VolumeStorageInfo{
						Destination: s3["destination"].(string),
					},
				})
			}
		}
	}
	if v, ok := d["enable_elastic_disk"]; ok {
		spec.EnableElasticDisk = v.(bool)
	}
	if v, ok := d["instance_pool_id"]; ok {
		spec.InstancePoolId = v.(string)
	}
	if v, ok := d["driver_instance_pool_id"]; ok {
		spec.DriverInstancePoolId = v.(string)
	}
	if v, ok := d["autotermination_minutes"]; ok {
		spec.AutoTerminationMinutes = v.(int)
	}
	if v, ok := d["enable_local_disk_encryption"]; ok {
		spec.EnableLocalDiskEncryption = v.(bool)
	}
	if v, ok := d["pin"]; ok {
		spec.Pin = v.(bool)
	}
	if v, ok := d["workload_type"]; ok && len(v.([]interface{})) == 1 && v.([]interface{})[0] != nil {
		var workloadType databricks.WorkloadTypes
		wt := v.([]interface{})[0].(map[string]interface{})

		if v, ok := wt["clients"]; ok && len(v.([]interface{})) == 1 && v.([]interface{})[0] != nil {
			clients := v.([]interface{})[0].(map[string]interface{})
			var clientTypes databricks.ClientTypes
			if v, ok := clients["notebooks"]; ok {
				clientTypes.Notebooks = v.(bool)
			}
			if v, ok := clients["jobs"]; ok {
				clientTypes.Jobs = v.(bool)
			}
			workloadType.Clients = clientTypes
		}

		spec.WorkloadType = &workloadType
	}
	if v, ok := d["data_security_mode"]; ok {
		spec.DataSecurityMode = v.(string)
	}
	if v, ok := d["runtime_engine"]; ok {
		if runtimeEngine, ok := v.(string); ok {
			spec.RuntimeEngine = runtimeEngine
		}
	}
	if v, ok := d["single_user_name"]; ok {
		spec.SingleUserName = v.(string)
	}
	if v, ok := d["is_single_node"]; ok && v.(bool) {
		spec.IsSingleNode = pointer.BoolPtr(v.(bool))
	}
	if v, ok := d["kind"]; ok && v.(string) != "" {
		spec.Kind = pointer.StringPtr(v.(string))
	}

	return &spec
}

func flattenClusterSpec(cluster *databricks.ClusterSpec) map[string]interface{} {
	m := make(map[string]interface{})
	if cluster.ClusterName != "" {
		m["cluster_name"] = cluster.ClusterName
	} else {
		m["cluster_name"] = nil
	}
	m["num_workers"] = cluster.NumWorkers
	if cluster.AutoScale != nil {
		m["autoscale"] = []interface{}{
			map[string]interface{}{
				"min_workers": cluster.AutoScale.MinWorkers,
				"max_workers": cluster.AutoScale.MaxWorkers,
			},
		}
	} else {
		m["autoscale"] = nil
	}
	if cluster.SparkVersion != sparkversion.SparkVersion("") {
		m["spark_version"] = string(cluster.SparkVersion)
	} else {
		m["spark_version"] = nil
	}
	if cluster.SparkConf != nil {
		m["spark_conf"] = cluster.SparkConf
	} else {
		m["spark_conf"] = nil
	}
	if cluster.SparkEnvVars != nil {
		m["spark_env_vars"] = cluster.SparkEnvVars
	} else {
		m["spark_env_vars"] = nil
	}
	if cluster.AwsAttributes != nil {
		m["aws_attributes"] = []interface{}{
			map[string]interface{}{
				"instance_profile_arn": cluster.AwsAttributes.InstanceProfileArn,
				"first_on_demand":      cluster.AwsAttributes.FirstOnDemand,
				"zone_id":              cluster.AwsAttributes.ZoneId,
			},
		}
	} else {
		m["aws_attributes"] = nil
	}
	if len(cluster.CustomTags) != 0 {
		var tags []interface{}
		for k, v := range cluster.CustomTags {
			tags = append(tags, map[string]interface{}{
				"key":   k,
				"value": v,
			})
		}
		// Maintain stable output order.
		sort.Slice(tags, func(i, j int) bool {
			return tags[i].(map[string]interface{})["key"].(string) < tags[j].(map[string]interface{})["key"].(string)
		})
		m["custom_tag"] = tags
	}
	if cluster.NodeTypeId != "" {
		m["node_type_id"] = cluster.NodeTypeId
	} else {
		m["node_type_id"] = nil
	}
	if cluster.DriverNodeTypeId != "" {
		m["driver_node_type_id"] = cluster.DriverNodeTypeId
	} else {
		m["driver_node_type_id"] = nil
	}
	if cluster.ClusterLogConf != nil {
		if cluster.ClusterLogConf.DBFS != nil {
			m["cluster_log_conf"] = []interface{}{
				map[string]interface{}{
					"dbfs": []interface{}{
						map[string]interface{}{
							"destination": cluster.ClusterLogConf.DBFS.Destination,
						},
					},
				},
			}
		} else if cluster.ClusterLogConf.S3 != nil {
			m["cluster_log_conf"] = []interface{}{
				map[string]interface{}{
					"s3": []interface{}{
						map[string]interface{}{
							"destination": cluster.ClusterLogConf.S3.Destination,
							"region":      cluster.ClusterLogConf.S3.Region,
						},
					},
				},
			}
		}
	} else {
		m["cluster_log_conf"] = nil
	}

	if len(cluster.InitScripts) != 0 {
		var scripts []interface{}
		for _, s := range cluster.InitScripts {
			if s.DBFS != nil {
				scripts = append(scripts, map[string]interface{}{
					"dbfs": []interface{}{
						map[string]interface{}{
							"destination": s.DBFS.Destination,
						},
					},
				})
			} else if s.S3 != nil {
				scripts = append(scripts, map[string]interface{}{
					"s3": []interface{}{
						map[string]interface{}{
							"destination": s.S3.Destination,
							"region":      s.S3.Region,
						},
					},
				})
			} else if s.Volumes != nil {
				scripts = append(scripts, map[string]interface{}{
					"volumes": []interface{}{
						map[string]interface{}{
							"destination": s.Volumes.Destination,
						},
					},
				})
			}
		}
		m["init_script"] = scripts
	} else {
		m["init_script"] = nil
	}

	if cluster.EnableElasticDisk {
		m["enable_elastic_disk"] = cluster.EnableElasticDisk
	} else {
		m["enable_elastic_disk"] = nil
	}
	if cluster.InstancePoolId != "" {
		m["instance_pool_id"] = cluster.InstancePoolId
	} else {
		m["instance_pool_id"] = nil
	}
	if cluster.DriverInstancePoolId != "" {
		m["driver_instance_pool_id"] = cluster.DriverInstancePoolId
	} else {
		m["driver_instance_pool_id"] = nil
	}
	if cluster.AutoTerminationMinutes != 0 {
		m["autotermination_minutes"] = cluster.AutoTerminationMinutes
	} else {
		m["autotermination_minutes"] = 0
	}
	m["enable_local_disk_encryption"] = cluster.EnableLocalDiskEncryption
	m["data_security_mode"] = cluster.DataSecurityMode
	m["runtime_engine"] = cluster.RuntimeEngine
	m["single_user_name"] = cluster.SingleUserName
	if cluster.IsSingleNode != nil {
		m["is_single_node"] = *cluster.IsSingleNode
	} else {
		m["is_single_node"] = nil
	}
	if cluster.Kind != nil {
		m["kind"] = *cluster.Kind
	} else {
		m["kind"] = nil
	}

	if cluster.WorkloadType != nil {
		m["workload_type"] = []interface{}{
			map[string]interface{}{
				"clients": []interface{}{
					map[string]interface{}{
						"notebooks": cluster.WorkloadType.Clients.Notebooks,
						"jobs":      cluster.WorkloadType.Clients.Jobs,
					},
				},
			},
		}
	} else {
		m["workload_type"] = nil
	}
	return m
}

func resourceClusterCreate(d *schema.ResourceData, m interface{}) error {
	client := m.(databricks.API)
	spec := expandClusterSpec(clusterResourceDataToMap(d))

	out, err := client.CreateCluster(context.Background(), &databricks.CreateClusterInput{
		ClusterSpec: *spec,
	})
	if err != nil {
		return oops.Wrapf(err, "")
	}

	d.SetId(out.ClusterId)
	return nil
}

func resourceClusterRead(d *schema.ResourceData, m interface{}) error {
	client := m.(databricks.API)
	out, err := client.GetCluster(context.Background(), &databricks.GetClusterInput{
		ClusterId: d.Id(),
	})
	if err != nil {
		if apiErr, ok := oops.Cause(err).(*databricks.APIError); ok {
			if strings.Contains(apiErr.Message, "does not exist") {
				d.SetId("")
				return nil
			}
		}
		return oops.Wrapf(err, "")
	}

	kv := flattenClusterSpec(&out.ClusterSpec)
	for k, v := range kv {
		if err := d.Set(k, v); err != nil {
			return oops.Wrapf(err, "%+v: %+v", k, v)
		}
	}
	return nil
}

func resourceClusterUpdate(d *schema.ResourceData, m interface{}) error {
	spec := expandClusterSpec(clusterResourceDataToMap(d))
	client := m.(databricks.API)
	if _, err := client.EditCluster(context.Background(), &databricks.EditClusterInput{
		ClusterId:   d.Id(),
		ClusterSpec: *spec,
	}); err != nil {
		return oops.Wrapf(err, "")
	}
	if spec.Pin {
		if _, err := client.PinCluster(context.Background(), &databricks.PinClusterInput{
			ClusterId: d.Id(),
		}); err != nil {
			return oops.Wrapf(err, "")
		}
	} else {
		if _, err := client.UnpinCluster(context.Background(), &databricks.UnpinClusterInput{
			ClusterId: d.Id(),
		}); err != nil {
			return oops.Wrapf(err, "")
		}
	}
	return nil
}

func resourceClusterDelete(d *schema.ResourceData, m interface{}) error {
	ctx := context.Background()
	client := m.(databricks.API)

	// Attempt to unpin a cluster first before deleting
	// We swallow the error here because a cluster might not be pinned in which case
	// we still want to continue and delete it
	client.UnpinCluster(ctx, &databricks.UnpinClusterInput{
		ClusterId: d.Id(),
	})

	if _, err := client.PermanentlyDeleteCluster(ctx, &databricks.PermanentlyDeleteClusterInput{
		ClusterId: d.Id(),
	}); err != nil {
		return err
	}
	return nil
}

func resourceClusterImport(d *schema.ResourceData, m interface{}) ([]*schema.ResourceData, error) {
	if err := resourceClusterRead(d, m); err != nil {
		return nil, err
	}
	return []*schema.ResourceData{d}, nil
}
