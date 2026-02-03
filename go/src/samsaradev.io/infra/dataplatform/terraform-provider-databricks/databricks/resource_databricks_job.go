package databricks

import (
	"context"
	"sort"
	"strconv"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/dataplatform/databricks"
)

// TODO: how does this differ from the one we have in `resource_databricks_cluster.go`?
// Confusingly, we later in this file reference `flattenClusterSpec` and `expandClusterSpec` which are
// defined in that file.
var newClusterSchema = &schema.Schema{
	Type:     schema.TypeList,
	Optional: true,
	MinItems: 0,
	MaxItems: 1,
	Elem: &schema.Resource{
		Schema: map[string]*schema.Schema{
			"num_workers": &schema.Schema{
				Type:     schema.TypeInt,
				Optional: true,
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
				Optional: true,
			},
			"spark_conf": &schema.Schema{
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
						"first_on_demand": &schema.Schema{
							Type:     schema.TypeInt,
							Optional: true,
						},
						"availability": &schema.Schema{
							Type:     schema.TypeString,
							Optional: true,
						},
						"zone_id": &schema.Schema{
							Type:     schema.TypeString,
							Optional: true,
							Computed: true,
						},
						"instance_profile_arn": &schema.Schema{
							Type:     schema.TypeString,
							Optional: true,
						},
						"spot_bid_price_percent": &schema.Schema{
							Type:     schema.TypeInt,
							Optional: true,
						},
						"ebs_volume_type": &schema.Schema{
							Type:     schema.TypeString,
							Optional: true,
						},
						"ebs_volume_count": &schema.Schema{
							Type:     schema.TypeInt,
							Optional: true,
						},
						"ebs_volume_size": &schema.Schema{
							Type:     schema.TypeInt,
							Optional: true,
						},
						"ebs_volume_iops": &schema.Schema{
							Type:     schema.TypeInt,
							Optional: true,
						},
						"ebs_volume_throughput": &schema.Schema{
							Type:     schema.TypeInt,
							Optional: true,
						},
					},
				},
			},
			"is_single_node": &schema.Schema{
				Type:     schema.TypeBool,
				Computed: true,
			},
			"kind": &schema.Schema{
				Type:     schema.TypeString,
				Computed: true,
			},
			"node_type_id": &schema.Schema{
				Type:     schema.TypeString,
				Optional: true,
			},
			"driver_node_type_id": &schema.Schema{
				Type:     schema.TypeString,
				Optional: true,
			},
			"ssh_public_keys": &schema.Schema{
				Type:     schema.TypeList,
				Optional: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
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
									"endpoint": &schema.Schema{
										Type:     schema.TypeString,
										Optional: true,
									},
									"enable_encryption": &schema.Schema{
										Type:     schema.TypeBool,
										Optional: true,
										Default:  false,
									},
									"encryption_type": &schema.Schema{
										Type:     schema.TypeString,
										Optional: true,
									},
									"kms_key": &schema.Schema{
										Type:     schema.TypeString,
										Optional: true,
									},
									"canned_acl": &schema.Schema{
										Type:     schema.TypeString,
										Optional: true,
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
									"endpoint": &schema.Schema{
										Type:     schema.TypeString,
										Optional: true,
									},
									"enable_encryption": &schema.Schema{
										Type:     schema.TypeBool,
										Optional: true,
										Default:  false,
									},
									"encryption_type": &schema.Schema{
										Type:     schema.TypeString,
										Optional: true,
									},
									"kms_key": &schema.Schema{
										Type:     schema.TypeString,
										Optional: true,
									},
									"canned_acl": &schema.Schema{
										Type:     schema.TypeString,
										Optional: true,
									},
								},
							},
						},
						"file": &schema.Schema{
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
					},
				},
			},
			"spark_env_vars": &schema.Schema{
				Type:     schema.TypeMap,
				Optional: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
			},
			"enable_elastic_disk": &schema.Schema{
				Type:     schema.TypeBool,
				Optional: true,
			},
			"enable_local_disk_encryption": &schema.Schema{
				Type:     schema.TypeBool,
				Optional: true,
			},
			"driver_instance_pool_id": &schema.Schema{
				Type:     schema.TypeString,
				Optional: true,
			},
			"instance_pool_id": &schema.Schema{
				Type:     schema.TypeString,
				Optional: true,
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
			"docker_image": &schema.Schema{
				Type:     schema.TypeList,
				Optional: true,
				MinItems: 0,
				MaxItems: 1,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"url": &schema.Schema{
							Type:     schema.TypeString,
							Required: true,
						},
					},
				},
			},
		},
	},
}
var jobSchema = map[string]*schema.Schema{
	"name": &schema.Schema{
		Type:     schema.TypeString,
		Required: true,
	},
	"email_notifications": &schema.Schema{
		Type:     schema.TypeList,
		Optional: true,
		MinItems: 0,
		MaxItems: 1,
		Elem: &schema.Resource{
			Schema: map[string]*schema.Schema{
				"on_start": &schema.Schema{
					Type:     schema.TypeList,
					Optional: true,
					Elem: &schema.Schema{
						Type: schema.TypeString,
					},
				},
				"on_success": &schema.Schema{
					Type:     schema.TypeList,
					Optional: true,
					Elem: &schema.Schema{
						Type: schema.TypeString,
					},
				},
				"on_failure": &schema.Schema{
					Type:     schema.TypeList,
					Optional: true,
					Elem: &schema.Schema{
						Type: schema.TypeString,
					},
				},
				"no_alert_for_skipped_runs": &schema.Schema{
					Type:     schema.TypeBool,
					Optional: true,
					Default:  true,
				},
			},
		},
	},
	"webhook_notifications": &schema.Schema{
		Type:     schema.TypeList,
		Optional: true,
		MinItems: 0,
		MaxItems: 1,
		Elem: &schema.Resource{
			Schema: map[string]*schema.Schema{
				"on_start": &schema.Schema{
					Type:     schema.TypeList,
					Optional: true,
					MaxItems: 3,
					Elem: &schema.Resource{
						Schema: map[string]*schema.Schema{
							"id": &schema.Schema{
								Type:     schema.TypeString,
								Optional: true,
							},
						},
					},
				},
				"on_success": &schema.Schema{
					Type:     schema.TypeList,
					Optional: true,
					MaxItems: 3,
					Elem: &schema.Resource{
						Schema: map[string]*schema.Schema{
							"id": &schema.Schema{
								Type:     schema.TypeString,
								Optional: true,
							},
						},
					},
				},
				"on_failure": &schema.Schema{
					Type:     schema.TypeList,
					Optional: true,
					MaxItems: 3,
					Elem: &schema.Resource{
						Schema: map[string]*schema.Schema{
							"id": &schema.Schema{
								Type:     schema.TypeString,
								Optional: true,
							},
						},
					},
				},
			},
		},
	},
	"timeout_seconds": &schema.Schema{
		Type:     schema.TypeInt,
		Optional: true,
	},
	"environments": &schema.Schema{
		Type:     schema.TypeList,
		Optional: true,
		Elem: &schema.Resource{
			Schema: map[string]*schema.Schema{
				"environment_key": &schema.Schema{
					Type:     schema.TypeString,
					Required: true,
				},
				"spec": &schema.Schema{
					Type:     schema.TypeList,
					Optional: true,
					Elem: &schema.Resource{
						Schema: map[string]*schema.Schema{
							"client": &schema.Schema{
								Type:     schema.TypeString,
								Required: true,
							},
							"dependencies": &schema.Schema{
								Type:     schema.TypeList,
								Optional: true,
								Elem: &schema.Schema{
									Type: schema.TypeString,
								},
							},
						},
					},
				},
			},
		},
	},
	"schedule": &schema.Schema{
		Type:     schema.TypeList,
		Optional: true,
		MinItems: 0,
		MaxItems: 1,
		Elem: &schema.Resource{
			Schema: map[string]*schema.Schema{
				"quartz_cron_expression": &schema.Schema{
					Type:     schema.TypeString,
					Required: true,
				},
				"timezone_id": &schema.Schema{
					Type:     schema.TypeString,
					Required: true,
				},
				"pause_status": &schema.Schema{
					Type:     schema.TypeString,
					Required: true,
				},
			},
		},
	},
	"format": &schema.Schema{
		Type:     schema.TypeString,
		Optional: true,
	},
	"tasks": &schema.Schema{
		Type:     schema.TypeList,
		Optional: true,
		MinItems: 0,
		Elem: &schema.Resource{
			Schema: map[string]*schema.Schema{
				"task_key": &schema.Schema{
					Type:     schema.TypeString,
					Required: true,
				},
				"description": &schema.Schema{
					Type:     schema.TypeString,
					Optional: true,
				},
				"depends_on": &schema.Schema{
					Type:     schema.TypeList,
					Optional: true,
					MinItems: 1,
					Elem: &schema.Resource{
						Schema: map[string]*schema.Schema{
							"task_key": &schema.Schema{
								Type:     schema.TypeString,
								Required: true,
							},
						},
					},
				},
				"existing_cluster_id": &schema.Schema{
					Type:     schema.TypeString,
					Optional: true,
				},
				"new_cluster": newClusterSchema,
				"job_cluster_key": &schema.Schema{
					Type:     schema.TypeString,
					Optional: true,
				},
				"notebook_task": &schema.Schema{
					Type:     schema.TypeList,
					Optional: true,
					MinItems: 0,
					MaxItems: 1,
					Elem: &schema.Resource{
						Schema: map[string]*schema.Schema{
							"notebook_path": &schema.Schema{
								Type:     schema.TypeString,
								Required: true,
							},
							"base_parameters": &schema.Schema{
								Type:     schema.TypeMap,
								Optional: true,
								Elem: &schema.Schema{
									Type: schema.TypeString,
								},
							},
							"source": &schema.Schema{
								Type:     schema.TypeString,
								Optional: true,
							},
						},
					},
				},
				"spark_jar_task": &schema.Schema{
					Type:     schema.TypeList,
					Optional: true,
					MinItems: 0,
					MaxItems: 1,
					Elem: &schema.Resource{
						Schema: map[string]*schema.Schema{
							"main_class_name": &schema.Schema{
								Type:     schema.TypeString,
								Required: true,
							},
							"parameters": &schema.Schema{
								Type:     schema.TypeList,
								Optional: true,
								Elem: &schema.Schema{
									Type: schema.TypeString,
								},
							},
						},
					},
				},
				"spark_python_task": &schema.Schema{
					Type:     schema.TypeList,
					Optional: true,
					MinItems: 0,
					MaxItems: 1,
					Elem: &schema.Resource{
						Schema: map[string]*schema.Schema{
							"python_file": &schema.Schema{
								Type:     schema.TypeString,
								Required: true,
							},
							"parameters": &schema.Schema{
								Type:     schema.TypeList,
								Optional: true,
								Elem: &schema.Schema{
									Type: schema.TypeString,
								},
							},
						},
					},
				},
				"spark_submit_task": &schema.Schema{
					Type:     schema.TypeList,
					Optional: true,
					MinItems: 0,
					MaxItems: 1,
					Elem: &schema.Resource{
						Schema: map[string]*schema.Schema{
							"parameters": &schema.Schema{
								Type:     schema.TypeList,
								Required: true,
								Elem: &schema.Schema{
									Type: schema.TypeString,
								},
							},
						},
					},
				},
				"pipeline_task": &schema.Schema{
					Type:     schema.TypeList,
					Optional: true,
					MinItems: 0,
					MaxItems: 1,
					Elem: &schema.Resource{
						Schema: map[string]*schema.Schema{
							"pipeline_id": &schema.Schema{
								Type:     schema.TypeString,
								Required: true,
							},
						},
					},
				},
				"python_wheel_task": &schema.Schema{
					Type:     schema.TypeList,
					Optional: true,
					MinItems: 0,
					MaxItems: 1,
					Elem: &schema.Resource{
						Schema: map[string]*schema.Schema{
							"package_name": &schema.Schema{
								Type:     schema.TypeString,
								Required: true,
							},
							"entry_point": &schema.Schema{
								Type:     schema.TypeString,
								Required: true,
							},
							"parameters": &schema.Schema{
								Type:     schema.TypeList,
								Required: true,
								Elem: &schema.Schema{
									Type: schema.TypeString,
								},
							},
						},
					},
				},
				"sql_task": &schema.Schema{
					Type:     schema.TypeList,
					Optional: true,
					MinItems: 0,
					MaxItems: 1,
					Elem: &schema.Resource{
						Schema: map[string]*schema.Schema{
							"query": &schema.Schema{
								Type:     schema.TypeList,
								Optional: true,
								MinItems: 0,
								MaxItems: 1,
								Elem: &schema.Resource{
									Schema: map[string]*schema.Schema{
										"query_id": &schema.Schema{
											Type:     schema.TypeString,
											Required: true,
										},
									},
								},
							},
							"dashboard": &schema.Schema{
								Type:     schema.TypeList,
								Optional: true,
								MinItems: 0,
								MaxItems: 1,
								Elem: &schema.Resource{
									Schema: map[string]*schema.Schema{
										"dashboard_id": &schema.Schema{
											Type:     schema.TypeString,
											Required: true,
										},
										"subscriptions": &schema.Schema{
											Type:     schema.TypeList,
											Optional: true,
											Elem: &schema.Resource{
												Schema: map[string]*schema.Schema{
													"user_name": &schema.Schema{
														Type:     schema.TypeString,
														Optional: true,
													},
													"destination_id": &schema.Schema{
														Type:     schema.TypeString,
														Optional: true,
													},
												},
											},
										},
										"custom_subject": &schema.Schema{
											Type:     schema.TypeString,
											Optional: true,
										},
									},
								},
							},
							"alert": &schema.Schema{
								Type:     schema.TypeList,
								Optional: true,
								MinItems: 0,
								MaxItems: 1,
								Elem: &schema.Resource{
									Schema: map[string]*schema.Schema{
										"alert_id": &schema.Schema{
											Type:     schema.TypeString,
											Required: true,
										},
										"subscriptions": &schema.Schema{
											Type:     schema.TypeList,
											Optional: true,
											Elem: &schema.Resource{
												Schema: map[string]*schema.Schema{
													"user_name": &schema.Schema{
														Type:     schema.TypeString,
														Optional: true,
													},
													"destination_id": &schema.Schema{
														Type:     schema.TypeString,
														Optional: true,
													},
												},
											},
										},
									},
								},
							},
							"parameters": &schema.Schema{
								Type:     schema.TypeMap,
								Optional: true,
								Elem: &schema.Schema{
									Type: schema.TypeString,
								},
							},
							"warehouse_id": &schema.Schema{
								Type:     schema.TypeString,
								Optional: true,
							},
						},
					},
				},
				"dbt_task": &schema.Schema{
					Type:     schema.TypeList,
					Optional: true,
					MinItems: 0,
					MaxItems: 1,
					Elem: &schema.Resource{
						Schema: map[string]*schema.Schema{
							"project_directory": &schema.Schema{
								Type:     schema.TypeString,
								Optional: true,
							},
							"commands": &schema.Schema{
								Type:     schema.TypeList,
								Required: true,
								MinItems: 1,
								MaxItems: 10,
								Elem: &schema.Schema{
									Type: schema.TypeString,
								},
							},
							"schema": &schema.Schema{
								Type:     schema.TypeString,
								Optional: true,
							},
							"warehouse_id": &schema.Schema{
								Type:     schema.TypeString,
								Optional: true,
							},
							"catalog": &schema.Schema{
								Type:     schema.TypeString,
								Optional: true,
							},
							"profiles_directory": &schema.Schema{
								Type:     schema.TypeString,
								Optional: true,
							},
						},
					},
				},
				"libraries": &schema.Schema{
					Type:     schema.TypeList,
					Optional: true,
					Elem: &schema.Resource{
						Schema: map[string]*schema.Schema{
							"jar": &schema.Schema{
								Type:     schema.TypeString,
								Optional: true,
							},
							"egg": &schema.Schema{
								Type:     schema.TypeString,
								Optional: true,
							},
							"whl": &schema.Schema{
								Type:     schema.TypeString,
								Optional: true,
							},
							"pypi": &schema.Schema{
								Type:     schema.TypeList,
								Optional: true,
								MinItems: 0,
								MaxItems: 1,
								Elem: &schema.Resource{
									Schema: map[string]*schema.Schema{
										"package": &schema.Schema{
											Type:     schema.TypeString,
											Required: true,
										},
										"repo": &schema.Schema{
											Type:     schema.TypeString,
											Optional: true,
										},
									},
								},
							},
							"maven": &schema.Schema{
								Type:     schema.TypeList,
								Optional: true,
								MinItems: 0,
								MaxItems: 1,
								Elem: &schema.Resource{
									Schema: map[string]*schema.Schema{
										"coordinates": &schema.Schema{
											Type:     schema.TypeString,
											Required: true,
										},
										"repo": &schema.Schema{
											Type:     schema.TypeString,
											Optional: true,
										},
										"exclusions": &schema.Schema{
											Type:     schema.TypeList,
											Optional: true,
											Elem: &schema.Schema{
												Type: schema.TypeString,
											},
										},
									},
								},
							},
							"cran": &schema.Schema{
								Type:     schema.TypeList,
								Optional: true,
								MinItems: 0,
								MaxItems: 1,
								Elem: &schema.Resource{
									Schema: map[string]*schema.Schema{
										"package": &schema.Schema{
											Type:     schema.TypeString,
											Required: true,
										},
										"repo": &schema.Schema{
											Type:     schema.TypeString,
											Optional: true,
										},
									},
								},
							},
						},
					},
				},
				"email_notifications": &schema.Schema{
					Type:     schema.TypeList,
					Optional: true,
					MinItems: 0,
					MaxItems: 1,
					Elem: &schema.Resource{
						Schema: map[string]*schema.Schema{
							"on_start": &schema.Schema{
								Type:     schema.TypeList,
								Optional: true,
								Elem: &schema.Schema{
									Type: schema.TypeString,
								},
							},
							"on_success": &schema.Schema{
								Type:     schema.TypeList,
								Optional: true,
								Elem: &schema.Schema{
									Type: schema.TypeString,
								},
							},
							"on_failure": &schema.Schema{
								Type:     schema.TypeList,
								Optional: true,
								Elem: &schema.Schema{
									Type: schema.TypeString,
								},
							},
							"no_alert_for_skipped_runs": &schema.Schema{
								Type:     schema.TypeBool,
								Optional: true,
								Default:  true,
							},
						},
					},
				},
				"timeout_seconds": &schema.Schema{
					Type:     schema.TypeInt,
					Optional: true,
				},
				"environment_key": &schema.Schema{
					Type:     schema.TypeString,
					Optional: true,
				},
				"max_retries": &schema.Schema{
					Type:     schema.TypeInt,
					Optional: true,
				},
				"min_retry_interval_millis": &schema.Schema{
					Type:     schema.TypeInt,
					Optional: true,
				},
				"retry_on_timeout": &schema.Schema{
					Type:     schema.TypeBool,
					Optional: true,
					Default:  false,
				},
			},
		},
	},
	"job_clusters": &schema.Schema{
		Type:     schema.TypeList,
		Optional: true,
		MinItems: 0,
		Elem: &schema.Resource{
			Schema: map[string]*schema.Schema{
				"job_cluster_key": &schema.Schema{
					Type:     schema.TypeString,
					Optional: true,
				},
				"new_cluster": newClusterSchema,
			},
		},
	},
	"tags": &schema.Schema{
		Type:     schema.TypeMap,
		Optional: true,
		Elem: &schema.Schema{
			Type: schema.TypeString,
		},
	},
	"queue": &schema.Schema{
		Type:     schema.TypeList,
		Optional: true,
		Elem: &schema.Resource{
			Schema: map[string]*schema.Schema{
				"enabled": &schema.Schema{
					Type:     schema.TypeBool,
					Default:  false,
					Optional: true,
				},
			},
		},
	},
	"performance_target": &schema.Schema{
		Type:     schema.TypeString,
		Optional: true,
	},
	"budget_policy_id": &schema.Schema{
		Type:     schema.TypeString,
		Optional: true,
	},
	"run_as": &schema.Schema{
		Type:     schema.TypeList,
		Optional: true,
		Elem: &schema.Resource{
			Schema: map[string]*schema.Schema{
				"user_name": &schema.Schema{
					Type:     schema.TypeString,
					Optional: true,
				},
				"service_principal_name": &schema.Schema{
					Type:     schema.TypeString,
					Optional: true,
				},
			},
		},
	},
	"git_source": &schema.Schema{
		Type:     schema.TypeList,
		MinItems: 0,
		MaxItems: 1,
		Optional: true,
		Elem: &schema.Resource{
			Schema: map[string]*schema.Schema{
				"git_url": &schema.Schema{
					Type:     schema.TypeString,
					Optional: true,
				},
				"git_provider": &schema.Schema{
					Type:     schema.TypeString,
					Optional: true,
				},
				"git_branch": &schema.Schema{
					Type:     schema.TypeString,
					Optional: true,
				},
				"git_tag": &schema.Schema{
					Type:     schema.TypeString,
					Optional: true,
				},
				"git_commit": &schema.Schema{
					Type:     schema.TypeString,
					Optional: true,
				},
			},
		},
	},
}

func resourceJob() *schema.Resource {
	return &schema.Resource{
		Create: resourceJobCreate,
		Read:   resourceJobRead,
		Update: resourceJobUpdate,
		Delete: resourceJobDelete,
		Importer: &schema.ResourceImporter{
			State: resourceJobImport,
		},
		Schema: jobSchema,
	}
}

func expandLibraries(d []interface{}) []*databricks.Library {
	var libraries []*databricks.Library
	for _, v := range d {
		v := v.(map[string]interface{})

		if v, ok := v["jar"]; ok && v.(string) != "" {
			libraries = append(libraries, &databricks.Library{
				Jar: v.(string),
			})
			continue
		}

		if v, ok := v["whl"]; ok && v.(string) != "" {
			libraries = append(libraries, &databricks.Library{
				Wheel: v.(string),
			})
			continue
		}

		if v, ok := v["maven"]; ok && len(v.([]interface{})) == 1 {
			v := v.([]interface{})[0].(map[string]interface{})
			libraries = append(libraries, &databricks.Library{
				Maven: &databricks.MavenLibrary{
					Coordinates: v["coordinates"].(string),
				},
			})
			continue
		}

		if v, ok := v["pypi"]; ok && len(v.([]interface{})) == 1 {
			v := v.([]interface{})[0].(map[string]interface{})
			libraries = append(libraries, &databricks.Library{
				Pypi: &databricks.PypiLibrary{
					Package: v["package"].(string),
				},
			})
			continue
		}
	}
	return libraries
}

func expandEmailNotifications(d map[string]interface{}) databricks.EmailNotifications {
	var emailNotifications databricks.EmailNotifications
	if v := d["on_start"]; len(v.([]interface{})) > 0 {
		for _, param := range v.([]interface{}) {
			emailNotifications.OnStart = append(emailNotifications.OnStart, param.(string))
		}
	}

	if v := d["on_success"]; len(v.([]interface{})) > 0 {
		for _, param := range v.([]interface{}) {
			emailNotifications.OnSuccess = append(emailNotifications.OnSuccess, param.(string))
		}
	}

	if v := d["on_failure"]; len(v.([]interface{})) > 0 {
		for _, param := range v.([]interface{}) {
			emailNotifications.OnFailure = append(emailNotifications.OnFailure, param.(string))
		}
	}
	if v, ok := d["no_alert_for_skipped_runs"]; ok {
		emailNotifications.NoAlertForSkippedRuns = v.(bool)
	}

	return emailNotifications
}

func expandWebhookNotifications(d map[string]interface{}) databricks.WebhookNotifications {
	var webhookNotifications databricks.WebhookNotifications

	if v := d["on_start"]; len(v.([]interface{})) > 0 {
		for _, notification := range v.([]interface{}) {
			n := notification.(map[string]interface{})
			if id, ok := n["id"]; ok {
				webhookNotifications.OnStart = append(webhookNotifications.OnStart, &databricks.WebhookNotificationDestination{
					Id: id.(string),
				})
			}
		}
	}
	if v := d["on_success"]; len(v.([]interface{})) > 0 {
		for _, notification := range v.([]interface{}) {
			n := notification.(map[string]interface{})
			if id, ok := n["id"]; ok {
				webhookNotifications.OnSuccess = append(webhookNotifications.OnSuccess, &databricks.WebhookNotificationDestination{
					Id: id.(string),
				})
			}
		}
	}
	if v := d["on_failure"]; len(v.([]interface{})) > 0 {
		for _, notification := range v.([]interface{}) {
			n := notification.(map[string]interface{})
			if id, ok := n["id"]; ok {
				webhookNotifications.OnFailure = append(webhookNotifications.OnFailure, &databricks.WebhookNotificationDestination{
					Id: id.(string),
				})
			}
		}
	}

	return webhookNotifications
}

func expandJobClusters(jobClusters []interface{}) []*databricks.JobCluster {
	var settings = []*databricks.JobCluster{}
	for _, d := range jobClusters {
		var jobCluster databricks.JobCluster
		d := d.(map[string]interface{})

		if v, ok := d["job_cluster_key"]; ok && v != "" {
			jobCluster.JobClusterKey = v.(string)
		} else {
			panic(oops.Wrapf(nil, "Error parsing JobCluster spec; 'job_cluster_key' is required"))
		}
		if v, ok := d["new_cluster"]; ok && len(v.([]interface{})) == 1 {
			d := v.([]interface{})[0].(map[string]interface{})
			if len(d) > 1 {
				jobCluster.NewCluster = expandClusterSpec(v.([]interface{})[0].(map[string]interface{}))
			}
		} else {
			panic(oops.Wrapf(nil, "Error parsing JobCluster spec; 'new_cluster' is required"))
		}
		settings = append(settings, &jobCluster)
	}
	return settings
}

func expandJobTaskSettings(jobTasks []interface{}) []*databricks.JobTaskSettings {
	var settings = []*databricks.JobTaskSettings{}
	for _, task := range jobTasks {
		task := task.(map[string]interface{})
		taskSettings := databricks.JobTaskSettings{}

		if v, ok := task["task_key"]; ok && v.(string) != "" {
			taskSettings.TaskKey = v.(string)
		} else {
			panic(oops.Wrapf(nil, "Error parsing task settings; 'task_key' is required"))
		}

		if v, ok := task["depends_on"]; ok && len(v.([]interface{})) > 0 {
			dependencies := []*databricks.TaskDependency{}
			for _, d := range v.([]interface{}) {
				dependsOn := d.(map[string]interface{})
				if taskKey, ok := dependsOn["task_key"]; ok && taskKey.(string) != "" {
					dependencies = append(dependencies, &databricks.TaskDependency{
						TaskKey: taskKey.(string),
					})
				} else {
					panic(oops.Wrapf(nil, "Error parsing task dependencies; 'task_key' cannot be empty string"))
				}
			}
			sort.Slice(dependencies, func(i, j int) bool {
				return dependencies[i].TaskKey < dependencies[j].TaskKey
			})
			taskSettings.DependsOn = dependencies
		}

		if v, ok := task["environment_key"]; ok {
			taskSettings.EnvironmentKey = v.(string)
		}

		// If the task is running in a serverless environment, we don't need to specify a cluster & libs.
		// EnvironmentKey is set for the serverless jobs.
		if taskSettings.EnvironmentKey == "" {
			// A task can only run on one cluster. It is unclear which of the following 3 will take precedence.
			// Assuming the order in api doc indicates the order of precedence.
			if v, ok := task["existing_cluster_id"]; ok {
				taskSettings.ExistingClusterId = v.(string)
			}

			if v, ok := task["new_cluster"]; ok && len(v.([]interface{})) == 1 {
				d := v.([]interface{})[0].(map[string]interface{})
				if len(d) > 1 {
					taskSettings.NewCluster = expandClusterSpec(v.([]interface{})[0].(map[string]interface{}))
				}
			}

			if v, ok := task["job_cluster_key"]; ok {
				taskSettings.JobClusterKey = v.(string)
			}

			if v, ok := task["libraries"]; ok && len(v.([]interface{})) > 0 {
				taskSettings.Libraries = expandLibraries(v.([]interface{}))
			}
		}

		if v, ok := task["spark_jar_task"]; ok && len(v.([]interface{})) == 1 {
			d := v.([]interface{})[0].(map[string]interface{})
			var taskSpec databricks.SparkJarTask

			if v, ok := d["main_class_name"]; ok {
				taskSpec.MainClassName = v.(string)
			}
			if v, ok := d["parameters"]; ok && len(v.([]interface{})) > 0 {
				for _, param := range v.([]interface{}) {
					paramAsString, err := getParamAsStringFromResourceData(param)
					if err != nil {
						panic(oops.Wrapf(err, "Error parsing param %+v; type = %T, value = %+v", param, v, v))
					}
					taskSpec.Parameters = append(taskSpec.Parameters, paramAsString)
				}
			}
			taskSettings.SparkJarTask = &taskSpec
		}

		if v, ok := task["spark_python_task"]; ok && len(v.([]interface{})) == 1 {
			d := v.([]interface{})[0].(map[string]interface{})
			var taskSpec databricks.SparkPythonTask

			if v, ok := d["python_file"]; ok {
				taskSpec.PythonFile = v.(string)
			}
			if v, ok := d["parameters"]; ok && len(v.([]interface{})) > 0 {
				for _, param := range v.([]interface{}) {
					paramAsString, err := getParamAsStringFromResourceData(param)
					if err != nil {
						panic(oops.Wrapf(err, "Error parsing param %+v; type = %T, value = %+v", param, v, v))
					}
					taskSpec.Parameters = append(taskSpec.Parameters, paramAsString)
				}
			}
			taskSettings.SparkPythonTask = &taskSpec
		}

		if v, ok := task["notebook_task"]; ok && len(v.([]interface{})) == 1 {
			d := v.([]interface{})[0].(map[string]interface{})
			var taskSpec databricks.NotebookTask

			if v, ok := d["notebook_path"]; ok {
				taskSpec.NotebookPath = v.(string)
			}
			if v, ok := d["source"]; ok {
				taskSpec.Source = v.(string)
			}
			if v, ok := d["base_parameters"]; ok && len(v.(map[string]interface{})) > 0 {
				taskSpec.BaseParameters = make(map[string]string)
				for k, parameterValue := range v.(map[string]interface{}) {
					paramAsString, err := getParamAsStringFromResourceData(parameterValue)
					if err != nil {
						panic(oops.Wrapf(err, "Error parsing param %+v; type = %T, value = %+v", parameterValue, v, v))
					}
					taskSpec.BaseParameters[k] = paramAsString
				}
			}
			taskSettings.NotebookTask = &taskSpec
		}

		if v, ok := task["sql_task"]; ok && len(v.([]interface{})) == 1 && v.([]interface{})[0] != nil {
			d := v.([]interface{})[0].(map[string]interface{})
			var taskSpec databricks.SqlTask
			taskCount := 0
			if v, ok := d["query"]; ok && len(v.([]interface{})) == 1 {
				query := v.([]interface{})[0].(map[string]interface{})
				if v, ok := query["query_id"]; ok {
					taskSpec.Query = &databricks.SqlTaskQuery{
						QueryId: v.(string),
					}
				}
				taskCount++
			}
			if v, ok := d["dashboard"]; ok && len(v.([]interface{})) == 1 {
				dashboard := v.([]interface{})[0].(map[string]interface{})
				if v, ok := dashboard["dashboard_id"]; ok {
					taskSpec.Dashboard = &databricks.SqlTaskDashboard{
						DashboardId: v.(string),
					}
				}
				if v, ok := dashboard["subscriptions"]; ok && len(v.([]interface{})) > 0 {
					var subscriptions []*databricks.SqlTaskSubscriptions
					for _, s := range v.([]interface{}) {
						sub := s.(map[string]interface{})
						if u, ok := sub["user_name"]; ok && u.(string) != "" {
							subscriptions = append(subscriptions, &databricks.SqlTaskSubscriptions{
								UserName: u.(string),
							})
						}
						if d, ok := sub["destination_id"]; ok && d.(string) != "" {
							subscriptions = append(subscriptions, &databricks.SqlTaskSubscriptions{
								DestinationId: d.(string),
							})
						}
					}
					taskSpec.Dashboard.Subscriptions = subscriptions
				}
				taskCount++
			}
			if v, ok := d["alert"]; ok && len(v.([]interface{})) == 1 {
				alert := v.([]interface{})[0].(map[string]interface{})
				if v, ok := alert["alert_id"]; ok {
					taskSpec.Alert = &databricks.SqlTaskAlert{
						AlertId: v.(string),
					}
				}
				if v, ok := alert["subscriptions"]; ok && len(v.([]interface{})) > 0 {
					var subscriptions []*databricks.SqlTaskSubscriptions
					for _, s := range v.([]interface{}) {
						sub := s.(map[string]interface{})
						if u, ok := sub["user_name"]; ok && u.(string) != "" {
							subscriptions = append(subscriptions, &databricks.SqlTaskSubscriptions{
								UserName: u.(string),
							})
						}
						if d, ok := sub["destination_id"]; ok && d.(string) != "" {
							subscriptions = append(subscriptions, &databricks.SqlTaskSubscriptions{
								DestinationId: d.(string),
							})
						}
					}
					taskSpec.Alert.Subscriptions = subscriptions
				}
				taskCount++
			}
			if v, ok := d["parameters"]; ok && len(v.(map[string]interface{})) > 0 {
				parameters := make(map[string]string)
				for k, parameterValue := range v.(map[string]interface{}) {
					paramAsString, err := getParamAsStringFromResourceData(parameterValue)
					if err != nil {
						panic(oops.Wrapf(err, "Error parsing param %+v; type = %T, value = %+v", parameterValue, v, v))
					}
					parameters[k] = paramAsString
				}
				taskSpec.Parameters = parameters
			}
			if v, ok := d["warehouse_id"]; ok {
				taskSpec.WarehouseId = v.(string)
			}
			if taskCount == 1 {

				taskSettings.JobClusterKey = ""
				taskSettings.NewCluster = nil
				taskSettings.ExistingClusterId = ""
				taskSettings.Libraries = nil
				taskSettings.SqlTask = &taskSpec
			}
		}

		if v, ok := task["dbt_task"]; ok && len(v.([]interface{})) == 1 {
			d := v.([]interface{})[0].(map[string]interface{})
			var taskSpec databricks.DbtTask

			if v, ok := d["project_directory"]; ok {
				taskSpec.ProjectDirectory = v.(string)
			}
			if v, ok := d["commands"]; ok && len(v.([]interface{})) > 0 {
				for _, command := range v.([]interface{}) {
					commandAsString, err := getParamAsStringFromResourceData(command)
					if err != nil || commandAsString == "" {
						panic(oops.Wrapf(err, "Error parsing command %+v; type = %T, value = %+v", command, v, v))
					}
					taskSpec.Commands = append(taskSpec.Commands, commandAsString)
				}
			}
			if v, ok := d["schema"]; ok {
				taskSpec.Schema = v.(string)
			}
			if v, ok := d["warehouse_id"]; ok {
				taskSpec.WarehouseId = v.(string)
			}
			if v, ok := d["catalog"]; ok {
				taskSpec.Catalog = v.(string)
			}
			if v, ok := d["profiles_directory"]; ok {
				taskSpec.ProfilesDirectory = v.(string)
			}
			taskSettings.DbtTask = &taskSpec
		}

		if v, ok := task["email_notifications"]; ok && len(v.([]interface{})) == 1 {
			d := v.([]interface{})[0].(map[string]interface{})
			emailNotifications := expandEmailNotifications(d)
			taskSettings.EmailNotifications = &emailNotifications
		}
		if v, ok := task["timeout_seconds"]; ok {
			taskSettings.TimeoutSeconds = v.(int)
		}

		if v, ok := task["max_retries"]; ok {
			taskSettings.MaxRetries = v.(int)
		}
		if v, ok := task["min_retry_interval_millis"]; ok {
			taskSettings.MinRetryIntervalMilliseconds = v.(int)
		}
		if v, ok := task["retry_on_timeout"]; ok {
			taskSettings.RetryOnTimeout = v.(bool)
		}
		settings = append(settings, &taskSettings)
	}
	return settings
}

func expandEnvironments(v []interface{}) []*databricks.ServerlessEnvironment {
	environments := []*databricks.ServerlessEnvironment{}
	for _, env := range v {
		d := env.(map[string]interface{})
		deps := []string{}
		client := ""

		if specList, ok := d["spec"].([]interface{}); ok && len(specList) > 0 {
			if spec, ok := specList[0].(map[string]interface{}); ok {

				if dependencies, ok := spec["dependencies"].([]interface{}); ok {
					for _, dep := range dependencies {
						if depStr, ok := dep.(string); ok {
							deps = append(deps, depStr)
						}
					}
				}

				if clientVal, ok := spec["client"].(string); ok {
					client = clientVal
				}
			}
		}
		environments = append(environments, &databricks.ServerlessEnvironment{
			EnvironmentKey: d["environment_key"].(string),
			Spec: &databricks.ServerlessEnvironmentSpec{
				Client:       client,
				Dependencies: deps,
			},
		})
	}
	return environments
}

func expandJobSettings(d *schema.ResourceData) databricks.JobSettings {
	settings := databricks.JobSettings{
		Name: d.Get("name").(string),
	}
	if v, ok := d.GetOk("email_notifications"); ok {
		d := v.([]interface{})[0].(map[string]interface{})
		emailNotifications := expandEmailNotifications(d)
		settings.EmailNotifications = &emailNotifications
	}
	if v, ok := d.GetOk("timeout_seconds"); ok {
		settings.TimeoutSeconds = v.(int)
	}
	if v, ok := d.GetOk("schedule"); ok && len(v.([]interface{})) == 1 {
		d := v.([]interface{})[0].(map[string]interface{})
		var schedule databricks.CronSchedule

		if v, ok := d["quartz_cron_expression"]; ok {
			schedule.QuartzCronExpression = v.(string)
		}
		if v, ok := d["timezone_id"]; ok {
			schedule.TimezoneId = v.(string)
		}
		if v, ok := d["pause_status"]; ok {
			schedule.PauseStatus = v.(string)
		}
		settings.Schedule = &schedule
	}
	if v, ok := d.GetOk("format"); ok {
		settings.Format = v.(string)
	}
	if v, ok := d.GetOk("environments"); ok && len(v.([]interface{})) > 0 {
		settings.Environments = expandEnvironments(v.([]interface{}))
	}
	if v, ok := d.GetOk("tasks"); ok && len(v.([]interface{})) > 0 {
		settings.Tasks = expandJobTaskSettings(v.([]interface{}))
	}
	if v, ok := d.GetOk("job_clusters"); ok && len(v.([]interface{})) > 0 {
		settings.JobClusters = expandJobClusters(v.([]interface{}))
	}
	if v, ok := d.GetOk("tags"); ok && len(v.(map[string]interface{})) > 0 {
		settings.Tags = make(map[string]string)
		for k, v := range v.(map[string]interface{}) {
			settings.Tags[k] = v.(string)
		}
	}

	if v, ok := d.GetOk("queue"); ok && len(v.([]interface{})) > 0 {
		queue := v.([]interface{})[0].(map[string]interface{})
		if v, ok := queue["enabled"]; ok {
			settings.Queue = &databricks.JobQueue{
				Enabled: v.(bool),
			}
		} else {
			panic(oops.Wrapf(nil, "Error parsing queue; 'enabled' is required"))
		}

	}
	if v, ok := d.GetOk("performance_target"); ok {
		settings.PerformanceTarget = v.(string)
	}
	if v, ok := d.GetOk("budget_policy_id"); ok {
		settings.BudgetPolicyId = v.(string)
	}

	if v, ok := d.GetOk("run_as"); ok && len(v.([]interface{})) > 0 {
		d := v.([]interface{})[0].(map[string]interface{})
		userName := ""
		servicePrincipalName := ""

		if v, ok := d["user_name"]; ok {
			userName = v.(string)
			settings.RunAs = &databricks.RunAsSetting{
				UserName: userName,
			}

		}

		if v, ok := d["service_principal_name"]; ok {
			if userName != "" {
				panic(oops.Wrapf(nil, "Error parsing run_as; 'user_name' and 'service_principal_name' cannot both be set"))
			}
			servicePrincipalName = v.(string)
			settings.RunAs = &databricks.RunAsSetting{
				ServicePrincipalName: servicePrincipalName,
			}

		}
	}

	if v, ok := d.GetOk("webhook_notifications"); ok && len(v.([]interface{})) == 1 {
		d := v.([]interface{})[0].(map[string]interface{})
		webhookNotifications := expandWebhookNotifications(d)
		settings.WebhookNotifications = &webhookNotifications
	}

	if v, ok := d.GetOk("git_source"); ok && len(v.([]interface{})) == 1 {
		d := v.([]interface{})[0].(map[string]interface{})
		if len(d) > 1 {
			var gitSource databricks.GitSource
			if v, ok := d["git_url"]; ok && v.(string) != "" {
				gitSource.GitUrl = v.(string)
			}
			if v, ok := d["git_provider"]; ok && v.(string) != "" {
				gitSource.GitProvider = v.(string)
			}
			if v, ok := d["git_branch"]; ok && v.(string) != "" {
				gitSource.GitBranch = v.(string)
			}
			if v, ok := d["git_commit"]; ok && v.(string) != "" {
				gitSource.GitCommit = v.(string)
			}
			if v, ok := d["git_tag"]; ok && v.(string) != "" {
				gitSource.GitTag = v.(string)
			}
			settings.GitSource = &gitSource
		}
	}
	return settings
}

func flattenLibraries(libraries []*databricks.Library) []interface{} {
	var d []interface{}
	for _, library := range libraries {
		if library.Jar != "" {
			d = append(d, map[string]interface{}{
				"jar": library.Jar,
			})
			continue
		}
		if library.Maven != nil {
			d = append(d, map[string]interface{}{
				"maven": []interface{}{
					map[string]interface{}{
						"coordinates": library.Maven.Coordinates,
					},
				},
			})
			continue
		}
		if library.Pypi != nil {
			d = append(d, map[string]interface{}{
				"pypi": []interface{}{
					map[string]interface{}{
						"package": library.Pypi.Package,
					},
				},
			})
			continue
		}
		if library.Wheel != "" {
			d = append(d, map[string]interface{}{
				"whl": library.Wheel,
			})
			continue
		}
	}
	return d
}

func flattenJobClusters(jobClusters []*databricks.JobCluster) []map[string]interface{} {
	clusters := []map[string]interface{}{}
	for _, jobCluster := range jobClusters {
		jc := make(map[string]interface{})
		jc["job_cluster_key"] = jobCluster.JobClusterKey

		clusterSpec := flattenClusterSpec(jobCluster.NewCluster)
		// Remove entries not applicable to automated cluster.
		delete(clusterSpec, "cluster_name")
		delete(clusterSpec, "autotermination_minutes")
		delete(clusterSpec, "workload_type")
		delete(clusterSpec, "is_single_node")
		delete(clusterSpec, "kind")
		jc["new_cluster"] = []interface{}{clusterSpec}
		clusters = append(clusters, jc)
	}
	return clusters
}

func flattenWebhookNotifications(webhooks *databricks.WebhookNotifications) map[string]interface{} {
	webhookNotifications := make(map[string]interface{})
	if webhooks.OnStart != nil && len(webhooks.OnStart) > 0 {
		var destinations []interface{}
		for _, n := range webhooks.OnStart {
			destinations = append(destinations, map[string]interface{}{
				"id": n.Id,
			})
		}
		webhookNotifications["on_start"] = destinations
	}
	if webhooks.OnSuccess != nil && len(webhooks.OnSuccess) > 0 {
		var destinations []interface{}
		for _, n := range webhooks.OnSuccess {
			destinations = append(destinations, map[string]interface{}{
				"id": n.Id,
			})
		}
		webhookNotifications["on_success"] = destinations
	}
	if webhooks.OnFailure != nil && len(webhooks.OnFailure) > 0 {
		var destinations []interface{}
		for _, n := range webhooks.OnFailure {
			destinations = append(destinations, map[string]interface{}{
				"id": n.Id,
			})
		}
		webhookNotifications["on_failure"] = destinations
	}
	return webhookNotifications
}

func flattenJobTaskSettings(settings []*databricks.JobTaskSettings) []map[string]interface{} {
	jobTaskSettings := []map[string]interface{}{}

	for _, taskSettings := range settings {
		flattened := make(map[string]interface{})

		flattened["task_key"] = taskSettings.TaskKey

		if taskSettings.Description != "" {
			flattened["description"] = taskSettings.Description
		} else {
			flattened["description"] = nil
		}

		if taskSettings.EnvironmentKey != "" {
			flattened["environment_key"] = taskSettings.EnvironmentKey
		} else {
			flattened["environment_key"] = nil
		}

		if taskSettings.DependsOn != nil && len(taskSettings.DependsOn) > 0 {
			sort.Slice(taskSettings.DependsOn, func(i, j int) bool {
				return taskSettings.DependsOn[i].TaskKey < taskSettings.DependsOn[j].TaskKey
			})
			dependencies := []map[string]string{}
			for _, d := range taskSettings.DependsOn {
				dependentTask := map[string]string{"task_key": d.TaskKey}
				dependencies = append(dependencies, dependentTask)
			}
			flattened["depends_on"] = dependencies
		} else {
			flattened["depends_on"] = nil
		}

		if taskSettings.ExistingClusterId != "" {
			flattened["existing_cluster_id"] = taskSettings.ExistingClusterId
		} else {
			flattened["existing_cluster_id"] = nil
		}

		if taskSettings.NewCluster != nil {
			clusterSpec := flattenClusterSpec(taskSettings.NewCluster)
			// Remove entries not applicable to automated cluster.
			delete(clusterSpec, "cluster_name")
			delete(clusterSpec, "autotermination_minutes")
			delete(clusterSpec, "workload_type")
			delete(clusterSpec, "is_single_node")
			delete(clusterSpec, "kind")
			flattened["new_cluster"] = []interface{}{clusterSpec}
		} else {
			flattened["new_cluster"] = []interface{}{emptyClusterResource()}
		}

		if taskSettings.JobClusterKey != "" {
			flattened["job_cluster_key"] = taskSettings.JobClusterKey
		} else {
			flattened["job_cluster_key"] = nil
		}

		if taskSettings.SparkJarTask != nil {
			task := make(map[string]interface{})
			task["main_class_name"] = taskSettings.SparkJarTask.MainClassName
			task["parameters"] = taskSettings.SparkJarTask.Parameters
			flattened["spark_jar_task"] = []interface{}{task}
		}

		if taskSettings.SparkPythonTask != nil {
			task := make(map[string]interface{})
			task["python_file"] = taskSettings.SparkPythonTask.PythonFile
			task["parameters"] = taskSettings.SparkPythonTask.Parameters
			flattened["spark_python_task"] = []interface{}{task}
		}

		if taskSettings.NotebookTask != nil {
			task := make(map[string]interface{})
			task["notebook_path"] = taskSettings.NotebookTask.NotebookPath
			task["base_parameters"] = taskSettings.NotebookTask.BaseParameters
			task["source"] = taskSettings.NotebookTask.Source
			flattened["notebook_task"] = []interface{}{task}
		}

		if taskSettings.SqlTask != nil {
			task := make(map[string]interface{})
			if taskSettings.SqlTask.Query != nil {
				task["query"] = []interface{}{
					map[string]interface{}{
						"query_id": taskSettings.SqlTask.Query.QueryId,
					},
				}
			} else {
				task["query"] = nil
			}
			if taskSettings.SqlTask.Dashboard != nil {
				var subscriptions []map[string]interface{}
				if len(taskSettings.SqlTask.Dashboard.Subscriptions) > 0 {
					for _, s := range taskSettings.SqlTask.Dashboard.Subscriptions {
						sub := map[string]interface{}{}
						if s.UserName != "" {
							sub["user_name"] = s.UserName
						}
						if s.DestinationId != "" {
							sub["destination_id"] = s.DestinationId
						}
						subscriptions = append(subscriptions, sub)
					}
				}
				task["dashboard"] = []interface{}{
					map[string]interface{}{
						"dashboard_id":   taskSettings.SqlTask.Dashboard.DashboardId,
						"custom_subject": taskSettings.SqlTask.Dashboard.CustomSubject,
						"subscriptions":  subscriptions,
					},
				}
			} else {
				task["dashboard"] = nil
			}
			if taskSettings.SqlTask.Alert != nil {
				var subscriptions []map[string]interface{}
				if len(taskSettings.SqlTask.Dashboard.Subscriptions) > 0 {
					for _, s := range taskSettings.SqlTask.Dashboard.Subscriptions {
						sub := map[string]interface{}{}
						if s.UserName != "" {
							sub["user_name"] = s.UserName
						}
						if s.DestinationId != "" {
							sub["destination_id"] = s.DestinationId
						}
						subscriptions = append(subscriptions, sub)
					}
				}
				task["alert"] = []interface{}{
					map[string]interface{}{
						"alert_id":      taskSettings.SqlTask.Alert.AlertId,
						"subscriptions": subscriptions,
					},
				}
			} else {
				task["alert"] = nil
			}
			if len(taskSettings.SqlTask.Parameters) > 0 {
				task["parameters"] = taskSettings.SqlTask.Parameters
			} else {
				task["parameters"] = nil
			}
			task["warehouse_id"] = taskSettings.SqlTask.WarehouseId
			flattened["sql_task"] = []interface{}{task}
		} else {
			flattened["sql_task"] = []interface{}{map[string]interface{}{"parameters": nil}}
		}

		if taskSettings.DbtTask != nil {
			task := make(map[string]interface{})
			task["project_directory"] = taskSettings.DbtTask.ProjectDirectory
			task["commands"] = taskSettings.DbtTask.Commands
			task["schema"] = taskSettings.DbtTask.Schema
			task["warehouse_id"] = taskSettings.DbtTask.WarehouseId
			task["catalog"] = taskSettings.DbtTask.Catalog
			task["profiles_directory"] = taskSettings.DbtTask.ProfilesDirectory
			flattened["dbt_task"] = []interface{}{task}
		}

		if taskSettings.EmailNotifications != nil && !taskSettings.EmailNotifications.IsEmpty() {
			emailNotifications := make(map[string]interface{})
			emailNotifications["on_start"] = taskSettings.EmailNotifications.OnStart
			emailNotifications["on_success"] = taskSettings.EmailNotifications.OnSuccess
			emailNotifications["on_failure"] = taskSettings.EmailNotifications.OnFailure
			emailNotifications["no_alert_for_skipped_runs"] = taskSettings.EmailNotifications.NoAlertForSkippedRuns
			flattened["email_notifications"] = []interface{}{emailNotifications}
		} else {
			flattened["email_notifications"] = nil
		}
		if taskSettings.Libraries != nil {
			flattened["libraries"] = flattenLibraries(taskSettings.Libraries)
		} else {
			flattened["libraries"] = nil
		}

		if taskSettings.TimeoutSeconds != 0 {
			flattened["timeout_seconds"] = taskSettings.TimeoutSeconds
		}

		if taskSettings.MaxRetries != 0 {
			flattened["max_retries"] = taskSettings.MaxRetries
		} else {
			flattened["max_retries"] = nil
		}

		if taskSettings.MinRetryIntervalMilliseconds != 0 {
			flattened["min_retry_interval_millis"] = taskSettings.MinRetryIntervalMilliseconds
		} else {
			flattened["min_retry_interval_millis"] = nil
		}

		if taskSettings.RetryOnTimeout != false {
			flattened["retry_on_timeout"] = taskSettings.RetryOnTimeout
		} else {
			flattened["retry_on_timeout"] = nil
		}

		jobTaskSettings = append(jobTaskSettings, flattened)
	}
	return jobTaskSettings
}

// The terraform hcl2 encoder contains a bug where it generates empty tf resources in the tf plan
// for Struct that have nested pointer attributes
func emptyClusterResource() map[string]interface{} {
	emptyClusterResource := make(map[string]interface{})
	emptyClusterResource["cluster_log_conf"] = []interface{}{make(map[string]interface{})}
	return emptyClusterResource
}

func flattenJobSettings(settings databricks.JobSettings) (map[string]interface{}, error) {
	m := make(map[string]interface{})
	m["name"] = settings.Name
	m["timeout_seconds"] = settings.TimeoutSeconds
	if settings.EmailNotifications != nil && !settings.EmailNotifications.IsEmpty() {
		emailNotifications := make(map[string]interface{})
		emailNotifications["on_start"] = settings.EmailNotifications.OnStart
		emailNotifications["on_success"] = settings.EmailNotifications.OnSuccess
		emailNotifications["on_failure"] = settings.EmailNotifications.OnFailure
		emailNotifications["no_alert_for_skipped_runs"] = settings.EmailNotifications.NoAlertForSkippedRuns
		m["email_notifications"] = []interface{}{emailNotifications}
	} else {
		m["email_notifications"] = nil
	}

	if settings.Schedule != nil {
		schedule := make(map[string]interface{})
		schedule["quartz_cron_expression"] = settings.Schedule.QuartzCronExpression
		schedule["timezone_id"] = settings.Schedule.TimezoneId
		schedule["pause_status"] = settings.Schedule.PauseStatus
		m["schedule"] = []interface{}{schedule}
	} else {
		m["schedule"] = nil
	}

	if settings.Format != "" {
		m["format"] = settings.Format
	} else {
		m["format"] = nil
	}

	if settings.JobClusters != nil && len(settings.JobClusters) > 0 {
		m["job_clusters"] = flattenJobClusters(settings.JobClusters)
	}

	if settings.Tasks != nil && len(settings.Tasks) > 0 {
		m["tasks"] = flattenJobTaskSettings(settings.Tasks)
	}

	if settings.Tags != nil && len(settings.Tags) > 0 {
		tags := make(map[string]string)
		for k, v := range settings.Tags {
			tags[k] = v
		}
	}

	if settings.WebhookNotifications != nil && !settings.WebhookNotifications.IsEmpty() {
		webhookNotifications := flattenWebhookNotifications(settings.WebhookNotifications)
		m["webhook_notifications"] = []interface{}{webhookNotifications}
	} else {
		m["webhook_notifications"] = nil
	}

	return m, nil
}

func resourceJobCreate(d *schema.ResourceData, m interface{}) error {
	client := m.(databricks.API)
	settings := expandJobSettings(d)

	out, err := client.CreateJob(context.Background(), &databricks.CreateJobInput{
		JobSettings: settings,
	})
	if err != nil {
		return oops.Wrapf(err, "")
	}

	d.SetId(strconv.FormatInt(out.JobId, 10))
	return nil
}

func resourceJobRead(d *schema.ResourceData, m interface{}) error {
	jobId, err := strconv.ParseInt(d.Id(), 10, 64)
	if err != nil {
		return oops.Wrapf(err, "parse job id: %s", d.Id())
	}

	client := m.(databricks.API)
	out, err := client.GetJob(context.Background(), &databricks.GetJobInput{
		JobId: jobId,
	})
	if err != nil {
		if apiErr, ok := oops.Cause(err).(*databricks.APIError); ok {
			if strings.HasSuffix(apiErr.Message, "does not exist.") {
				d.SetId("")
				return nil
			}
		}
		return oops.Wrapf(err, "")
	}

	var jobSettings databricks.JobSettings
	if out.JobSettings.Tags != nil && out.JobSettings.Tags["format"] == databricks.MultiTaskKey {
		jobSettings = out.JobSettings
	} else {
		jobSettings = databricks.AdaptMultiTaskJobSettings(out.JobSettings)
	}

	kv, err := flattenJobSettings(jobSettings)
	if err != nil {
		return oops.Wrapf(err, "flattenJobSettings")
	}
	for k, v := range kv {
		if err := d.Set(k, v); err != nil {
			return oops.Wrapf(err, "%+v: %+v", k, v)
		}
	}
	return nil
}

func resourceJobUpdate(d *schema.ResourceData, m interface{}) error {
	jobId, err := strconv.ParseInt(d.Id(), 10, 64)
	settings := expandJobSettings(d)

	if err != nil {
		return oops.Wrapf(err, "parse job id: %s", d.Id())
	}
	client := m.(databricks.API)
	if _, err := client.ResetJob(context.Background(), &databricks.ResetJobInput{
		JobId:       jobId,
		NewSettings: settings,
	}); err != nil {
		return err
	}
	return nil
}

func resourceJobDelete(d *schema.ResourceData, m interface{}) error {
	jobId, err := strconv.ParseInt(d.Id(), 10, 64)
	if err != nil {
		return oops.Wrapf(err, "parse job id: %s", d.Id())
	}

	client := m.(databricks.API)
	if _, err := client.DeleteJob(context.Background(), &databricks.DeleteJobInput{
		JobId: jobId,
	}); err != nil {
		return err
	}
	return nil
}

func resourceJobImport(d *schema.ResourceData, m interface{}) ([]*schema.ResourceData, error) {
	jobId, err := strconv.ParseInt(d.Id(), 10, 64)
	if err != nil {
		return nil, oops.Wrapf(err, "parse job id: %s", d.Id())
	}

	d.SetId(strconv.FormatInt(jobId, 10))
	if err := resourceJobRead(d, m); err != nil {
		return nil, err
	}
	return []*schema.ResourceData{d}, nil
}

func getParamAsStringFromResourceData(param interface{}) (string, error) {
	if param == nil {
		return "", nil
	}
	if _, ok := param.(string); !ok {
		return "", oops.Errorf("Cannot cast param to string: %+v", param)
	}
	return param.(string), nil
}
