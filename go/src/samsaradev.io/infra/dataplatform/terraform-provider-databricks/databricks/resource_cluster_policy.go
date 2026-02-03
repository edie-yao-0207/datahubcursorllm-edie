package databricks

import (
	"context"
	"encoding/json"

	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/dataplatform/databricks"
)

var clusterPolicySchema = map[string]*schema.Schema{
	"policy_id": {
		Type:     schema.TypeString,
		Computed: true,
	},
	"name": {
		Type:     schema.TypeString,
		Required: true,
	},
	"definition": {
		Type:     schema.TypeString,
		Required: true,
	},
	"creator_user_name": {
		Type:     schema.TypeString,
		Computed: true,
	},
	"created_at_timestamp": {
		Type:     schema.TypeInt,
		Computed: true,
	},
}

func resourceClusterPolicy() *schema.Resource {
	return &schema.Resource{
		Create: create,
		Read:   read,
		Update: update,
		Delete: clusterPolicyDelete,
		Schema: clusterPolicySchema,
		Importer: &schema.ResourceImporter{
			State: schema.ImportStatePassthrough,
		},
	}
}

func create(d *schema.ResourceData, m interface{}) error {
	client := m.(databricks.API)
	out, err := client.CreateClusterPolicy(context.Background(), &databricks.CreateClusterPolicyInput{
		Name:       d.Get("name").(string),
		Definition: d.Get("definition").(string),
	})

	if err != nil {
		return err
	}

	d.SetId(out.PolicyId)
	return err
}

func read(d *schema.ResourceData, m interface{}) error {
	client := m.(databricks.API)

	out, err := client.GetClusterPolicy(context.Background(), &databricks.GetClusterPolicyInput{PolicyId: d.Id()})
	if err != nil {
		if apiErr, ok := oops.Cause(err).(*databricks.APIError); ok {
			if apiErr.ErrorCode == databricks.ErrorCodeResourceDoesNotExist {
				// Indicate that we should remove this resource.
				d.SetId("")
				return nil
			}
		}
		return oops.Wrapf(err, "")
	}

	d.SetId(out.PolicyId)
	d.Set("name", out.Name)
	d.Set("definition", out.Definition)
	return nil
}

func update(d *schema.ResourceData, m interface{}) error {
	client := m.(databricks.API)
	_, err := client.EditClusterPolicy(context.Background(), &databricks.EditClusterPolicyInput{
		PolicyId:   d.Id(),
		Name:       d.Get("name").(string),
		Definition: d.Get("definition").(string),
	})

	return err
}

func clusterPolicyDelete(d *schema.ResourceData, m interface{}) error {
	client := m.(databricks.API)
	_, err := client.DeleteClusterPolicy(context.Background(), &databricks.DeleteClusterPolicyInput{
		PolicyId: d.Id(),
	})
	return err
}

// I don't think we actually need this. But i'm keeping them implemented here because
// every other terraform resource eventually needs them.
func flattenClusterPolicy(policy *databricks.ClusterPolicy) (map[string]interface{}, error) {
	out, err := json.Marshal(policy)
	if err != nil {
		return nil, oops.Wrapf(err, "failed to marshal cluster policy")
	}
	var jsonMap map[string]interface{}
	err = json.Unmarshal(out, &jsonMap)
	if err != nil {
		return nil, oops.Wrapf(err, "failed to unmarshal cluster policy into map")
	}

	return jsonMap, nil
}

func expandClusterPolicy(m map[string]interface{}) (*databricks.ClusterPolicy, error) {
	out, err := json.Marshal(m)
	if err != nil {
		return nil, oops.Wrapf(err, "failed to marshal map to json")
	}
	var policy databricks.ClusterPolicy
	err = json.Unmarshal(out, &policy)
	if err != nil {
		return nil, oops.Wrapf(err, "failed to unmarshal policy from json map")
	}
	return &policy, nil
}
