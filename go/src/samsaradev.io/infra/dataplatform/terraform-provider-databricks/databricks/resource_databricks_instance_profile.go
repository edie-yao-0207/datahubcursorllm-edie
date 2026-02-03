package databricks

import (
	"context"

	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/dataplatform/databricks"
)

var instanceProfileSchema = map[string]*schema.Schema{
	"instance_profile_arn": &schema.Schema{
		Type:     schema.TypeString,
		Required: true,
	},
	"skip_validation": &schema.Schema{
		Type:     schema.TypeBool,
		Optional: true,
	},
	"is_meta_instance_profile": &schema.Schema{
		Type:     schema.TypeBool,
		Computed: true,
	},
	"iam_role_arn": &schema.Schema{
		Type:     schema.TypeString,
		Optional: true,
	},
}

func resourceInstanceProfile() *schema.Resource {
	return &schema.Resource{
		Create: resourceInstanceProfileCreate,
		Read:   resourceInstanceProfileRead,
		Update: resourceInstanceProfileUpdate,
		Delete: resourceInstanceProfileDelete,
		Importer: &schema.ResourceImporter{
			State: resourceInstanceProfileImport,
		},
		Schema: instanceProfileSchema,
	}
}

func resourceInstanceProfileCreate(d *schema.ResourceData, m interface{}) error {
	client := m.(databricks.API)
	arn := d.Get("instance_profile_arn").(string)

	_, err := client.AddInstanceProfile(context.Background(), &databricks.AddInstanceProfileInput{
		InstanceProfileArn: arn,
		SkipValidation:     d.Get("skip_validation").(bool),
		IamRoleArn:         d.Get("iam_role_arn").(string),
	})
	if err != nil {
		return oops.Wrapf(err, "")
	}

	d.SetId(arn)
	return nil
}

func resourceInstanceProfileRead(d *schema.ResourceData, m interface{}) error {
	client := m.(databricks.API)
	out, err := client.ListInstanceProfiles(context.Background(), &databricks.ListInstanceProfilesInput{})
	if err != nil {
		return oops.Wrapf(err, "")
	}

	var exists bool
	for _, instanceProfile := range out.InstanceProfiles {
		if instanceProfile.InstanceProfileArn == d.Id() {
			exists = true
			if err := d.Set("instance_profile_arn", instanceProfile.InstanceProfileArn); err != nil {
				return oops.Wrapf(err, "instance_profile_arn")
			}
			if err := d.Set("is_meta_instance_profile", instanceProfile.IsMetaInstanceProfile); err != nil {
				return oops.Wrapf(err, "is_meta_instance_profile")
			}
			if err := d.Set("iam_role_arn", instanceProfile.IamRoleArn); err != nil {
				return oops.Wrapf(err, "iam_role_arn")
			}
		}
	}
	if !exists {
		d.SetId("")
	}
	return nil
}

func resourceInstanceProfileUpdate(d *schema.ResourceData, m interface{}) error {
	client := m.(databricks.API)
	if _, err := client.EditInstanceProfile(context.Background(), &databricks.EditInstanceProfileInput{
		InstanceProfileArn: d.Id(),
		IamRoleArn:         d.Get("iam_role_arn").(string),
	}); err != nil {
		return oops.Wrapf(err, "")
	}
	return nil
}

func resourceInstanceProfileDelete(d *schema.ResourceData, m interface{}) error {
	client := m.(databricks.API)
	if _, err := client.RemoveInstanceProfile(context.Background(), &databricks.RemoveInstanceProfileInput{
		InstanceProfileArn: d.Id(),
	}); err != nil {
		return oops.Wrapf(err, "")
	}
	d.SetId("")
	return nil
}

func resourceInstanceProfileImport(d *schema.ResourceData, m interface{}) ([]*schema.ResourceData, error) {
	if err := resourceInstanceProfileRead(d, m); err != nil {
		return nil, err
	}
	return []*schema.ResourceData{d}, nil
}
