package databricks

import (
	"context"
	"net/http"

	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/dataplatform/databricks"
)

var userSchema = map[string]*schema.Schema{
	"user_name": &schema.Schema{
		Type:     schema.TypeString,
		ForceNew: true,
		Required: true,
	},
	"display_name": &schema.Schema{
		Type:     schema.TypeString,
		Required: true,
	},
	"entitlements": &schema.Schema{
		Type:     schema.TypeSet,
		Set:      schema.HashString,
		Optional: true,
		Elem: &schema.Schema{
			Type: schema.TypeString,
		},
	},
}

func expandUser(d *schema.ResourceData) databricks.User {
	return databricks.User{
		UserName:    d.Get("user_name").(string),
		DisplayName: d.Get("display_name").(string),
	}
}

func resourceUser() *schema.Resource {
	return &schema.Resource{
		Create: resourceUserCreate,
		Read:   resourceUserRead,
		Update: resourceUserUpdate,
		Delete: resourceUserDelete,
		Importer: &schema.ResourceImporter{
			State: resourceUserImport,
		},
		Schema: userSchema,
	}
}

func expandEntitlements(ifaces []interface{}) []*databricks.Entitlement {
	entitlements := make([]*databricks.Entitlement, 0)
	for _, iface := range ifaces {
		entitlements = append(entitlements, &databricks.Entitlement{
			Value: iface.(string),
		})
	}
	return entitlements
}

func flattenEntitlements(entitlements []*databricks.Entitlement) []interface{} {
	ifaces := make([]interface{}, 0)
	for _, entitlement := range entitlements {
		ifaces = append(ifaces, entitlement.Value)
	}
	return ifaces
}

func resourceUserCreate(d *schema.ResourceData, m interface{}) error {
	client := m.(databricks.API)

	out, err := client.CreateUser(context.Background(), &databricks.CreateUserInput{
		User: expandUser(d),
	})
	if err != nil {
		return oops.Wrapf(err, "")
	}

	d.SetId(out.Id)
	return nil
}

func resourceUserRead(d *schema.ResourceData, m interface{}) error {
	client := m.(databricks.API)
	out, err := client.GetUser(context.Background(), &databricks.GetUserInput{
		Id: d.Id(),
	})
	if err != nil {
		if httpError, ok := oops.Cause(err).(*databricks.HTTPStatusError); ok {
			if httpError.StatusCode == http.StatusNotFound {
				d.SetId("")
				return nil
			}
		}
		return oops.Wrapf(err, "")
	}

	if err := d.Set("user_name", out.UserName); err != nil {
		return oops.Wrapf(err, "user_name")
	}
	if err := d.Set("display_name", out.DisplayName); err != nil {
		return oops.Wrapf(err, "display_name")
	}
	if err := d.Set("entitlements", flattenEntitlements(out.Entitlements)); err != nil {
		return oops.Wrapf(err, "entitlements")
	}
	return nil
}

func resourceUserUpdate(d *schema.ResourceData, m interface{}) error {
	client := m.(databricks.API)

	// Use Patch API to prevent updating other information such as group membership.
	_, err := client.PatchUser(context.Background(), &databricks.PatchUserInput{
		Id: d.Id(),
		Operations: []*databricks.PatchOperation{
			{
				Op:   "replace",
				Path: "userName",
				Value: []*databricks.PatchOperationValue{
					{
						Value: d.Get("user_name").(string),
					},
				},
			},
			{
				Op:   "replace",
				Path: "displayName",
				Value: []*databricks.PatchOperationValue{
					{
						Value: d.Get("display_name").(string),
					},
				},
			},
			{
				Op:    "replace",
				Path:  "entitlements",
				Value: expandEntitlements(d.Get("entitlements").(*schema.Set).List()),
			},
		},
	})
	if err != nil {
		return oops.Wrapf(err, "")
	}
	return nil
}

func resourceUserDelete(d *schema.ResourceData, m interface{}) error {
	client := m.(databricks.API)
	if _, err := client.DeleteUser(context.Background(), &databricks.DeleteUserInput{
		Id: d.Id(),
	}); err != nil {
		return oops.Wrapf(err, "")
	}
	return nil
}

func resourceUserImport(d *schema.ResourceData, m interface{}) ([]*schema.ResourceData, error) {
	if err := resourceUserRead(d, m); err != nil {
		return nil, err
	}
	return []*schema.ResourceData{d}, nil
}
