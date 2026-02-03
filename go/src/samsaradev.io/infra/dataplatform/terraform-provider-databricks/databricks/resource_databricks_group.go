package databricks

import (
	"context"
	"net/http"

	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/team/databricksadmins"
)

var groupSchema = map[string]*schema.Schema{
	"display_name": &schema.Schema{
		Type:     schema.TypeString,
		Required: true,
		ForceNew: true, // Databricks does not support updating group names.
	},
	"members": &schema.Schema{
		Type:     schema.TypeSet,
		Set:      schema.HashString,
		Optional: true,
		Elem: &schema.Schema{
			Type: schema.TypeString,
		},
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

func resourceGroup() *schema.Resource {
	return &schema.Resource{
		Create: resourceGroupCreate,
		Read:   resourceGroupRead,
		Update: resourceGroupUpdate,
		Delete: resourceGroupDelete,
		Importer: &schema.ResourceImporter{
			State: resourceGroupImport,
		},
		Schema: groupSchema,
	}
}

func expandMembers(membersIface []interface{}) []*databricks.GroupMember {
	members := make([]*databricks.GroupMember, 0)
	for _, iface := range membersIface {
		members = append(members, &databricks.GroupMember{
			Value: iface.(string),
		})
	}
	return members
}

func flattenMembers(members []*databricks.GroupMember) []interface{} {
	membersIface := make([]interface{}, 0)
	for _, member := range members {
		membersIface = append(membersIface, member.Value)
	}
	return membersIface
}

func resourceGroupCreate(d *schema.ResourceData, m interface{}) error {
	client := m.(databricks.API)
	out, err := client.CreateGroup(context.Background(), &databricks.CreateGroupInput{
		Group: databricks.Group{
			DisplayName:  d.Get("display_name").(string),
			Members:      expandMembers(d.Get("members").(*schema.Set).List()),
			Entitlements: expandEntitlements(d.Get("entitlements").(*schema.Set).List()),
		},
	})
	if err != nil {
		return oops.Wrapf(err, "")
	}
	d.SetId(out.Id)
	return nil
}

func resourceGroupRead(d *schema.ResourceData, m interface{}) error {
	client := m.(databricks.API)
	out, err := client.GetGroup(context.Background(), &databricks.GetGroupInput{
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
	if err := d.Set("entitlements", flattenEntitlements(out.Entitlements)); err != nil {
		return oops.Wrapf(err, "entitlements")
	}

	members := out.Members
	// Skip over superadmin principals that are not managed by terraform
	if out.DisplayName == databricksadmins.DatabricksAdminsGroup {
		members = []*databricks.GroupMember{}
		superAdminsMap := databricksadmins.MakeDatabricksAdminsMap(databricksadmins.DatabricksSuperAdmins)
		for _, m := range out.Members {
			if _, ok := superAdminsMap[m.Value]; !ok {
				members = append(members, m)
			}
		}
	}
	if err := d.Set("members", flattenMembers(members)); err != nil {
		return oops.Wrapf(err, "members")
	}
	if err := d.Set("display_name", out.DisplayName); err != nil {
		return oops.Wrapf(err, "display_name")
	}
	return nil
}

func resourceGroupUpdate(d *schema.ResourceData, m interface{}) error {
	client := m.(databricks.API)

	members := d.Get("members").(*schema.Set).List()
	// Insert superadmin principals that are not managed by terraform into 'admins' group
	// when any updates are performed
	groupName := d.Get("display_name").(string)
	if groupName == databricksadmins.DatabricksAdminsGroup {
		for _, sadmin := range databricksadmins.DatabricksSuperAdmins {
			var m interface{}
			m = sadmin
			members = append(members, m)
		}
	}
	_, err := client.PatchGroup(context.Background(), &databricks.PatchGroupInput{
		Id: d.Id(),
		Operations: []*databricks.PatchOperation{
			{
				Op:    "replace",
				Path:  "members",
				Value: expandMembers(members),
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

func resourceGroupDelete(d *schema.ResourceData, m interface{}) error {
	client := m.(databricks.API)
	if _, err := client.DeleteGroup(context.Background(), &databricks.DeleteGroupInput{
		Id: d.Id(),
	}); err != nil {
		return oops.Wrapf(err, "")
	}
	return nil
}

func resourceGroupImport(d *schema.ResourceData, m interface{}) ([]*schema.ResourceData, error) {
	if err := resourceGroupRead(d, m); err != nil {
		return nil, err
	}
	return []*schema.ResourceData{d}, nil
}
