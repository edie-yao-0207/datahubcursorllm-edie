package databricks

import (
	"context"
	"fmt"

	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/dataplatform/databricks"
)

var permissionsSchema = map[string]*schema.Schema{
	"object_type": &schema.Schema{
		Type:     schema.TypeString,
		Required: true,
		ForceNew: true,
	},
	"object_id": &schema.Schema{
		Type:     schema.TypeString,
		Required: true,
		ForceNew: true,
	},
	"access_control": &schema.Schema{
		Type:     schema.TypeSet,
		Required: true,
		Elem: &schema.Resource{
			Schema: map[string]*schema.Schema{
				"user_name": &schema.Schema{
					Type:     schema.TypeString,
					Optional: true,
				},
				"group_name": &schema.Schema{
					Type:     schema.TypeString,
					Optional: true,
				},
				"service_principal_name": &schema.Schema{
					Type:     schema.TypeString,
					Optional: true,
				},
				"permission_level": &schema.Schema{
					Type:     schema.TypeString,
					Required: true,
				},
			},
		},
	},
}

func resourcePermissions() *schema.Resource {
	return &schema.Resource{
		Create: resourcePermissionsCreate,
		Read:   resourcePermissionsRead,
		Update: resourcePermissionsUpdate,
		Delete: resourcePermissionsDelete,
		Importer: &schema.ResourceImporter{
			State: resourcePermissionsImport,
		},
		Schema: permissionsSchema,
	}
}

func resourcePermissionsCreate(d *schema.ResourceData, m interface{}) error {
	return resourcePermissionsUpdate(d, m)
}

func resourcePermissionsUpdate(d *schema.ResourceData, m interface{}) error {
	client := m.(databricks.API)
	objectType := databricks.ObjectType(d.Get("object_type").(string))
	objectId := d.Get("object_id").(string)
	reqs := expandPermissions(d.Get("access_control").(*schema.Set).List())

	out, err := client.PutPermissions(context.Background(), &databricks.PutPermissionsInput{
		ObjectType:        objectType,
		ObjectId:          objectId,
		AccessControlList: reqs,
	})
	if err != nil {
		return oops.Wrapf(err, "")
	}

	d.SetId(fmt.Sprintf("%s-%s", objectType, objectId))
	if err := d.Set("access_control", flattenPermissions(out.AccessControlList)); err != nil {
		return oops.Wrapf(err, "access_control")
	}
	return nil
}

func resourcePermissionsRead(d *schema.ResourceData, m interface{}) error {
	client := m.(databricks.API)
	objectType := databricks.ObjectType(d.Get("object_type").(string))
	objectId := d.Get("object_id").(string)
	out, err := client.GetPermissions(context.Background(), &databricks.GetPermissionsInput{
		ObjectType: objectType,
		ObjectId:   objectId,
	})
	if err != nil {
		if aerr, ok := oops.Cause(err).(*databricks.APIError); ok {
			if aerr.ErrorCode == databricks.ErrorCodeResourceDoesNotExist {
				d.SetId("")
				return nil
			}
		}
		return oops.Wrapf(err, "")
	}

	d.SetId(fmt.Sprintf("%s-%s", objectType, objectId))
	if err := d.Set("access_control", flattenPermissions(out.AccessControlList)); err != nil {
		return oops.Wrapf(err, "access_control")
	}
	return nil
}

func resourcePermissionsDelete(d *schema.ResourceData, m interface{}) error {
	// Do not unset permission because we don't know the default to set to.
	return nil
}

func resourcePermissionsImport(d *schema.ResourceData, m interface{}) ([]*schema.ResourceData, error) {
	if err := resourcePermissionsRead(d, m); err != nil {
		return nil, err
	}
	return []*schema.ResourceData{d}, nil
}

func expandPermissions(d []interface{}) []*databricks.AccessControlRequest {
	var reqs []*databricks.AccessControlRequest
	for _, iface := range d {
		d := iface.(map[string]interface{})
		var req databricks.AccessControlRequest
		if userName, ok := d["user_name"]; ok {
			req.AccessControlPrincipal.UserName = userName.(string)
		}
		if groupName, ok := d["group_name"]; ok {
			req.AccessControlPrincipal.GroupName = groupName.(string)
		}
		if servicePrincipalName, ok := d["service_principal_name"]; ok {
			req.AccessControlPrincipal.ServicePrincipalName = servicePrincipalName.(string)
		}
		if permissionLevel, ok := d["permission_level"]; ok {
			req.PermissionLevel = databricks.PermissionLevel(permissionLevel.(string))
		}
		reqs = append(reqs, &req)
	}
	return reqs
}

func flattenPermissions(acl []*databricks.AccessControlResponse) []interface{} {
	var out []interface{}
	for _, ac := range acl {
		// 'admins' and 'groups' are 'system groups', which I don't think we can modify.
		// Ref: https://docs.databricks.com/en/administration-guide/users-groups/groups.html#difference-between-account-groups-and-workspace-local-groups
		if ac.GroupName == "admins" && ac.PermissionLevel != nil && *ac.PermissionLevel == databricks.PermissionLevelCanManage {
			continue
		}
		if ac.GroupName == "users" && ac.PermissionLevel != nil && *ac.PermissionLevel == databricks.PermissionLevelCanView {
			continue
		}
		var permissionLevel *databricks.PermissionLevel

		// Some objects contain the permission level at the root of the AccessControlResponse
		if ac.PermissionLevel != nil {
			permissionLevel = ac.PermissionLevel
		} else {
			// Others contain it nested in the AllPermissions slice.
			for _, perm := range ac.AllPermissions {
				if perm.Inherited {
					continue
				}
				permissionLevel = &perm.PermissionLevel
			}
		}
		if permissionLevel == nil {
			// Ignore inherited permission.
			continue
		}

		m := make(map[string]interface{})
		m["permission_level"] = *permissionLevel
		if ac.AccessControlPrincipal.UserName != "" {
			m["user_name"] = ac.AccessControlPrincipal.UserName
		} else {
			m["user_name"] = nil
		}
		if ac.AccessControlPrincipal.GroupName != "" {
			m["group_name"] = ac.AccessControlPrincipal.GroupName
		} else {
			m["group_name"] = nil
		}
		if ac.AccessControlPrincipal.ServicePrincipalName != "" {
			m["service_principal_name"] = ac.AccessControlPrincipal.ServicePrincipalName
		} else {
			m["service_principal_name"] = nil
		}
		out = append(out, m)
	}
	return out
}
