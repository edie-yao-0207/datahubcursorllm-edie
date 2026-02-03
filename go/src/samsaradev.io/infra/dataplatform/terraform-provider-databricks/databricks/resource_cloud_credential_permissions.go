package databricks

import (
	"context"
	"errors"
	"sort"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/dataplatform/databricks"
)

var cloudCredentialsPermissionsSchema = map[string]*schema.Schema{
	"name": &schema.Schema{
		Type:     schema.TypeString,
		Required: true,
		ForceNew: true,
	},
	"principals_with_access": &schema.Schema{
		Type:     schema.TypeSet,
		Required: true,
		Elem: &schema.Schema{
			Type: schema.TypeString,
		},
	},
}

func resourceCloudCredentialPermissions() *schema.Resource {
	return &schema.Resource{
		Create: cloudCredentialPermissionsCreate,
		Read:   cloudCredentialPermissionsRead,
		Update: cloudCredentialPermissionsUpdate,
		Delete: cloudCredentialPermissionsDelete,
		Schema: cloudCredentialsPermissionsSchema,
	}
}

// This resource is interesting in that theres no concept of "creation" or "deletion" of permissions, really.
// You just update the existing ones.

func cloudCredentialPermissionsCreate(d *schema.ResourceData, m interface{}) error {
	err := cloudCredentialPermissionsUpdate(d, m)
	if err != nil {
		return oops.Wrapf(err, "updating permissions")
	}
	name := d.Get("name").(string)
	d.SetId(name)
	return nil
}

func cloudCredentialPermissionsUpdate(d *schema.ResourceData, m interface{}) error {
	name := d.Get("name").(string)
	newPrincipals, err := extractNewPrincipals(d)
	if err != nil {
		return oops.Wrapf(err, "failed to extract new principals")
	}
	client := m.(databricks.API)
	return updateCredentialPermissions(client, name, newPrincipals)
}

func cloudCredentialPermissionsDelete(d *schema.ResourceData, m interface{}) error {
	name := d.Get("name").(string)
	newPrincipals := make(map[string]struct{})
	client := m.(databricks.API)
	err := updateCredentialPermissions(client, name, newPrincipals)
	if err != nil {
		if apiErr, ok := oops.Cause(err).(*databricks.APIError); ok {
			if strings.Contains(apiErr.Message, "does not exist") {
				// Indicate that we should remove this resource.
				// This usually happens when the underlying cloud credential doesn't exist anymore.
				// If that happens then these perms don't even exist so we can remove them.
				d.SetId("")
				return nil
			}
		}
		return oops.Wrapf(err, "failed to update permissions and remove everything")
	}
	d.SetId("")
	return nil
}

func cloudCredentialPermissionsRead(d *schema.ResourceData, m interface{}) error {
	client := m.(databricks.API)
	name := d.Get("name").(string)
	out, err := client.ListCredentialPermissions(context.Background(), &databricks.ListCredentialPermissionsInput{
		Name: name,
	})
	if err != nil {
		if apiErr, ok := oops.Cause(err).(*databricks.APIError); ok {
			if strings.Contains(apiErr.Message, "does not exist") {
				// Indicate that we should remove this resource.
				// This usually happens when the underlying cloud credential doesn't exist anymore.
				// If that happens then these perms don't even exist so we can remove them.
				d.SetId("")
				return nil
			}
		}
		return oops.Wrapf(err, "")
	}

	principals := make([]interface{}, 0, len(out.PrivilegeAssignments))
	for _, out := range out.PrivilegeAssignments {
		principals = append(principals, out.Principal)
	}

	err = d.Set("principals_with_access", principals)
	if err != nil {
		return oops.Wrapf(err, "failed setting principals_with_access")
	}
	return nil
}

func extractNewPrincipals(d *schema.ResourceData) (map[string]struct{}, error) {
	principals := d.Get("principals_with_access").(*schema.Set).List()
	newPrincipals := make(map[string]struct{})
	for _, p := range principals {
		principal, ok := p.(string)
		if !ok {
			return nil, errors.New("failed to convert principal to string")
		}
		newPrincipals[principal] = struct{}{}
	}
	return newPrincipals, nil
}

func updateCredentialPermissions(client databricks.API, name string, newPrincipals map[string]struct{}) error {
	out, err := client.ListCredentialPermissions(context.Background(), &databricks.ListCredentialPermissionsInput{
		Name: name,
	})
	if err != nil {
		return oops.Wrapf(err, "failed to list perms")
	}

	existing := make(map[string]struct{})
	for _, pa := range out.PrivilegeAssignments {
		// Note: theoretically there can be more permissions than just `ACCESS` but we just assume there aren't for
		// simplicity.
		existing[pa.Principal] = struct{}{}
	}

	add, remove := getRemovalAndCreation(existing, newPrincipals)
	var changes []databricks.Change
	for _, elem := range add {
		changes = append(changes, databricks.Change{
			Principal: elem,
			Add:       []string{databricks.CredentialAccess},
		})
	}
	for _, elem := range remove {
		changes = append(changes, databricks.Change{
			Principal: elem,
			Remove:    []string{databricks.CredentialAccess},
		})
	}

	_, err = client.ChangeCredentialPermissions(context.Background(), &databricks.PatchCredentialPermissionsInput{
		Name:    name,
		Changes: changes,
	})
	if err != nil {
		return oops.Wrapf(err, "failed to change perms")
	}
	return nil
}

func getRemovalAndCreation(old, new map[string]struct{}) ([]string, []string) {
	var additions, removals []string

	// Remove elements in old that are not in new.
	for k := range old {
		if _, ok := new[k]; !ok {
			removals = append(removals, k)
		}
	}

	// Add elements in new that are not in old.
	for k := range new {
		if _, ok := old[k]; !ok {
			additions = append(additions, k)
		}
	}

	// Sort for stability
	sort.Strings(additions)
	sort.Strings(removals)
	return additions, removals
}
