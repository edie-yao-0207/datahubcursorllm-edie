package databricks

import (
	"context"
	"fmt"

	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/dataplatform/databricks"
)

// dataSourceUser returns information about user specified by user name or user id.
func dataSourceUser() *schema.Resource {
	return &schema.Resource{
		Schema: map[string]*schema.Schema{
			"user_name": {
				Type:          schema.TypeString,
				Optional:      true,
				ConflictsWith: []string{"user_id"},
			},
			"user_id": {
				Type:          schema.TypeString,
				Optional:      true,
				ConflictsWith: []string{"user_name"},
			},
			"display_name": {
				Type:     schema.TypeString,
				Computed: true,
			},
		},
		Read: getUser,
	}
}

// lookupUser looks up a user from the databricks API using userId or email.
// If userId is provided, that is used first, email is used as a fallback if no id is provided.
// If a userId is provided, and no user is found, an error is returned and fallback to email is not tried.
func lookupUser(api databricks.API, userId, email string) (*databricks.User, error) {
	// Look up by id if specified.
	if userId != "" {
		output, err := api.GetUser(context.Background(), &databricks.GetUserInput{
			Id: userId,
		})
		if err != nil {
			return nil, oops.Wrapf(err, "")
		}
		return &output.User, err
	}

	// Otherwise fall back to email.
	userList, err := api.GetUsers(context.Background(), &databricks.GetUsersInput{
		Attributes: []string{"id", "userName", "displayName"},
		Filter:     fmt.Sprintf("userName eq '%s'", email),
	})
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	if len(userList.Users) == 0 {
		return nil, oops.Errorf("user not found %s", email)
	}
	return userList.Users[0], nil
}

// getUser looks up a user by Id or email from the databricks API.
// It sets the non-specified properties of the user on the schema resource data.
func getUser(d *schema.ResourceData, m interface{}) error {
	client := m.(databricks.API)

	user, err := lookupUser(client, d.Get("user_id").(string), d.Get("user_name").(string))
	if err != nil {
		return oops.Wrapf(err, "")
	}

	// Set properties on the resource data based on the user we looked up.
	d.SetId(user.Id)
	d.Set("user_name", user.UserName)
	d.Set("display_name", user.DisplayName)
	return nil
}
