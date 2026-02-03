package databricks

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"github.com/samsarahq/go/oops"
)

// compositeID is a Composite Resource identifier for a terraform resource.
// It is useful whenever you need 2 pieces of information to look up a resource.
//
// For example if you have a Fridge resource, and a Food resource, and in order
// to look up a Food, you also need to provide a FridgeID you might:
//
// Create a newCompositeID("fridge_id", "food_id", "/")
// Call Pack() when you create a new food resource. This will set the compositeID on the food resource.
// Call Unpack() whenever you need to look up a food resource - this will return to you both the:
// - FridgeID
// - FoodID
type compositeID struct {
	left, right string
	separator   string
	schema      map[string]*schema.Schema
}

// newCompositeId returns a new compositeID.
func newCompositeId(left, right, separator string) *compositeID {
	return &compositeID{
		left:      left,
		right:     right,
		separator: separator,
		schema: map[string]*schema.Schema{
			left: {
				Type:     schema.TypeString,
				ForceNew: true,
				Required: true,
			},
			right: {
				Type:     schema.TypeString,
				ForceNew: true,
				Required: true,
			},
		},
	}
}

// Pack sets the "id" field on the terraform resource to be the composite Id.
// Effectively, this encodes all pieces of information needed to look up the
// resource into a single ID.
func (c *compositeID) Pack(d *schema.ResourceData) {
	d.SetId(fmt.Sprintf("%s%s%v", d.Get(c.left), c.separator, d.Get(c.right)))
}

// Unpack decomposes the ID of the resource into the original composite parts.
// It also sets the composite pieces as properties on the resource.
func (c *compositeID) Unpack(d *schema.ResourceData) (string, string, error) {
	id := d.Id()
	parts := strings.SplitN(id, c.separator, 2)

	if len(parts) != 2 {
		d.SetId("")
		return "", "", oops.Errorf("invalid ID: %s", id)
	}
	if parts[0] == "" {
		d.SetId("")
		return "", "", oops.Errorf("%s cannot be empty", c.left)
	}
	if parts[1] == "" {
		d.SetId("")
		return "", "", oops.Errorf("%s cannot be empty", c.right)
	}

	d.Set(c.left, parts[0])
	if c.schema[c.right].Type == schema.TypeInt {
		i64, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return parts[0], parts[1], err
		}
		d.Set(c.right, i64)
	} else {
		d.Set(c.right, parts[1])
	}
	return parts[0], parts[1], nil
}
