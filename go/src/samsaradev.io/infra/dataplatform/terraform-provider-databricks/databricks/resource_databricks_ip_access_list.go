package databricks

import (
	"context"

	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/dataplatform/databricks"
)

var ipAccessListSchema = map[string]*schema.Schema{
	"label": {
		Type:     schema.TypeString,
		Required: true,
	},
	"list_type": {
		Type:     schema.TypeString,
		Required: true,
	},
	"ip_addresses": {
		Type:     schema.TypeList,
		Required: true,
		Elem: &schema.Schema{
			Type: schema.TypeString,
		},
	},
	"enabled": {
		Type:     schema.TypeBool,
		Required: true,
	},
}

func resourceIPAccessList() *schema.Resource {
	return &schema.Resource{
		Create: resourceIPAccessListCreate,
		Read:   resourceIPAccessListRead,
		Update: resourceIPAccessListUpdate,
		Delete: resourceIPAccessListDelete,
		Importer: &schema.ResourceImporter{
			State: resourceIPAccessListImport,
		},
		Schema: ipAccessListSchema,
	}
}

func resourceIPAccessListCreate(d *schema.ResourceData, m interface{}) error {
	client := m.(databricks.API)

	ipAddresses, err := expandIPAddresses(d)
	if err != nil {
		return oops.Wrapf(err, "Unable to expand IP Addresses")
	}

	out, err := client.CreateIPAccessList(context.Background(), &databricks.CreateIPAccessListInput{
		IPAccessListInput: databricks.IPAccessListInput{
			Label:       d.Get("label").(string),
			ListType:    databricks.IPAccessListType(d.Get("list_type").(string)),
			IPAddresses: ipAddresses,
		},
	})
	if err != nil {
		return oops.Wrapf(err, "")
	}

	d.SetId(out.IPAccessList.ListId)
	err = setIPAccessListAttributes(d, out.IPAccessList)
	if err != nil {
		return oops.Wrapf(err, "")
	}
	return nil
}

func resourceIPAccessListRead(d *schema.ResourceData, m interface{}) error {
	client := m.(databricks.API)
	out, err := client.GetIPAccessList(context.Background(), &databricks.GetIPAccessListInput{
		IPAccessListId: d.Id(),
	})
	if err != nil {
		return oops.Wrapf(err, "")
	}

	err = setIPAccessListAttributes(d, out.IPAccessList)
	if err != nil {
		return oops.Wrapf(err, "")
	}
	return nil
}

func resourceIPAccessListUpdate(d *schema.ResourceData, m interface{}) error {
	client := m.(databricks.API)

	ipAddresses, err := expandIPAddresses(d)
	if err != nil {
		return oops.Wrapf(err, "Unable to expand IP Addresses")
	}

	out, err := client.EditIPAccessList(context.Background(), &databricks.EditIPAccessListInput{
		IPAccessListId: d.Id(),
		IPAccessListInput: databricks.IPAccessListInput{
			Label:       d.Get("label").(string),
			ListType:    databricks.IPAccessListType(d.Get("list_type").(string)),
			IPAddresses: ipAddresses,
		},
		Enabled: true,
	})
	if err != nil {
		return oops.Wrapf(err, "")
	}

	err = setIPAccessListAttributes(d, out.IPAccessList)
	if err != nil {
		return oops.Wrapf(err, "")
	}
	return nil
}

func resourceIPAccessListDelete(d *schema.ResourceData, m interface{}) error {
	client := m.(databricks.API)
	if err := client.DeleteIPAccessList(context.Background(), &databricks.DeleteIPAccessListInput{
		IPAccessListId: d.Id(),
	}); err != nil {
		return oops.Wrapf(err, "")
	}
	return nil
}

func resourceIPAccessListImport(d *schema.ResourceData, m interface{}) ([]*schema.ResourceData, error) {
	if err := resourceIPAccessListRead(d, m); err != nil {
		return nil, err
	}

	return []*schema.ResourceData{d}, nil
}

func setIPAccessListAttributes(d *schema.ResourceData, out databricks.IPAccessList) error {
	if err := d.Set("label", out.Label); err != nil {
		return oops.Wrapf(err, "update: label")
	}
	if err := d.Set("list_type", string(out.ListType)); err != nil {
		return oops.Wrapf(err, "update: list_type")
	}
	if err := d.Set("ip_addresses", out.IPAddresses); err != nil {
		return oops.Wrapf(err, "update: ip_addresses")
	}
	if err := d.Set("enabled", out.Enabled); err != nil {
		return oops.Wrapf(err, "update: enabled")
	}
	return nil
}

func expandIPAddresses(d *schema.ResourceData) ([]string, error) {
	var ipAddresses []string
	if ipAddressesIface, ok := d.Get("ip_addresses").([]interface{}); ok {
		for _, addr := range ipAddressesIface {
			ipAddresses = append(ipAddresses, addr.(string))
		}
	} else {
		return nil, oops.Errorf("Unable to cast ip_addresses to slice")
	}

	return ipAddresses, nil
}
