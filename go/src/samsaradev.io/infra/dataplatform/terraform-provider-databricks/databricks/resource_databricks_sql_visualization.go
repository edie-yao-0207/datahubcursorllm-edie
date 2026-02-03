package databricks

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/dataplatform/databricks"
)

var visCompositeID = newCompositeId("query_id", "visualization_id", "/")

var sqlVisualizationSchema = map[string]*schema.Schema{
	"query_id": {
		Type:     schema.TypeString,
		Required: true,
	},
	"visualization_id": {
		Type:     schema.TypeString,
		Computed: true,
	},
	"type": {
		Type:     schema.TypeString,
		Required: true,
	},
	"name": {
		Type:     schema.TypeString,
		Required: true,
	},
	"description": {
		Type:     schema.TypeString,
		Optional: true,
	},
	"options": {
		Type:     schema.TypeString,
		Required: true,
		DiffSuppressFunc: func(_, old, new string, d *schema.ResourceData) bool {
			sanitizedOld, err := jsonRemarshal([]byte(old))
			if err != nil {
				log.Printf("[WARN] Unable to remarshal value %#v", old)
				return false
			}

			sanitizedNew, err := jsonRemarshal([]byte(new))
			if err != nil {
				log.Printf("[WARN] Unable to remarshal value %#v", new)
				return false
			}

			return bytes.Equal(sanitizedOld, sanitizedNew)
		},
	},
}

func expandVisualization(d map[string]interface{}) *databricks.Visualization {
	var visualization databricks.Visualization
	if v, ok := d["query_id"]; ok {
		visualization.QueryID = v.(string)
	}
	if v, ok := d["type"]; ok {
		visualization.Type = strings.ToUpper(v.(string))
	}
	if v, ok := d["name"]; ok {
		visualization.Name = v.(string)
	}
	if v, ok := d["description"]; ok {
		visualization.Description = v.(string)
	}
	if v, ok := d["options"]; ok {
		visualization.Options = json.RawMessage(v.(string))
	}
	return &visualization
}

func flattenVisualization(visualization *databricks.Visualization) map[string]interface{} {
	m := make(map[string]interface{})
	if visualization.QueryID != "" {
		m["query_id"] = visualization.QueryID
	} else {
		m["query_id"] = nil
	}
	if visualization.ID != "" {
		m["visualization_id"] = visualization.ID
	} else {
		m["visualization_id"] = nil
	}
	if visualization.Type != "" {
		m["type"] = strings.ToLower(visualization.Type)
	} else {
		m["type"] = nil
	}
	if visualization.Name != "" {
		m["name"] = visualization.Name
	} else {
		m["name"] = nil
	}
	if visualization.Description != "" {
		m["description"] = visualization.Description
	} else {
		m["description"] = nil
	}
	stringOptions := string(visualization.Options)
	if stringOptions != "" {
		m["options"] = stringOptions
	} else {
		m["options"] = nil
	}
	return m
}

func visualizationResourceDataToMap(d *schema.ResourceData) map[string]interface{} {
	m := make(map[string]interface{})
	for field := range sqlVisualizationSchema {
		m[field] = d.Get(field)
	}
	return m
}

// jsonRemarshal unmarshals, then marshals json.
// It is used to sanitize whitespace in the input.
// Two equivalent json representations with different whitespace
// will return the same result.
func jsonRemarshal(in []byte) ([]byte, error) {
	if len(in) == 0 {
		return in, nil
	}

	var v interface{}
	if err := json.Unmarshal(in, &v); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	out, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func resourceSqlVisualization() *schema.Resource {
	return &schema.Resource{
		Schema: sqlVisualizationSchema,
		Create: resourceSqlVisualizationCreate,
		Read:   resourceSqlVisualizationRead,
		Update: resourceSqlVisualizationUpdate,
		Delete: resourceSqlVisualizationDelete,
		Importer: &schema.ResourceImporter{
			State: resourceSqlVisualizationImport,
		},
	}
}

func resourceSqlVisualizationCreate(d *schema.ResourceData, m interface{}) error {
	client := m.(databricks.API)
	visMap := visualizationResourceDataToMap(d)
	visualization := expandVisualization(visMap)

	newVis, err := client.CreateSQLVisualization(context.Background(), visualization)
	if err != nil {
		return oops.Wrapf(err, "")
	}

	// Set the visualization_id so that we can package the composite key later.
	d.Set("visualization_id", newVis.ID)

	// Set the composite resource id.
	visCompositeID.Pack(d)
	return nil
}

func resourceSqlVisualizationRead(d *schema.ResourceData, m interface{}) error {
	queryID, visID, err := visCompositeID.Unpack(d)
	if err != nil {
		return oops.Wrapf(err, "")
	}

	client := m.(databricks.API)
	out, err := client.GetSQLVisualization(context.Background(), queryID, visID)
	if err != nil {
		if apiErr, ok := oops.Cause(err).(*databricks.APIError); ok {
			if strings.Contains(apiErr.Message, "does not exist") {
				d.SetId("")
				return nil
			}
		}
		return oops.Wrapf(err, "")
	}

	kv := flattenVisualization(out)
	for k, v := range kv {
		if err := d.Set(k, v); err != nil {
			return oops.Wrapf(err, "%+v: %+v", k, v)
		}
	}
	return nil
}

func resourceSqlVisualizationUpdate(d *schema.ResourceData, m interface{}) error {
	_, visID, err := visCompositeID.Unpack(d)
	if err != nil {
		return oops.Wrapf(err, "")
	}

	client := m.(databricks.API)
	visMap := visualizationResourceDataToMap(d)
	visualization := expandVisualization(visMap)

	if err := client.EditSQLVisualization(context.Background(), visID, visualization); err != nil {
		return oops.Wrapf(err, "")
	}
	return nil
}

func resourceSqlVisualizationDelete(d *schema.ResourceData, m interface{}) error {
	_, visID, err := visCompositeID.Unpack(d)
	if err != nil {
		return oops.Wrapf(err, "")
	}

	client := m.(databricks.API)
	if err := client.DeleteSQLVisualization(context.Background(), visID); err != nil {
		return oops.Wrapf(err, "")
	}
	return nil
}

func resourceSqlVisualizationImport(d *schema.ResourceData, m interface{}) ([]*schema.ResourceData, error) {
	if err := resourceSqlVisualizationRead(d, m); err != nil {
		return nil, err
	}
	return []*schema.ResourceData{d}, nil
}
