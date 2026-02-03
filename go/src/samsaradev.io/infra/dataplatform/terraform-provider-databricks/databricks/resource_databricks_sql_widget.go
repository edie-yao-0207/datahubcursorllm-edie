package databricks

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"reflect"
	"sort"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/dataplatform/databricks"
)

var widgetCompositeID = newCompositeId("dashboard_id", "widget_id", "/")

var sqlWidgetSchema = map[string]*schema.Schema{
	"dashboard_id": {
		Type:     schema.TypeString,
		Required: true,
	},
	"widget_id": {
		Type:     schema.TypeString,
		Computed: true,
	},
	"visualization_id": {
		Type:     schema.TypeString,
		Optional: true,
		DiffSuppressFunc: func(_, old, new string, d *schema.ResourceData) bool {
			return extractVisualizationID(old) == extractVisualizationID(new)
		},
	},
	"name": {
		// Not used in the api, but used as our string identifier.
		Type:     schema.TypeString,
		Required: true,
	},
	"text": {
		Type:          schema.TypeString,
		Optional:      true,
		ConflictsWith: []string{"visualization_id"},
	},
	"options": {
		Type:     schema.TypeList,
		Required: true,
		MinItems: 1,
		MaxItems: 1,
		Elem: &schema.Resource{
			Schema: map[string]*schema.Schema{
				"title": {
					Type:     schema.TypeString,
					Optional: true,
				},
				"description": {
					Type:     schema.TypeString,
					Optional: true,
				},
				"parameter_mapping": {
					Type:     schema.TypeList,
					Optional: true,
					Elem: &schema.Resource{
						Schema: map[string]*schema.Schema{
							"name": {
								Type:     schema.TypeString,
								Required: true,
							},
							"type": {
								Type:     schema.TypeString,
								Required: true,
							},
							"map_to": {
								Type:     schema.TypeString,
								Required: true,
							},
							"title": {
								Type:     schema.TypeString,
								Optional: true,
							},
							"value": {
								Type:     schema.TypeString,
								Optional: true,
							},
							"values": {
								Type:     schema.TypeList,
								Optional: true,
								Elem: &schema.Schema{
									Type: schema.TypeString,
								},
							},
						},
					},
				},
				"position": {
					Type:     schema.TypeList,
					MinItems: 1,
					MaxItems: 1,
					Required: true,
					Elem: &schema.Resource{
						Schema: map[string]*schema.Schema{
							"auto_height": {
								Type:     schema.TypeBool,
								Optional: true,
								Default:  false,
							},
							"size_x": {
								Type:     schema.TypeInt,
								Required: true,
							},
							"size_y": {
								Type:     schema.TypeInt,
								Required: true,
							},
							"pos_x": {
								Type:     schema.TypeInt,
								Required: true,
							},
							"pos_y": {
								Type:     schema.TypeInt,
								Required: true,
							},
						},
					},
				},
			},
		},
	},
	"visualization": {
		Type:     schema.TypeString,
		Optional: true,
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

// extractVisualizationID uses second part of ID if it's a composite ID,
// or the verbatim value. This allows setting the visualization ID as the
// backend visualization ID or as the visualization's resource composite ID.
// Both will work.
func extractVisualizationID(id string) string {
	parts := strings.SplitN(id, "/", 2)
	return parts[len(parts)-1]
}

func expandWidget(d map[string]interface{}) *databricks.Widget {
	var widget databricks.Widget

	if v, ok := d["dashboard_id"]; ok {
		widget.DashboardID = v.(string)
	}
	if v, ok := d["visualization_id"]; ok {
		visID := v.(string)
		widget.VisualizationID = &visID
	}
	if v, ok := d["text"]; ok {
		text := v.(string)
		widget.Text = &text
	}
	if v, ok := d["options"]; ok && len(v.([]interface{})) == 1 {
		widget.Options = databricks.WidgetOptions{}
		options := v.([]interface{})[0].(map[string]interface{})

		if v, ok := options["title"]; ok {
			widget.Options.Title = v.(string)
		}
		if v, ok := options["description"]; ok {
			widget.Options.Description = v.(string)
		}

		if v, ok := options["position"]; ok && len(v.([]interface{})) == 1 {
			widget.Options.Position = &databricks.WidgetPosition{}
			position := v.([]interface{})[0].(map[string]interface{})

			if v, ok := position["auto_height"]; ok {
				widget.Options.Position.AutoHeight = v.(bool)
			}
			if v, ok := position["size_x"]; ok {
				widget.Options.Position.SizeX = v.(int)
			}
			if v, ok := position["size_y"]; ok {
				widget.Options.Position.SizeY = v.(int)
			}
			if v, ok := position["pos_x"]; ok {
				widget.Options.Position.PosX = v.(int)
			}
			if v, ok := position["pos_y"]; ok {
				widget.Options.Position.PosY = v.(int)
			}
		}

		if v, ok := options["parameter_mapping"]; ok && len(v.([]interface{})) > 0 {
			widget.Options.ParameterMapping = make(map[string]databricks.WidgetParameterMapping)
			paramMapping := v.([]interface{})

			for _, mapping := range paramMapping {
				widgetParamMapping := databricks.WidgetParameterMapping{}

				params := mapping.(map[string]interface{})
				var param string

				if v, ok := params["name"]; ok {
					widgetParamMapping.Name = v.(string)
					param = v.(string)
				}
				if v, ok := params["type"]; ok {
					widgetParamMapping.Type = v.(string)
				}
				if v, ok := params["map_to"]; ok {
					widgetParamMapping.MapTo = v.(string)
				}
				if v, ok := params["title"]; ok {
					widgetParamMapping.Title = v.(string)
				}

				// Values might be a single string, or an array of strings.
				if v, ok := params["value"]; ok {
					widgetParamMapping.Value = v.(string)
				} else if v, ok := params["values"]; ok {
					widgetParamMapping.Value = v.([]string)
				}

				widget.Options.ParameterMapping[param] = widgetParamMapping
			}
		}
	}

	return &widget
}

func flattenWidget(widget *databricks.Widget) map[string]interface{} {
	m := make(map[string]interface{})
	if widget.DashboardID != "" {
		m["dashboard_id"] = widget.DashboardID
	}
	if widget.ID != "" {
		m["widget_id"] = widget.ID
	}
	if widget.VisualizationID != nil {
		m["visualization_id"] = fmt.Sprint(*widget.VisualizationID)
	}
	if widget.Text != nil {
		m["text"] = *widget.Text
	}

	widgetOptions := make(map[string]interface{})
	if widget.Options.Title != "" {
		widgetOptions["title"] = widget.Options.Title
	}
	if widget.Options.Description != "" {
		widgetOptions["description"] = widget.Options.Description
	}

	if widget.Options.Position != nil {
		position := make(map[string]interface{})
		position["auto_height"] = widget.Options.Position.AutoHeight
		position["size_x"] = widget.Options.Position.SizeX
		position["size_y"] = widget.Options.Position.SizeY
		position["pos_x"] = widget.Options.Position.PosX
		position["pos_y"] = widget.Options.Position.PosY
		widgetOptions["position"] = []interface{}{position}
	}

	paramMappings := make([]interface{}, len(widget.Options.ParameterMapping))
	i := 0
	for _, mapping := range widget.Options.ParameterMapping {
		mappings := make(map[string]interface{})
		mappings["name"] = mapping.Name
		mappings["type"] = mapping.Type
		mappings["map_to"] = mapping.MapTo
		mappings["title"] = mapping.Title

		// Value might be a string or a []string.
		switch val := mapping.Value.(type) {
		case string:
			mappings["value"] = val
		case []string:
			mappings["values"] = val
		case nil:
		default:
			log.Fatalf("Don't know what to do for type: %#v", reflect.TypeOf(val).String())
		}
		paramMappings[i] = mappings
		i++
	}
	sort.Slice(paramMappings, func(i, j int) bool {
		a := paramMappings[i].(map[string]interface{})["name"].(string)
		b := paramMappings[j].(map[string]interface{})["name"].(string)

		return a < b
	})
	widgetOptions["parameter_mapping"] = paramMappings
	m["options"] = []interface{}{widgetOptions}

	return m
}

func widgetResourceDataToMap(d *schema.ResourceData) map[string]interface{} {
	m := make(map[string]interface{})
	for field := range sqlWidgetSchema {
		m[field] = d.Get(field)
	}
	return m
}

func resourceSqlWidget() *schema.Resource {
	return &schema.Resource{
		Schema: sqlWidgetSchema,
		Create: resourceSqlWidgetCreate,
		Read:   resourceSqlWidgetRead,
		Update: resourceSqlWidgetUpdate,
		Delete: resourceSqlWidgetDelete,
		Importer: &schema.ResourceImporter{
			State: resourceSqlWidgetImport,
		},
	}
}

func resourceSqlWidgetCreate(d *schema.ResourceData, m interface{}) error {
	client := m.(databricks.API)
	widgetMap := widgetResourceDataToMap(d)
	widget := expandWidget(widgetMap)

	newWidget, err := client.CreateSQLWidget(context.Background(), widget)
	if err != nil {
		return oops.Wrapf(err, "")
	}

	// Set the "widget_id" so that we can package the composite id.
	d.Set("widget_id", newWidget.ID)
	widgetCompositeID.Pack(d)
	return nil
}

func resourceSqlWidgetRead(d *schema.ResourceData, m interface{}) error {
	dashID, widgetID, err := widgetCompositeID.Unpack(d)
	if err != nil {
		return oops.Wrapf(err, "")
	}

	client := m.(databricks.API)
	out, err := client.GetSQLWidget(context.Background(), dashID, widgetID)
	if err != nil {
		if apiErr, ok := oops.Cause(err).(*databricks.APIError); ok {
			if strings.Contains(apiErr.Message, "does not exist") {
				d.SetId("")
				return nil
			}
		}
		return oops.Wrapf(err, "")
	}

	kv := flattenWidget(out)
	for k, v := range kv {
		if err := d.Set(k, v); err != nil {
			return oops.Wrapf(err, "%+v: %+v", k, v)
		}
	}
	return nil
}

func resourceSqlWidgetUpdate(d *schema.ResourceData, m interface{}) error {
	_, widgetID, err := widgetCompositeID.Unpack(d)
	if err != nil {
		return oops.Wrapf(err, "")
	}

	client := m.(databricks.API)
	widgetMap := widgetResourceDataToMap(d)
	widget := expandWidget(widgetMap)

	if err := client.EditSQLWidget(context.Background(), widgetID, widget); err != nil {
		return oops.Wrapf(err, "")
	}
	return nil
}

func resourceSqlWidgetDelete(d *schema.ResourceData, m interface{}) error {
	_, widgetID, err := widgetCompositeID.Unpack(d)
	if err != nil {
		return oops.Wrapf(err, "")
	}

	client := m.(databricks.API)
	if err := client.DeleteSQLWidget(context.Background(), widgetID); err != nil {
		return oops.Wrapf(err, "")
	}
	return nil
}

func resourceSqlWidgetImport(d *schema.ResourceData, m interface{}) ([]*schema.ResourceData, error) {
	if err := resourceSqlWidgetRead(d, m); err != nil {
		return nil, err
	}
	return []*schema.ResourceData{d}, nil
}
