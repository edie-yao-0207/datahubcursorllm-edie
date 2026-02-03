package databricks

import (
	"context"
	"encoding/json"
	"hash/fnv"
	"log"
	"reflect"
	"sort"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/dataplatform/databricks"
)

// stringHash takes a string and returns an int representing
// a basic hash of that string.
var stringHash = func(i interface{}) int {
	s := i.(string)
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32())
}

var sqlQuerySchema = map[string]*schema.Schema{
	"data_source_id": {
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
	"query": {
		Type:     schema.TypeString,
		Required: true,
	},
	"run_as_role": {
		Type:     schema.TypeString,
		Optional: true,
		Default:  "viewer",
	},
	// TODO: we need to add `ForceNew` as well here.
	"parent": {
		Type:     schema.TypeString,
		Optional: true,
		DiffSuppressFunc: func(k, old, new string, d *schema.ResourceData) bool {
			// In cases where we didn't set the parent field, `new` will be ""
			// but `old` will usually have some value like folders/129832728 set
			// by the databricks API. To avoid constant diffs, we suppress this diff.
			// This mimics behavior in the official provider.
			// https://github.com/databricks/terraform-provider-databricks/blob/main/sql/resource_sql_query.go#L28C1-L29C1
			// Note this means that you can't move a query from having a parent to not having a parent.
			// This feels like an okay trade-off given that we should probably not upload things to the
			// root parent anyways.
			if new == "" {
				return true
			}
			return false
		},
	},
	"tags": {
		Type:     schema.TypeSet,
		Optional: true,
		Elem: &schema.Schema{
			Type: schema.TypeString,
		},
	},
	"parameter": {
		Type:     schema.TypeList,
		Optional: true,
		Elem: &schema.Resource{
			Schema: map[string]*schema.Schema{
				"name": {
					Type:     schema.TypeString,
					Required: true,
				},
				"title": {
					Type:     schema.TypeString,
					Required: true,
				},
				// Only one of the following may be set.
				"text": {
					Type:          schema.TypeList,
					Optional:      true,
					MinItems:      0,
					MaxItems:      1,
					ConflictsWith: []string{"parameter.number", "parameter.date_range", "parameter.query", "parameter.date_range_relative"},
					Elem: &schema.Resource{
						Schema: map[string]*schema.Schema{
							"value": {
								Type:     schema.TypeString,
								Required: true,
							},
						},
					},
				},
				"number": {
					Type:          schema.TypeList,
					Optional:      true,
					MinItems:      0,
					MaxItems:      1,
					ConflictsWith: []string{"parameter.text", "parameter.date_range", "parameter.query", "parameter.date_range_relative"},
					Elem: &schema.Resource{
						Schema: map[string]*schema.Schema{
							"value": {
								Type:     schema.TypeFloat,
								Required: true,
							},
						},
					},
				},
				"date_range_relative": {
					Type:          schema.TypeList,
					Optional:      true,
					MinItems:      0,
					MaxItems:      1,
					ConflictsWith: []string{"parameter.number", "parameter.date_range", "parameter.query", "parameter.date_range"},
					Elem: &schema.Resource{
						Schema: map[string]*schema.Schema{
							"value": {
								Type:     schema.TypeString,
								Required: true,
							},
						},
					},
				},
				"date_range": {
					Type:          schema.TypeList,
					Optional:      true,
					MinItems:      0,
					MaxItems:      1,
					ConflictsWith: []string{"parameter.text", "parameter.number", "parameter.query", "parameter.date_range_relative"},
					Elem: &schema.Resource{
						Schema: map[string]*schema.Schema{
							"value": {
								Type:     schema.TypeList,
								Required: true,
								MinItems: 1,
								MaxItems: 1,
								Elem: &schema.Resource{
									Schema: map[string]*schema.Schema{
										"start": {
											Type:     schema.TypeString,
											Required: true,
										},
										"end": {
											Type:     schema.TypeString,
											Required: true,
										},
									},
								},
							},
						},
					},
				},
				"query": {
					Type:          schema.TypeList,
					Optional:      true,
					MinItems:      0,
					MaxItems:      1,
					ConflictsWith: []string{"parameter.text", "parameter.number", "parameter.date_range", "parameter.date_range_relative"},
					Elem: &schema.Resource{
						Schema: map[string]*schema.Schema{
							"values": {
								Type:     schema.TypeList,
								Required: true,
								Elem: &schema.Schema{
									Type: schema.TypeString,
								},
							},
							"query_id": {
								Type:     schema.TypeString,
								Required: true,
							},
							"multi": {
								Optional: true,
								Type:     schema.TypeList,
								MinItems: 1,
								MaxItems: 1,
								Elem: &schema.Resource{
									Schema: map[string]*schema.Schema{
										"prefix": {
											Type:     schema.TypeString,
											Required: true,
										},
										"suffix": {
											Type:     schema.TypeString,
											Required: true,
										},
										"separator": {
											Type:     schema.TypeString,
											Required: true,
										},
									},
								},
							},
						},
					},
				},
			},
		},
	},
}

func expandQuery(d map[string]interface{}) *databricks.Query {
	var query databricks.Query
	if v, ok := d["data_source_id"]; ok {
		query.DataSourceID = v.(string)
	}
	if v, ok := d["name"]; ok {
		query.Name = v.(string)
	}
	if v, ok := d["description"]; ok {
		query.Description = v.(string)
	}
	if v, ok := d["query"]; ok {
		query.Query = v.(string)
	}
	if v, ok := d["parent"]; ok {
		query.Parent = v.(string)
	}
	if v, ok := d["run_as_role"]; ok {
		query.RunAsRole = v.(string)
	}
	if v, ok := d["tags"]; ok {
		val := v.(*schema.Set)
		for _, tag := range val.List() {
			query.Tags = append(query.Tags, tag.(string))
			sort.Strings(query.Tags)
		}
	}
	if v, ok := d["parameter"]; ok {
		if query.Options == nil {
			query.Options = &databricks.QueryOptions{}
		}
		query.Options.Parameters = []interface{}{}

		for _, paramIface := range v.([]interface{}) {
			param := paramIface.(map[string]interface{})

			apiParam := databricks.QueryParameter{}
			if name, ok := param["name"]; ok {
				apiParam.Name = name.(string)
			}
			if title, ok := param["title"]; ok {
				apiParam.Title = title.(string)
			}

			var iface interface{}

			if v, ok := param["text"]; ok && len(v.([]interface{})) == 1 {
				apiParam.Type = databricks.QueryParameterTypeNameText
				text := v.([]interface{})[0].(map[string]interface{})
				iface = &databricks.QueryParameterText{
					QueryParameter: apiParam,
					Value:          text["value"].(string),
				}
			} else if v, ok := param["number"]; ok && len(v.([]interface{})) == 1 {
				apiParam.Type = databricks.QueryParameterTypeNameNumber
				number := v.([]interface{})[0].(map[string]interface{})
				iface = &databricks.QueryParameterNumber{
					QueryParameter: apiParam,
					Value:          number["value"].(float64),
				}
			} else if v, ok := param["date_range_relative"]; ok && len(v.([]interface{})) == 1 {
				apiParam.Type = databricks.QueryParameterTypeNameDateRange
				dateRangeRel := v.([]interface{})[0].(map[string]interface{})
				iface = &databricks.QueryParameterDateRangeRelative{
					QueryParameter: apiParam,
					Value:          dateRangeRel["value"].(string),
				}
			} else if v, ok := param["date_range"]; ok && len(v.([]interface{})) == 1 {
				apiParam.Type = databricks.QueryParameterTypeNameDateRange
				dateRange := v.([]interface{})[0].(map[string]interface{})
				if v, ok := dateRange["value"]; ok && len(v.([]interface{})) == 1 {
					value := v.([]interface{})[0].(map[string]interface{})
					iface = &databricks.QueryParameterDateRange{
						QueryParameter: apiParam,
						Value: databricks.QueryParameterDateRangeValue{
							Start: value["start"].(string),
							End:   value["end"].(string),
						},
					}
				}
			} else if v, ok := param["query"]; ok && len(v.([]interface{})) == 1 {
				apiParam.Type = databricks.QueryParameterTypeNameQuery
				query := v.([]interface{})[0].(map[string]interface{})

				values := []string{}
				if v, ok := query["values"]; ok && len(v.([]interface{})) > 0 {
					vals := v.([]interface{})
					for _, val := range vals {
						values = append(values, val.(string))
					}
				}

				queryParamQuery := &databricks.QueryParameterQuery{
					QueryParameter: apiParam,
					QueryID:        query["query_id"].(string),
					Values:         values,
				}

				if v, ok := query["multi"]; ok && len(v.([]interface{})) == 1 {
					queryMulti := v.([]interface{})[0].(map[string]interface{})
					if len(queryMulti) > 0 {
						multi := &databricks.QueryParameterMultipleValuesOptions{}
						if v, ok := queryMulti["prefix"]; ok {
							multi.Prefix = v.(string)
						}
						if v, ok := queryMulti["suffix"]; ok {
							multi.Suffix = v.(string)
						}
						if v, ok := queryMulti["separator"]; ok {
							multi.Separator = v.(string)
						}
						queryParamQuery.Multi = multi
					}
				}

				iface = queryParamQuery
			}
			query.Options.Parameters = append(query.Options.Parameters, iface)
		}
	}
	return &query
}

func flattenQuery(query *databricks.Query) map[string]interface{} {
	m := make(map[string]interface{})
	if query.DataSourceID != "" {
		m["data_source_id"] = query.DataSourceID
	} else {
		m["data_source_id"] = nil
	}
	if query.Name != "" {
		m["name"] = query.Name
	} else {
		m["name"] = nil
	}
	if query.Description != "" {
		m["description"] = query.Description
	} else {
		m["description"] = nil
	}
	if query.Parent != "" {
		m["parent"] = query.Parent
	}
	if query.Query != "" {
		m["query"] = query.Query
	} else {
		m["query"] = nil
	}
	if query.RunAsRole != "" {
		m["run_as_role"] = query.RunAsRole
	} else {
		m["run_as_role"] = nil
	}

	var tags []interface{}
	for _, t := range query.Tags {
		tags = append(tags, string(t))
	}
	tagSet := schema.NewSet(stringHash, tags)
	m["tags"] = tagSet

	if query.Options != nil && len(query.Options.Parameters) > 0 {
		var parameters []interface{}

		for _, qp := range query.Options.Parameters {
			p := make(map[string]interface{})

			switch qpv := qp.(type) {
			case *databricks.QueryParameterText:
				p["name"] = qpv.Name
				p["title"] = qpv.Title
				p["text"] = []interface{}{
					map[string]interface{}{
						"value": qpv.Value,
					},
				}
			case *databricks.QueryParameterNumber:
				p["name"] = qpv.Name
				p["title"] = qpv.Title
				p["number"] = []interface{}{
					map[string]interface{}{
						"value": qpv.Value,
					},
				}
			case *databricks.QueryParameterDateRangeRelative:
				p["name"] = qpv.Name
				p["title"] = qpv.Title
				p["date_range_relative"] = []interface{}{
					map[string]interface{}{
						"value": qpv.Value,
					},
				}
			case *databricks.QueryParameterDateRange:
				p["name"] = qpv.Name
				p["title"] = qpv.Title
				p["date_range"] = []interface{}{
					map[string]interface{}{
						"value": []interface{}{
							map[string]interface{}{
								"start": qpv.Value.Start,
								"end":   qpv.Value.End,
							},
						},
					},
				}
			case *databricks.QueryParameterQuery:
				p["name"] = qpv.Name
				p["title"] = qpv.Title

				values := []interface{}{}
				for _, val := range qpv.Values {
					values = append(values, val)
				}

				query := map[string]interface{}{
					"values":   values,
					"query_id": qpv.QueryID,
				}

				multi := map[string]interface{}{}
				if qpv.Multi != nil {
					multi["suffix"] = qpv.Multi.Suffix
					multi["prefix"] = qpv.Multi.Suffix
					multi["separator"] = qpv.Multi.Separator
					query["multi"] = []interface{}{multi}
				}

				p["query"] = []interface{}{query}
			default:
				log.Fatalf("Don't know what to do for type: %#v", reflect.TypeOf(qpv).String())
			}

			parameters = append(parameters, p)
		}
		m["parameter"] = parameters
	} else {
		m["parameter"] = nil
	}

	return m
}

func queryResourceDataToMap(d *schema.ResourceData) map[string]interface{} {
	m := make(map[string]interface{})
	for field := range sqlQuerySchema {
		m[field] = d.Get(field)
	}
	return m
}

func resourceSqlQuery() *schema.Resource {
	return &schema.Resource{
		Schema: sqlQuerySchema,
		Create: resourceSqlQueryCreate,
		Read:   resourceSqlQueryRead,
		Update: resourceSqlQueryUpdate,
		Delete: resourceSqlQueryDelete,
		Importer: &schema.ResourceImporter{
			State: resourceSqlQueryImport,
		},
	}
}

func resourceSqlQueryCreate(d *schema.ResourceData, m interface{}) error {
	client := m.(databricks.API)
	queryMap := queryResourceDataToMap(d)
	query := expandQuery(queryMap)

	newQuery, err := client.CreateSqlQuery(context.Background(), query)
	if err != nil {
		return oops.Wrapf(err, "")
	}

	// New queries are created with a table visualization by default.
	// We don't manage that table in tf, so make a best effort to remove it immediately.
	for _, v := range newQuery.Visualizations {
		var vis databricks.Visualization
		if err := json.Unmarshal(v, &vis); err != nil {
			return oops.Wrapf(err, "")
		}

		// Swallow error here - this is a best-effort attempt.
		if err := client.DeleteSQLVisualization(context.Background(), vis.ID); err != nil {
			log.Printf("[WARN] Unable to delete automatically created visualization for query %s (%s)", newQuery.ID, vis.ID)
		}
	}

	newQuery.Visualizations = []json.RawMessage{}
	d.SetId(newQuery.ID)
	return nil
}

func resourceSqlQueryRead(d *schema.ResourceData, m interface{}) error {
	client := m.(databricks.API)
	out, err := client.GetSqlQuery(context.Background(), d.Id())
	if err != nil {
		if apiErr, ok := oops.Cause(err).(*databricks.APIError); ok {
			if strings.Contains(apiErr.Message, "does not exist") {
				d.SetId("")
				return nil
			}
		}
		return oops.Wrapf(err, "")
	}

	kv := flattenQuery(out)
	for k, v := range kv {
		if err := d.Set(k, v); err != nil {
			return oops.Wrapf(err, "%+v: %+v", k, v)
		}
	}
	return nil
}

func resourceSqlQueryUpdate(d *schema.ResourceData, m interface{}) error {
	client := m.(databricks.API)
	queryMap := queryResourceDataToMap(d)
	query := expandQuery(queryMap)

	if err := client.EditSqlQuery(context.Background(), d.Id(), query); err != nil {
		return oops.Wrapf(err, "")
	}
	return nil
}

func resourceSqlQueryDelete(d *schema.ResourceData, m interface{}) error {
	client := m.(databricks.API)
	if err := client.DeleteSqlQuery(context.Background(), d.Id()); err != nil {
		return oops.Wrapf(err, "")
	}
	return nil
}

func resourceSqlQueryImport(d *schema.ResourceData, m interface{}) ([]*schema.ResourceData, error) {
	if err := resourceSqlQueryRead(d, m); err != nil {
		return nil, err
	}
	return []*schema.ResourceData{d}, nil
}
