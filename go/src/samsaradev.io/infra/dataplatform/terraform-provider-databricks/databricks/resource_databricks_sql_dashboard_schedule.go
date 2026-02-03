package databricks

import (
	"context"
	"encoding/json"
	"sort"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/dataplatform/databricks"
)

var scheduleCompositeID = newCompositeId("dashboard_id", "schedule_id", "/")

var sqlDashboardScheduleSchema = map[string]*schema.Schema{
	"dashboard_id": {
		Type:     schema.TypeString,
		Required: true,
	},
	"schedule_id": {
		Type:     schema.TypeString,
		Computed: true,
	},
	"name": {
		// Not used in the api, but used as our string identifier.
		Type:     schema.TypeString,
		Required: true,
	},
	"cron": {
		Type:     schema.TypeString,
		Required: true,
	},
	"active": {
		Type:     schema.TypeBool,
		Required: true,
	},
	"job_id": {
		Type:     schema.TypeString,
		Computed: true,
	},
	"data_source_id": {
		Type:     schema.TypeString,
		Optional: true,
	},
	"subscriptions": {
		Type:     schema.TypeList,
		Optional: true,
		Elem: &schema.Resource{
			Schema: map[string]*schema.Schema{
				"id": {
					Type:     schema.TypeString,
					Computed: true,
				},
				"target_type": {
					Type:     schema.TypeString,
					Required: true,
				},
				"target_name": {
					Type:     schema.TypeString,
					Required: true,
				},
				"target_id": {
					Type:     schema.TypeString,
					Required: true,
				},
				"created_by": {
					Computed: true,
					Type:     schema.TypeList,
					MinItems: 1,
					MaxItems: 1,
					Elem: &schema.Resource{
						Schema: map[string]*schema.Schema{
							"id": {
								Type:     schema.TypeString,
								Required: true,
							},
							"name": {
								Type:     schema.TypeString,
								Required: true,
							},
							"email": {
								Type:     schema.TypeString,
								Required: true,
							},
						},
					},
				},
				"target": {
					Computed: true,
					Type:     schema.TypeList,
					MinItems: 1,
					MaxItems: 1,
					Elem: &schema.Resource{
						Schema: map[string]*schema.Schema{
							"id": {
								Type:     schema.TypeString,
								Required: true,
							},
							"name": {
								Type:     schema.TypeString,
								Required: true,
							},
							"email": {
								Type:     schema.TypeString,
								Required: true,
							},
						},
					},
				},
			},
		},
	},
}

func expandSchedule(d map[string]interface{}) *databricks.SqlDashboardSchedule {
	var schedule databricks.SqlDashboardSchedule

	if v, ok := d["dashboard_id"]; ok {
		schedule.DashboardId = v.(string)
	}
	if v, ok := d["cron"]; ok {
		schedule.Cron = v.(string)
	}
	if v, ok := d["active"]; ok {
		schedule.Active = v.(bool)
	}
	if v, ok := d["job_id"]; ok {
		schedule.JobId = v.(string)
	}
	if v, ok := d["data_source_id"]; ok {
		schedule.DataSourceId = v.(string)
	}

	if v, ok := d["subscriptions"]; ok && len(v.([]interface{})) > 0 {
		subscriptions := v.([]interface{})
		for _, subscription := range subscriptions {
			scheduleSub := databricks.SqlDashboardSubscription{}

			sub := subscription.(map[string]interface{})

			if v, ok := sub["id"]; ok {
				scheduleSub.Id = v.(string)
			}
			if v, ok := sub["target_type"]; ok {
				scheduleSub.TargetType = databricks.SqlDashboardTargetType(v.(string))
			}
			if v, ok := sub["target_id"]; ok {
				scheduleSub.TargetId = json.Number(v.(string))
			}

			if v, ok := sub["created_by"]; ok && len(v.([]interface{})) == 1 {
				scheduleSub.CreatedBy = databricks.SqlDashboardTarget{}
				createdBy := v.([]interface{})[0].(map[string]interface{})

				if v, ok := createdBy["id"]; ok {
					scheduleSub.CreatedBy.Id = json.Number(v.(string))
				}
				if v, ok := createdBy["name"]; ok {
					scheduleSub.CreatedBy.Name = v.(string)
				}
				if v, ok := createdBy["email"]; ok {
					scheduleSub.CreatedBy.Email = v.(string)
				}
			}

			if v, ok := sub["target"]; ok && len(v.([]interface{})) == 1 {
				scheduleSub.Target = databricks.SqlDashboardTarget{}
				target := v.([]interface{})[0].(map[string]interface{})

				if v, ok := target["id"]; ok {
					scheduleSub.Target.Id = json.Number(v.(string))
				}
				if v, ok := target["name"]; ok {
					scheduleSub.Target.Name = v.(string)
				}
				if v, ok := target["email"]; ok {
					scheduleSub.Target.Email = v.(string)
				}
			}

			schedule.Subscriptions = append(schedule.Subscriptions, scheduleSub)
		}
	}

	return &schedule
}

func flattenSchedule(schedule *databricks.SqlDashboardSchedule) map[string]interface{} {
	m := make(map[string]interface{})
	if schedule.DashboardId != "" {
		m["dashboard_id"] = schedule.DashboardId
	}
	if schedule.Id != "" {
		m["schedule_id"] = schedule.Id
	}
	if schedule.Cron != "" {
		m["cron"] = schedule.Cron
	}
	m["active"] = schedule.Active
	if schedule.JobId != "" {
		m["job_id"] = schedule.JobId
	}
	if schedule.DataSourceId != "" {
		m["data_source_id"] = schedule.DataSourceId
	}

	scheduleSubscriptions := make([]interface{}, 0, len(schedule.Subscriptions))
	for _, subscription := range schedule.Subscriptions {
		sub := make(map[string]interface{})
		sub["id"] = subscription.Id
		sub["target_type"] = string(subscription.TargetType)
		sub["target_name"] = subscription.Target.Email
		sub["target_id"] = string(subscription.TargetId)

		createdBy := make(map[string]interface{})
		createdBy["id"] = string(subscription.CreatedBy.Id)
		createdBy["name"] = subscription.CreatedBy.Name
		createdBy["email"] = subscription.CreatedBy.Email
		sub["created_by"] = []interface{}{createdBy}

		target := make(map[string]interface{})
		target["id"] = string(subscription.Target.Id)
		target["name"] = subscription.Target.Name
		target["email"] = subscription.Target.Email
		sub["target"] = []interface{}{target}

		scheduleSubscriptions = append(scheduleSubscriptions, sub)
	}
	sort.Slice(scheduleSubscriptions, func(i, j int) bool {
		a := scheduleSubscriptions[i].(map[string]interface{})["target_name"].(string)
		b := scheduleSubscriptions[j].(map[string]interface{})["target_name"].(string)

		return a < b
	})
	m["subscriptions"] = scheduleSubscriptions

	return m
}

func scheduleResourceDataToMap(d *schema.ResourceData) map[string]interface{} {
	m := make(map[string]interface{})
	for field := range sqlDashboardScheduleSchema {
		m[field] = d.Get(field)
	}
	return m
}

func resourceSqlDashboardSchedule() *schema.Resource {
	return &schema.Resource{
		Schema: sqlDashboardScheduleSchema,
		Create: resourceSqlDashboardScheduleCreate,
		Read:   resourceSqlDashboardScheduleRead,
		Update: resourceSqlDashboardScheduleUpdate,
		Delete: resourceSqlDashboardScheduleDelete,
		Importer: &schema.ResourceImporter{
			State: resourceSqlDashboardScheduleImport,
		},
	}
}

func resourceSqlDashboardScheduleCreate(d *schema.ResourceData, m interface{}) error {
	client := m.(databricks.API)
	scheduleMap := scheduleResourceDataToMap(d)
	schedule := expandSchedule(scheduleMap)

	newSchedule, err := client.CreateSqlDashboardSchedule(context.Background(), &databricks.CreateSqlDashboardScheduleInput{
		DashboardId: schedule.DashboardId,
		Schedule:    schedule,
	})
	if err != nil {
		return oops.Wrapf(err, "")
	}

	// Set the schedule_id so that we can package the composite id.
	d.Set("schedule_id", newSchedule.Id)
	scheduleCompositeID.Pack(d)
	return nil
}

func resourceSqlDashboardScheduleRead(d *schema.ResourceData, m interface{}) error {
	dashId, scheduleId, err := scheduleCompositeID.Unpack(d)
	if err != nil {
		return oops.Wrapf(err, "")
	}

	client := m.(databricks.API)
	out, err := client.GetSqlDashboardSchedule(context.Background(), &databricks.GetSqlDashboardScheduleInput{
		DashboardId: dashId,
		ScheduleId:  scheduleId,
	})
	if err != nil {
		if apiErr, ok := oops.Cause(err).(*databricks.APIError); ok {
			if strings.Contains(apiErr.Message, "does not exist") {
				d.SetId("")
				return nil
			}
		}
		return oops.Wrapf(err, "")
	}

	kv := flattenSchedule(out)
	for k, v := range kv {
		if err := d.Set(k, v); err != nil {
			return oops.Wrapf(err, "%+v: %+v", k, v)
		}
	}
	return nil
}

func resourceSqlDashboardScheduleUpdate(d *schema.ResourceData, m interface{}) error {
	dashboardId, scheduleId, err := scheduleCompositeID.Unpack(d)
	if err != nil {
		return oops.Wrapf(err, "")
	}

	client := m.(databricks.API)
	scheduleMap := scheduleResourceDataToMap(d)
	schedule := expandSchedule(scheduleMap)
	schedule.Id = scheduleId

	if _, err := client.EditSqlDashboardSchedule(context.Background(), &databricks.EditSqlDashboardScheduleInput{
		DashboardId: dashboardId,
		Schedule:    schedule,
	}); err != nil {
		return oops.Wrapf(err, "")
	}
	return nil
}

func resourceSqlDashboardScheduleDelete(d *schema.ResourceData, m interface{}) error {
	dashboardId, scheduleId, err := scheduleCompositeID.Unpack(d)
	if err != nil {
		return oops.Wrapf(err, "")
	}

	client := m.(databricks.API)
	if err := client.DeleteSqlDashboardSchedule(context.Background(), &databricks.DeleteSqlDashboardScheduleInput{
		DashboardId: dashboardId,
		ScheduleId:  scheduleId,
	}); err != nil {
		return oops.Wrapf(err, "")
	}
	return nil
}

func resourceSqlDashboardScheduleImport(d *schema.ResourceData, m interface{}) ([]*schema.ResourceData, error) {
	if err := resourceSqlDashboardRead(d, m); err != nil {
		return nil, err
	}
	return []*schema.ResourceData{d}, nil
}
