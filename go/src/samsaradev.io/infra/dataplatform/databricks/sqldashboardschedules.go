package databricks

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/samsarahq/go/oops"
)

const dashboardScheduleAPIRoot = "api/2.0/preview/sql/dashboards/"

// SQL Dashboard Schedule API is currently undocumented :(
// Databricks support sent me an unofficial document containing some pointers
// for how to get started - but mentioned it is subject to change.
type SqlDashboardSchedule struct {
	Id            string                     `json:"id"`
	DashboardId   string                     `json:"dashboard_id"`
	Cron          string                     `json:"cron"`
	Active        bool                       `json:"active"`
	JobId         string                     `json:"job_id"`
	DataSourceId  string                     `json:"data_source_id"`
	Subscriptions []SqlDashboardSubscription `json:"subscriptions"`
}

type SqlDashboardTargetType string

const (
	SqlDashboardTargetTypeUser SqlDashboardTargetType = "user"
)

type SqlDashboardSubscription struct {
	Id         string                 `json:"id"`
	CreatedBy  SqlDashboardTarget     `json:"created_by"`
	TargetType SqlDashboardTargetType `json:"target_type"`
	TargetId   json.Number            `json:"target_id"`
	Target     SqlDashboardTarget     `json:"target"`
}

type SqlDashboardTarget struct {
	Id    json.Number `json:"id"`
	Name  string      `json:"name"`
	Email string      `json:"email"`
}

type CreateSqlDashboardScheduleInput struct {
	DashboardId string
	Schedule    *SqlDashboardSchedule
}

type GetSqlDashboardScheduleInput struct {
	DashboardId string
	ScheduleId  string
}

type EditSqlDashboardScheduleInput struct {
	DashboardId string
	Schedule    *SqlDashboardSchedule
}

type DeleteSqlDashboardScheduleInput struct {
	DashboardId string
	ScheduleId  string
}

type SQLDashboardScheduleAPI interface {
	CreateSqlDashboardSchedule(context.Context, *CreateSqlDashboardScheduleInput) (*SqlDashboardSchedule, error)
	GetSqlDashboardSchedule(context.Context, *GetSqlDashboardScheduleInput) (*SqlDashboardSchedule, error)
	EditSqlDashboardSchedule(context.Context, *EditSqlDashboardScheduleInput) (*SqlDashboardSchedule, error)
	DeleteSqlDashboardSchedule(context.Context, *DeleteSqlDashboardScheduleInput) error
}

func (c *Client) CreateSqlDashboardSchedule(ctx context.Context, input *CreateSqlDashboardScheduleInput) (*SqlDashboardSchedule, error) {
	var schedule SqlDashboardSchedule
	if err := c.do(ctx, http.MethodPost, dashboardScheduleAPIRoot+input.DashboardId+"/refresh_schedules", input.Schedule, &schedule); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &schedule, nil
}

func (c *Client) GetSqlDashboardSchedule(ctx context.Context, input *GetSqlDashboardScheduleInput) (*SqlDashboardSchedule, error) {
	var output []*SqlDashboardSchedule

	if err := c.do(ctx, http.MethodGet, dashboardScheduleAPIRoot+input.DashboardId+"/refresh_schedules", struct{}{}, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}

	for _, schedule := range output {
		if schedule != nil && schedule.Id == input.ScheduleId {
			return schedule, nil
		}
	}

	// Return not found.
	return nil, oops.Wrapf(HTTPStatusError{
		StatusCode: http.StatusNotFound,
		Status:     "NOT_FOUND",
		Body:       fmt.Sprintf("Cannot find schedule %s attached to dashboard %s", input.ScheduleId, input.DashboardId),
	}, "")
}

func (c *Client) EditSqlDashboardSchedule(ctx context.Context, input *EditSqlDashboardScheduleInput) (*SqlDashboardSchedule, error) {
	resp := &SqlDashboardSchedule{}

	if err := c.do(ctx, http.MethodPut, dashboardScheduleAPIRoot+input.DashboardId+"/refresh_schedules/"+input.Schedule.Id, input.Schedule, resp); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return resp, nil
}

func (c *Client) DeleteSqlDashboardSchedule(ctx context.Context, input *DeleteSqlDashboardScheduleInput) error {
	// Trash arguments required by c.do() but we do not care about.
	type deleteSqlDashboardOutput struct{}
	resp := &deleteSqlDashboardOutput{}

	if err := c.do(ctx, http.MethodDelete, dashboardScheduleAPIRoot+input.DashboardId+"/refresh_schedules/"+input.ScheduleId, input, resp); err != nil {
		return oops.Wrapf(err, "")
	}
	return nil
}
