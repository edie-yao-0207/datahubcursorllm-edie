package databricks

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"samsaradev.io/infra/dataplatform/databricks"
)

func TestFlattenExpandDashboardSchedule(t *testing.T) {
	schedule := &databricks.SqlDashboardSchedule{
		Cron:         "H H * * H",
		Active:       true,
		JobId:        "job-id-123",
		DataSourceId: "data-source-id",
		Subscriptions: []databricks.SqlDashboardSubscription{
			{
				Id: "subscription-id-123",
				CreatedBy: databricks.SqlDashboardTarget{
					Id:    "created-by-id-123",
					Name:  "user-123",
					Email: "user.123@samsara.com",
				},
				TargetType: databricks.SqlDashboardTargetTypeUser,
				TargetId:   "target-id-123",
				Target: databricks.SqlDashboardTarget{
					Id:    "target-user-id-123",
					Name:  "target-123",
					Email: "target.123@samsara.com",
				},
			},
			{
				Id: "subscription-id-abc",
				CreatedBy: databricks.SqlDashboardTarget{
					Id:    "created-by-id-abc",
					Name:  "user-abc",
					Email: "user.abc@samsara.com",
				},
				TargetType: databricks.SqlDashboardTargetTypeUser,
				TargetId:   "target-id-abc",
				Target: databricks.SqlDashboardTarget{
					Id:    "target-user-id-abc",
					Name:  "target-abc",
					Email: "target.abc@samsara.com",
				},
			},
		},
	}

	flattened := flattenSchedule(schedule)
	expanded := expandSchedule(flattened)

	assert.Equal(t, expanded, schedule)
}
