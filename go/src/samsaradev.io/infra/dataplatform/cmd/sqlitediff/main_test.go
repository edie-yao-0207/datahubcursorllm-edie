package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMakeSqliteCommands(t *testing.T) {
	oldReportName := "driver_app_engagement_report"
	newReportName := "driver_app_engagement_report_temp"
	baseDir := "parth_testing"
	var startMs int64 = 1621659600000 // May 22, 00:00:00 CST
	var endMs int64 = 1621918800000   // May 25, 00:00:00 CST
	pkColumns := []string{"month", "org_id", "duration_ms", "interval_start", "driver_id"}
	nonPkColumns := []string{"num_app_opens"}
	query := makeSqliteCommands(baseDir, oldReportName, newReportName, startMs, endMs, pkColumns, nonPkColumns, "org_id=1003")
	expected := `
attach 'parth_testing/original/org_id=1003/db.sqlite' as db_orig;
attach 'parth_testing/migrated/org_id=1003/db.sqlite' as db_migrated;

.headers ON

SELECT month, org_id, duration_ms, interval_start, driver_id, a.num_app_opens AS num_app_opens_orig, b.num_app_opens AS num_app_opens_migrated
FROM db_orig.driver_app_engagement_report a
LEFT JOIN db_migrated.driver_app_engagement_report_temp b
  USING(month, org_id, duration_ms, interval_start, driver_id)
WHERE (((a.num_app_opens IS NULL AND b.num_app_opens IS NOT NULL) OR (a.num_app_opens IS NOT NULL AND b.num_app_opens IS NULL) OR a.num_app_opens != b.num_app_opens))
AND duration_ms = 3600000
AND interval_start >= 1621659600000
AND interval_start < 1621918800000

UNION

SELECT month, org_id, duration_ms, interval_start, driver_id, a.num_app_opens AS num_app_opens_orig, b.num_app_opens AS num_app_opens_migrated
FROM db_migrated.driver_app_engagement_report_temp b
LEFT JOIN db_orig.driver_app_engagement_report a
  USING(month, org_id, duration_ms, interval_start, driver_id)
WHERE (((a.num_app_opens IS NULL AND b.num_app_opens IS NOT NULL) OR (a.num_app_opens IS NOT NULL AND b.num_app_opens IS NULL) OR a.num_app_opens != b.num_app_opens))
AND duration_ms = 3600000
AND interval_start >= 1621659600000
AND interval_start < 1621918800000;

.headers OFF

select 'done';
`
	assert.Equal(t, strings.TrimSpace(expected), strings.TrimSpace(query))
}
