INSERT INTO fuel_maintenance.historical_ifta_report_delta
PARTITION (date)
SELECT * FROM fuel_maintenance.historical_ifta_report WHERE org_id in (23474, 23468, 23473, 23484, 23494, 23471, 23476, 23467, 23496, 23482) and date >= "2019-10-01" and date <= "2021-03-02";
