import pytest
from datamodel.ops.monitors import CheckType, TableMonitor, get_last_update_age


def test_asset_without_timestamp_column():
    monitor = TableMonitor(
        table="dataengineering.firmware_events",
        check=CheckType.DATA_FRESHNESS_TABLE_MOST_RECENT_EVENT,
        freshness_slo_hours=1,
        date_partition_column="date",
    )
    spark = None  # we are not testing Spark i/o, just that the function exits early
    context = None  # we are not testing Spark i/o, just that the function exits early

    with pytest.raises(Exception) as err_msg:
        _ = get_last_update_age(context, spark, monitor)

    assert str(err_msg.value) == "Event based monitors must have a timestamp column"
