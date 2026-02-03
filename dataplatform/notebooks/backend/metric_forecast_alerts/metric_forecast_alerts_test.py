import types

import pytest

# COMMAND ----------

# MAGIC %run "backend/backend/metric_forecast_alerts/metric_forecast_alerts"

# COMMAND ----------

sample_historical_data_query = """
select
  date as ds,
  count(value.int_value) as y
from
  kinesisstats.osdenginestate
where
  date >= '2020-05-19'
  and date <= '2020-07-19'
group by
  date
"""
sample_model = MetricForecastAlert(
    sample_historical_data_query,
    ["dev-data-science"],
    "Test Alert",
    weekly_seasonality=True,
)

# COMMAND ----------

sample_current_data_query = """
select
  date as ds,
  count(value.int_value) as y
from
  kinesisstats.osdenginestate
where
  date >= '2020-07-20' and date <= '2020-08-31'
group by
  date
"""

sample_current_data_as_pd_df = spark.sql(sample_current_data_query).toPandas()

# COMMAND ----------


def get_sample_params(
    historical_query_or_df=sample_historical_data_query,
    alert_channels=["dev-data-science"],
    alert_name="Test Alert",
    daily_seasonality=False,
    weekly_seasonality=False,
    confidence=0.95,
):
    return {
        "historical_query_or_df": historical_query_or_df,
        "alert_channels": alert_channels,
        "alert_name": alert_name,
        "daily_seasonality": daily_seasonality,
        "weekly_seasonality": weekly_seasonality,
        "confidence": confidence,
    }


def get_sample_run_and_alert_params(
    current_df_or_query=sample_current_data_query,
    alert_threshold=1,
    alert_type="prediction_threshold",
    model_threshold=0.1,
):
    return {
        "current_df_or_query": current_df_or_query,
        "alert_threshold": alert_threshold,
        "alert_type": alert_type,
        "model_threshold": model_threshold,
    }


# COMMAND ----------

# Valid Params Tests


def test_valid_init_params():
    testcases = [
        {
            "description": "Test None historical data",
            "params": get_sample_params(historical_query_or_df=None),
        },
        {
            "description": "Test None alert channels",
            "params": get_sample_params(alert_channels=None),
        },
        {
            "description": "Test None alert name",
            "params": get_sample_params(alert_name=None),
        },
        {
            "description": "Test None daily seasonality",
            "params": get_sample_params(daily_seasonality=None),
        },
        {
            "description": "Test None weekly seasonality",
            "params": get_sample_params(weekly_seasonality=None),
        },
        {
            "description": "Test None confidence",
            "params": get_sample_params(confidence=None),
        },
        {
            "description": "Test invalid type historical data",
            "params": get_sample_params(historical_query_or_df=1),
        },
        {
            "description": "Test invalid type alert channels",
            "params": get_sample_params(alert_channels=1),
        },
        {
            "description": "Test invalid type alert name",
            "params": get_sample_params(alert_name=1),
        },
        {
            "description": "Test invalid type daily seasonality",
            "params": get_sample_params(daily_seasonality="string"),
        },
        {
            "description": "Test invalid type weekly seasonality",
            "params": get_sample_params(weekly_seasonality="string"),
        },
        {
            "description": "Test invalid type confidence",
            "params": get_sample_params(confidence="string"),
        },
        {
            "description": "Test incorrect historical dataframe columns",
            "params": get_sample_params(
                historical_query_or_df="""
        select 1 as invalid from clouddb.organizations limit 1
      """
            ),
        },
    ]
    for tc in testcases:
        params = tc["params"]
        with pytest.raises(Exception):
            MetricForecastAlert(
                params["historical_query_or_df"],
                params["alert_channels"],
                params["alert_name"],
                daily_seasonality=params["daily_seasonality"],
                weekly_seasonality=params["weekly_seasonality"],
                confidence=params["confidence"],
            )


def generic_forecast_param_fn(forecast_fn, invalid_timestamps_df):
    testcases = [
        {"description": "Test None timestamps df", "timestamps_df": None},
        {"description": "Test invalid type timestamps df", "timestamps_df": 1},
        {
            "description": "Test incorrect columns timestamps df",
            "timestamps_df": invalid_timestamps_df,
        },
    ]
    for tc in testcases:
        with pytest.raises(Exception):
            forecast_fn(tc["timestamps_df"])


def test_forecast_fns_params():
    generic_forecast_param_fn(
        sample_model.forecast, pd.DataFrame([1], columns=["invalid"])
    )
    generic_forecast_param_fn(
        sample_model.show_forecast,
        spark.createDataFrame(pd.DataFrame([1], columns=["invalid"])),
    )
    generic_forecast_param_fn(
        sample_model.show_forecast_with_current_data,
        spark.createDataFrame(pd.DataFrame([1], columns=["invalid"])),
    )


def test_run_and_alert_params():
    testcases = [
        {
            "description": "Test None current_df_or_query",
            "params": get_sample_run_and_alert_params(current_df_or_query=None),
        },
        {
            "description": "Test None alert threshold",
            "params": get_sample_run_and_alert_params(alert_threshold=None),
        },
        {
            "description": "Test None alert type",
            "params": get_sample_run_and_alert_params(alert_type=None),
        },
        {
            "description": "Test None model threshold",
            "params": get_sample_run_and_alert_params(model_threshold=None),
        },
        {
            "description": "Test invalid type current_df_or_query",
            "params": get_sample_run_and_alert_params(current_df_or_query=1),
        },
        {
            "description": "Test invalid type alert threshold",
            "params": get_sample_run_and_alert_params(
                alert_threshold=0.1
            ),  # shouldn't be a float
        },
        {
            "description": "Test invalid type alert type",
            "params": get_sample_run_and_alert_params(alert_type=1),
        },
        {
            "description": "Test invalid type model threshold",
            "params": get_sample_run_and_alert_params(model_threshold="string"),
        },
        {
            "description": "Test invalid value for alert type",
            "params": get_sample_run_and_alert_params(alert_type="invalid alert type"),
        },
    ]
    for tc in testcases:
        with pytest.raises(Exception):
            sample_model.run_and_alert(
                params["current_df_or_query"],
                params["alert_threshold"],
                params["alert_type"],
                params["model_threshold"],
            )


# COMMAND ----------

# Test we can identify deviations correctly
# Test we can identify deviations correctly
def test_data_deviations():
    testcases = [
        {
            "description": "Test identify engine state deviation w model threshold",
            "alert_type": "prediction_threshold",
            "current_df_or_query": sample_current_data_as_pd_df,
            "expected_num_outside_threshold": 5,
            "filter_holidays": False,
        },
        {
            "description": "Test identify engine state deviation w model confidence bounds",
            "alert_type": "confidence_bounds",
            "current_df_or_query": pd.DataFrame(
                list(zip([datetime.strptime("2020-08-05", "%Y-%m-%d")], [-1])),
                columns=["ds", "y"],
            ),
            "expected_num_outside_threshold": 1,
            "filter_holidays": False,
        },
        {
            "description": "Test no alert on expected results",
            "alert_type": "confidence_bounds",
            "current_df_or_query": pd.DataFrame(
                list(
                    zip([datetime.strptime("2020-08-05", "%Y-%m-%d")], [1.5 * 10**7])
                ),
                columns=["ds", "y"],
            ),
            "expected_num_outside_threshold": 0,
            "filter_holidays": False,
        },
        {
            "description": "Test correctly filter holidays",
            "alert_type": "prediction_threshold",
            "current_df_or_query": pd.DataFrame(
                list(
                    zip(
                        [
                            datetime.strptime("2020-09-07", "%Y-%m-%d"),
                            datetime.strptime("2020-10-08", "%Y-%m-%d"),
                        ],
                        [-1, -1],
                    )
                ),
                columns=["ds", "y"],  # labor day
            ),
            "expected_num_outside_threshold": 1,
            "filter_holidays": True,
        },
    ]
    for tc in testcases:
        num_outside_threshold = 0
        sample_model.filter_holidays = tc["filter_holidays"]
        alert_df = sample_model._get_alert_df(tc["current_df_or_query"])
        if tc["alert_type"] == "prediction_threshold":
            num_outside_threshold = sample_model._num_outside_given_threshold(alert_df)
        if tc["alert_type"] == "confidence_bounds":
            num_outside_threshold = sample_model._num_outside_model_bounds(alert_df)
        assert num_outside_threshold == tc["expected_num_outside_threshold"]


# COMMAND ----------


fs = list(
    globals().values()
)  # Do this to hack around the "dict size changed during iteration"
test_fns = []
for f in fs:
    if type(f) != types.FunctionType:
        continue
    if f.__name__[0:4] != "test":
        continue
    test_fns.append(f)
for test_fn in test_fns:
    test_fn()
