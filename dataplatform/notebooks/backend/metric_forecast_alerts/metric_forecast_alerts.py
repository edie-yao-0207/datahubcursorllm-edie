# Databricks notebook source
# MAGIC %run backend/backend/databricks_data_alerts/alerting_system

# COMMAND ----------

# MAGIC %run /backend/dataplatform/boto3_helpers

# COMMAND ----------

# Following install commands were commented out as they are no longer
# supported in UC clusters. Please ensure that this notebook is run
# in a cluster with the required libraries installed.
# dbutils.library.installPyPI("fbprophet")
# dbutils.library.installPyPI("prophet")
# dbutils.library.installPyPI("plotly")

# COMMAND ----------

from datetime import datetime
import logging

import boto3
import pandas as pd  #
import pyspark.sql.functions as F

from fbprophet import Prophet
import matplotlib
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
import matplotlib.pyplot as plt
import seaborn as sns

# TODO: Ideally we would use Koalas, but Koalas does not support Prophet. If that changes, we should modify this to support Koalas.


# import warnings
# warnings.filterwarnings("ignore")
logging.getLogger("py4j").setLevel(logging.ERROR)
matplotlib.use("Agg")

# COMMAND ----------

# HACK to save plots without displaying them. Databricks only doesn't display them in a udf
@udf
def save_plot(metric_data, s3_path, prediction_threshold):
    metric_data = sorted(metric_data, key=lambda x: x.timestamp)
    timestamps = [md.timestamp for md in metric_data]
    actual_y = [md.actual_y for md in metric_data]
    pred_y = [md.predicted_y for md in metric_data]
    lower_thresh = [
        md.predicted_y - prediction_threshold * md.predicted_y for md in metric_data
    ]
    upper_thresh = [
        md.predicted_y + prediction_threshold * md.predicted_y for md in metric_data
    ]
    plt.plot(timestamps, actual_y, c="red")
    plt.plot(timestamps, pred_y, c="skyblue")
    plt.plot(timestamps, lower_thresh, c="black", linestyle="dashed")
    plt.plot(timestamps, upper_thresh, c="black", linestyle="dashed")
    plt.legend(["Actual", "Predicted", "Thresholds"])
    dbfs_path = s3_path.replace("s3://", "/dbfs/mnt/")
    figure = plt.gcf()
    figure.set_size_inches(14, 10)
    plt.savefig(f"{dbfs_path}/plot.png", bbox_inches="tight", dpi=100)
    plt.close()
    return f"{dbfs_path}/plot.png"


# COMMAND ----------


class MetricForecastAlert:
    def _validate_arg_exists_and_type(self, arg, arg_types, arg_name: str):
        if arg is None:
            raise Exception(f"{arg_name} cannot be null!")
        if isinstance(arg_types, type):
            if not isinstance(arg, arg_types):
                raise Exception(
                    f"{arg_name} has an invalid type. Type must be {arg_types}"
                )
            return
        has_valid_arg_type = any([isinstance(arg, arg_type) for arg_type in arg_types])
        if not has_valid_arg_type:
            raise Exception(
                f"{arg_name} has an invalid type. Type must be one of {arg_types}"
            )

    def _validate_init_args(
        self,
        historical_query_or_df: Union[str, pyspark.sql.dataframe.DataFrame],
        alert_channels: List[str],
        alert_name: str,
        daily_seasonality: Union[bool, int],
        weekly_seasonality: Union[bool, int],
        confidence: float,
        filter_holidays: bool,
    ):
        self._validate_arg_exists_and_type(
            historical_query_or_df,
            [str, pyspark.sql.dataframe.DataFrame],
            "Historical Data/Query",
        )
        self._validate_arg_exists_and_type(alert_channels, list, "Alert Channels")
        self._validate_arg_exists_and_type(alert_name, str, "Alert Name")
        self._validate_arg_exists_and_type(
            daily_seasonality, [bool, int], "Daily Seasonality"
        )
        self._validate_arg_exists_and_type(
            weekly_seasonality, [bool, int], "Weekly Seasonality"
        )
        self._validate_arg_exists_and_type(confidence, float, "Confidence")
        self._validate_arg_exists_and_type(filter_holidays, bool, "Filter Holidays")

    def _validate_columns(
        self,
        df: Union[pyspark.sql.dataframe.DataFrame, pd.DataFrame],
        cols_to_validate,
        custom_err=None,
    ):
        columns = df.columns
        if isinstance(df, pd.DataFrame):
            columns = columns.values.tolist()

        if columns.sort() != cols_to_validate.sort():
            err = f"Dataframe must have these columns: {cols_to_validate}"
            if custom_err:
                err = custom_err
            raise Exception(err)

    def _generate_time_data(
        self, query_or_df: Union[str, pyspark.sql.dataframe.DataFrame]
    ) -> pd.DataFrame:
        time_data = query_or_df
        if isinstance(query_or_df, str):
            time_data = spark.sql(query_or_df)
        self._validate_columns(
            time_data,
            ["ds", "y"],
            custom_err="The resulting dataframe's columns must be named ds and y. ds is the timestamp column and y is the value column. Please modify your dataframe/query to match this format.",
        )
        return time_data.toPandas()

    def __init__(
        self,
        historical_query_or_df: Union[str, pyspark.sql.dataframe.DataFrame],
        alert_channels: List[str],
        alert_name: str,
        daily_seasonality: Union[bool, int] = False,
        weekly_seasonality: Union[bool, int] = False,
        confidence=0.95,
        filter_holidays: bool = False,
    ):
        """
        Create an alerter object to train on historical data and alert on current data. The model will be trained on instantation of the object.

        - historical_query_or_df:  The historical data to train the model on. Can be passed in as a pyspark DataFrame or a query string. The dataframe (or resulting dataframe from the query) must have only two columns titled ds and y. The ds column should be the timestamp column and the y column should be the values.
        - alert_channels: A list of Slack channels to send the alert to.
        - alert_name: A name to identify this alert
        - daily_seasonality: Whether the data is cyclical on a daily basis
        - weekly_seasonality: Whether the data is cyclical on a weekly basis
        - Note on the seasonality parameters. You can pass in a number instead which will help the model react to quick changes. If your data has quick changes (such as drastic dips on the weekend) it might help to adjust this number. The higher the number, the better Prophet can handle drastic changes.
        - confidence: The confidence intervals of the model. The larger this number is, the more data points have to deviate to be considered out of the prediction bounds of the model.
        - These parameters get passed into the instantiation of the Prophet model and are largely unchanged. For everything else, we use Prophet’s defaults.
        """
        self._validate_init_args(
            historical_query_or_df,
            alert_channels,
            alert_name,
            daily_seasonality,
            weekly_seasonality,
            confidence,
            filter_holidays,
        )
        self.historical_data = self._generate_time_data(historical_query_or_df)
        self.model = Prophet(
            interval_width=confidence,
            daily_seasonality=daily_seasonality,
            weekly_seasonality=weekly_seasonality,
        )
        self.alert_name = alert_name
        self.channels = alert_channels
        self.model.add_country_holidays(country_name="US")
        self.model.fit(self.historical_data)
        self.holidays = (
            spark.table("definitions.445_calendar")
            .filter(F.col("holiday"))
            .select("date")
            .rdd.flatMap(lambda x: x)
            .collect()
        )
        self.filter_holidays = filter_holidays

    def check_not_holiday(self, ds):
        ds_list = ds.tolist()
        not_holidays = []
        for date in ds_list:
            is_not_holiday = True
            for holiday in self.holidays:
                ds_to_check = date
                if isinstance(date, pd.Timestamp):
                    ds_to_check = date.to_pydatetime().date()
                if (holiday - ds_to_check).days == 0:
                    is_not_holiday = False
            not_holidays.append(is_not_holiday)
        return pd.Series(not_holidays)

    def _get_time_df_with_historical(self, timestamps_df: pd.DataFrame) -> pd.DataFrame:
        historical_time_series = self.historical_data["ds"]
        timestamps_with_historical = historical_time_series.append(timestamps_df["ds"])
        unique_timestamps_with_historical = timestamps_with_historical.unique()
        return pd.Series(unique_timestamps_with_historical).to_frame(name="ds")

    def forecast(
        self, timestamps_df: pd.DataFrame, include_history: bool = True
    ) -> pd.DataFrame:
        """
        Return a dataframe containing the model predictions for a set of timestamps.

        timestamps_df: A Pandas DataFrame with one column, ds, that contains the timestamps that the model should evaluate.
        include_history: If this is true, the returned dataframe will contain the model predictions for the historical data that it was trained on as well as the predictions for the dates in timestamps_df.
        """
        self._validate_arg_exists_and_type(
            timestamps_df, pd.DataFrame, "Timestamps Dataframe"
        )
        self._validate_columns(timestamps_df, ["ds"])
        self._validate_arg_exists_and_type(include_history, bool, "Include History")
        predict_times = timestamps_df
        if include_history:
            predict_times = self._get_time_df_with_historical(timestamps_df)
        return self.model.predict(predict_times)

    def show_forecast(
        self,
        timestamps_sdf: pyspark.sql.dataframe.DataFrame,
        include_history: bool = True,
    ):
        """
        Plot the the model predictions for a set of timestamps.

        timestamps_df: A PySpark DataFrame with one column, ds, that contains the timestamps that the model should evaluate.
        include_history: If this is true, the plot will display the model predictions for the historical data that it was trained on as well as the predictions for the dates in timestamps_df.
        """
        self._validate_arg_exists_and_type(
            timestamps_sdf, pyspark.sql.dataframe.DataFrame, "Timestamps Dataframe"
        )
        self._validate_arg_exists_and_type(include_history, bool, "Include History")
        timestamps_df = timestamps_sdf.toPandas()
        forecast_df = self.forecast(timestamps_df, include_history)
        predict_fig = self.model.plot(forecast_df, xlabel="time", ylabel="metric")
        display(predict_fig)

    def show_forecast_with_current_data(
        self,
        current_df_or_query: Union[str, pyspark.sql.dataframe.DataFrame],
        include_history: bool = True,
    ):
        """
        Plot the current data alongside with the model's forecasts.

        current_df_or_query: The data to plot.  Can be passed in as a pyspark DataFrame or a query string. The dataframe (or resulting dataframe from the query) must have only two columns titled ds and y. The ds column should be the timestamp column and the y column should be the values.
        inlcude_history: If this is true, the plot will display the model predictions for the historical data it was trained on as well.
        """
        self._validate_arg_exists_and_type(
            current_data, [str, pyspark.sql.dataframe.DataFrame], "Current Data"
        )
        current_data_df = self._generate_time_data(current_df_or_query)
        current_timestamps_df = current_data_df["ds"].to_frame(name="ds")
        forecast_df = self.forecast(
            current_timestamps_df, include_history=include_history
        )
        current_data_df.set_index("ds", inplace=True)
        current_data_df.index = pd.to_datetime(current_data_df.index)
        self.model.plot(forecast_df, xlabel="time", ylabel="metric")
        sns.lineplot(current_data_df.index, current_data_df["y"], c="r")
        legend = ["prediction", "actual"]
        legend_elements = [
            Line2D([0], [0], color="steelblue", lw="2", label="Prediction"),
            Line2D([0], [0], color="r", lw="2", label="Actual"),
            Patch(facecolor="#CCE2EF", label="Confidence Intervals"),
        ]
        if include_history:
            legend = ["historical"] + legend
            legend_elements = [
                Line2D(
                    [0],
                    [0],
                    marker="o",
                    color="w",
                    markerfacecolor="black",
                    label="Historical",
                )
            ] + legend_elements
        plt.legend(handles=legend_elements)
        display()

    def _get_forecast_with_current_data(
        self, current_data: pd.DataFrame
    ) -> pd.DataFrame:
        self._validate_arg_exists_and_type(current_data, pd.DataFrame, "Current Data")
        current_data_timestamps = current_data["ds"]
        forecast_df = self.forecast(
            current_data_timestamps.to_frame(name="ds"), include_history=True
        )
        forecast_df = forecast_df.set_index("ds")
        current_data = current_data.set_index("ds")
        compare_df = forecast_df.join(current_data)
        compare_df["ds"] = compare_df.index
        return compare_df

    def _num_outside_ranges(
        self,
        datapoint_df: pd.DataFrame,
        lower_range_bound: float,
        upper_range_bound: float,
    ) -> int:
        # Returns the number of datapoints outside the bounds of the model prediction
        datapoint_df["is_incorrect"] = (datapoint_df["y"] <= lower_range_bound) | (
            datapoint_df["y"] >= upper_range_bound
        )
        datapoint_df["is_incorrect"] = datapoint_df["is_incorrect"].astype(int)
        return datapoint_df["is_incorrect"].sum()

    def _num_outside_model_bounds(self, current_data: pd.DataFrame) -> int:
        # Returns the number of datapoints outside the bounds of the model prediction
        compare_df = self._get_forecast_with_current_data(current_data)
        return self._num_outside_ranges(
            compare_df, compare_df["yhat_lower"], compare_df["yhat_upper"]
        )

    def _num_outside_given_threshold(
        self, current_data: pd.DataFrame, threshold: float = 0.1
    ) -> int:
        # Returns the number of datapoints outside a percent threhsold of the model prediction
        compare_df = self._get_forecast_with_current_data(current_data)
        upper_bound = compare_df["yhat"] + threshold * compare_df["yhat"]
        lower_bound = compare_df["yhat"] - threshold * compare_df["yhat"]
        return self._num_outside_ranges(compare_df, lower_bound, upper_bound)

    def _get_alert_df(self, current_data_df):
        alert_df = current_data_df
        # filter holidays
        if self.filter_holidays:
            alert_df = alert_df[self.check_not_holiday(alert_df["ds"])]
        return alert_df

    def run_and_alert(
        self,
        current_df_or_query: pyspark.sql.dataframe.DataFrame,
        alert_threshold: int = 1,
        alert_type: str = "prediction_threshold",
        model_threshold: float = 0.1,
    ):
        """
        Run the model on current data (passed in as a query or dataframe) and send an alert if necessary.

        - current_df_or_query: The data to evaluate.  Can be passed in as a pyspark DataFrame or a query string. The dataframe (or resulting dataframe from the query) must have only two columns titled ds and y. The ds column should be the timestamp column and the y column should be the values.
        - alert_threshold: How many points have to deviate from the model before an alert is fired
        - alert_type: This parameter can take in two values:  `"prediction_threshold"` or `"confidence_bounds"`
            - prediction_threshold: A point is considered to be deviating from the model if it is some % threshold different than the model’s prediction. The % threshold is given by the `model_threshold` parameter.
            - confidence_bounds: A point is considered to be deviating from the model if it is outside of the confidence intervals of the model. Typically you’ll want to use this if you only want to catch large deviations, as the confidence intervals tend to be large.
        """
        num_outside = 0
        self._validate_arg_exists_and_type(
            current_data, [str, pyspark.sql.dataframe.DataFrame], "Current Data"
        )
        self._validate_arg_exists_and_type(alert_threshold, int, "Alert Threshold")
        self._validate_arg_exists_and_type(alert_type, str, "Alert Type")
        if not (
            alert_type == "prediction_threshold" or alert_type == "confidence_bounds"
        ):
            raise Exception(
                "Alert type must be either prediction_threshold or confidence_bounds"
            )
        self._validate_arg_exists_and_type(model_threshold, float, "Model Threshold")
        current_data_df = self._generate_time_data(current_df_or_query)
        alert_df = self._get_alert_df(current_data_df)
        if alert_type == "prediction_threshold":
            num_outside = self._num_outside_given_threshold(
                alert_df, threshold=model_threshold
            )
        if alert_type == "confidence_bounds":
            num_outside = self._num_outside_model_bounds(alert_df)
        if num_outside >= alert_threshold:
            file, plot_path = self._write_results_to_s3(
                current_data_df, alert_type, model_threshold
            )
            alert_msg = self._gen_alert_msg(
                num_outside,
                file,
                alert_type=alert_type,
                model_threshold=model_threshold,
            )
            send_slack_message_to_channels(alert_msg, self.channels)
            send_image_to_channels(plot_path, self.channels)

    def _prep_df_for_s3(
        self,
        compare_sdf: pyspark.sql.dataframe.DataFrame,
        alert_type,
        prediction_threshold,
    ) -> pyspark.sql.dataframe.DataFrame:
        compare_sdf = compare_sdf.withColumnRenamed("ds", "timestamp")
        compare_sdf = compare_sdf.withColumnRenamed("y", "actual_y")
        compare_sdf = compare_sdf.withColumnRenamed("yhat", "predicted_y")
        compare_sdf = compare_sdf.withColumnRenamed(
            "yhat_lower", "predicted_y_lower_bound"
        )
        compare_sdf = compare_sdf.withColumnRenamed(
            "yhat_upper", "predicted_y_upper_bound"
        )
        if alert_type == "prediction_threshold":
            compare_sdf = compare_sdf.withColumn(
                "predicted_y_lower_bound",
                F.col("predicted_y") - prediction_threshold * F.col("predicted_y"),
            )
            compare_sdf = compare_sdf.withColumn(
                "predicted_y_upper_bound",
                F.col("predicted_y") + prediction_threshold * F.col("predicted_y"),
            )
        return compare_sdf.select(
            "timestamp",
            "actual_y",
            "predicted_y",
            "predicted_y_lower_bound",
            "predicted_y_upper_bound",
        )

    def _write_plot_to_s3(
        self,
        compare_sdf: pyspark.sql.dataframe.DataFrame,
        s3_path: str,
        prediction_threshold: float,
    ):
        # Hack to save plot w/o display, have to do it in a udf
        compare_sdf_group = compare_sdf.withColumn("group_col", F.lit(1))
        compare_sdf_group = compare_sdf_group.groupBy("group_col").agg(
            F.collect_list(F.struct("timestamp", "actual_y", "predicted_y")).alias(
                "metric_data"
            )
        )
        compare_sdf_group = compare_sdf_group.withColumn(
            "plt_path",
            save_plot(
                F.col("metric_data"), F.lit(s3_path), F.lit(prediction_threshold)
            ),
        )
        compare_sdf_group.rdd.count()

    def _write_results_to_s3(
        self,
        current_data_df: pd.DataFrame,
        alert_type: str,
        prediction_threshold: float,
    ) -> str:
        compare_df = self._get_forecast_with_current_data(current_data_df)
        compare_sdf = spark.createDataFrame(compare_df)
        compare_sdf = self._prep_df_for_s3(
            compare_sdf, alert_type, prediction_threshold
        )
        alert_fname = self.alert_name.replace(" ", "_")
        folder_name = f"{alert_fname}_{int(round(time.time() * 1000))}"
        s3path = (
            f"s3://samsara-databricks-playground/metric-forecast-csvs/{folder_name}"
        )

        compare_sdf.coalesce(1).write.format("csv").save(s3path, header="true")

        file_in_s3_location = get_s3_client(
            "samsara-databricks-playground-read"
        ).list_objects_v2(
            Bucket="samsara-databricks-playground",
            Prefix=f"metric-forecast-csvs/{folder_name}",
        )[
            "Contents"
        ]
        self._write_plot_to_s3(compare_sdf, s3path, prediction_threshold)
        for file in file_in_s3_location:
            cur_filename = file["Key"].split("/")[-1]
            if cur_filename.startswith("part-"):
                return (
                    f"https://s3.internal.samsara.com/s3/samsara-databricks-playground/metric-forecast-csvs/{folder_name}/{cur_filename}",
                    f"/dbfs/mnt/samsara-databricks-playground/metric-forecast-csvs/{folder_name}/plot.png",
                )

    def _gen_alert_msg(
        self,
        num_outside: int,
        file: str,
        alert_type: str = "confidence_bounds",
        model_threshold: float = 0.1,
    ) -> str:
        alert_type_msg = "confidence intervals"
        if alert_type == "prediction_threshold":
            alert_type_msg = f"prediction threshold of {model_threshold*100}%"
        alert_msg = f"{self.alert_name}: Detected unusual behavior. There were {num_outside} datapoints outside the {alert_type_msg} of the model. Access the results at: {file}. A plot of the predicted vs actual results is below:"
        return alert_msg


# COMMAND ----------

# MAGIC %md
# MAGIC ## Welcome To Using The Timeseries Anomaly Detection System!
# MAGIC This notebook will provides the MetricForecastAlert class. This class takes in historical data, trains a model on it, and then takes in current data and fires an alert if anomalous behavior is detected.
# MAGIC
# MAGIC This is an example notebook showing how to use the system: https://samsara.cloud.databricks.com/?o=8972003451708087#notebook/4325226456381256/command/4325226456381261
# MAGIC
# MAGIC For more information on the API, run `help(MetricForecastAlert)`
