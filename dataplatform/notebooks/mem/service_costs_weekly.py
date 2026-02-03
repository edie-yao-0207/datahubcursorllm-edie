# Databricks notebook source
# MAGIC %pip install slack_sdk
# MAGIC

# COMMAND ----------

# MAGIC %run /backend/backend/databricks_data_alerts/slack_utility

# COMMAND ----------

# IMPORTANT NOTE TO ENGINEER :
# If you change anything, please make run alerting_system_test_cases and ensure that all the tests to pass
# You must run the file on a dev cluster (not production cluster)

# This Notebook is responsible for sending slack alert notifications using slack bot

from typing import List, Dict

# Import packages
import boto3
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError


# Sends the message to specified slack channels using the slack API
# Returns the first slack channel that the slack bot failed to send messages to
def send_slack_message_to_channels_with_blocks(
    slack_msg: str, slack_channels: List[str], blocks: List[Dict[any, any]] = []
):
    slack_result_error_msg = None
    client = get_client()
    for cur_channel in slack_channels:
        try:
            response = client.chat_postMessage(
                channel=cur_channel, text=slack_msg, blocks=blocks
            )
        except SlackApiError as e:
            if not slack_result_error_msg:
                slack_result_error_msg = cur_channel + " -- " + e.response["error"]
            else:
                slack_result_error_msg += (
                    "\n" + cur_channel + " -- " + e.response["error"]
                )
    return slack_result_error_msg


# COMMAND ----------

# general utils
import datetime as DT


def get_slash_formatted_timetstamp(timestamp):
    m = timestamp.strftime("%m")
    d = timestamp.strftime("%d")
    return f"{m}/{d}"


def get_2dp_number_string(num):
    return f"{'{:.2f}'.format(num)}"


def get_formatted_string(timestamp, cost):
    s = f"({get_slash_formatted_timetstamp(timestamp)}) ${get_2dp_number_string(cost)}"
    return s


def get_date_x_days_ago(days_ago):
    today = get_today()
    week_ago = today - DT.timedelta(days=days_ago)
    return week_ago


def get_today():
    return DT.date.today()


def get_date_range_string(start_date, end_date):
    return f"{get_slash_formatted_timetstamp(start_date)} to {get_slash_formatted_timetstamp(end_date)}"


# COMMAND ----------

# graph building

import matplotlib
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
import matplotlib.pyplot as plt
import tempfile


def send_cost_graph_to_channel(service_to_costs, channels, date_range):
    legends = []
    for key in service_to_costs:
        service_costs = service_to_costs[key]
        timestamps = []
        costs = []
        for daily_cost in service_costs:
            timestamps.append(get_slash_formatted_timetstamp(daily_cost.timestamp))
            costs.append(daily_cost.cost)
        timestamps.reverse()
        costs.reverse()
        plt.plot(timestamps, costs)
        legends.append(key)
    plt.legend(legends, loc="center left", bbox_to_anchor=(1, 0.5))
    plt.title(f"Costs for {date_range}")
    plt.ylabel("Costs ($)")
    plt.xlabel("Date")
    figure = plt.gcf()
    figure.set_size_inches(20, 14)
    with tempfile.TemporaryDirectory() as tmpdirname:
        file_path = f"{tmpdirname}/cost-graph.jpeg"
        plt.savefig(f"{file_path}", bbox_inches="tight", dpi=100)
        plt.close()
        err = send_image_to_channels(file_path, channels)
        if err:
            print(f"Error occurred during post of image: {err}")


# COMMAND ----------

# query utils


def get_query(services, earliest_date, latest_date, teams, cost_allocations):
    services_param = ",".join(services)
    teams_param = ",".join(teams)
    cost_allocations_param = ",".join(cost_allocations)
    in_params = []
    if services_param:
        in_params.append(f"service in ({services_param})")
    if teams_param:
        in_params.append(f"team in ({teams_param})")
    full_param = " or ".join(in_params)

    if cost_allocations_param:
        full_param = f"{full_param} and costAllocation in ({cost_allocations_param})"

    query = f"""
    select
      service,
      timestamp,
      sum(cost) as cost
    from
      bigquery.aws_cost_v2
    where
      ({full_param})
      and timestamp >= "{earliest_date}"
      and timestamp < "{latest_date}"
    group by
      service, timestamp
    order by
      service, timestamp desc
    """
    print(query)
    return query


# COMMAND ----------


def send_numbers_metrics_to_channel(
    service_to_costs, cost_allocations, channels, days_to_lookback, date_range
):
    # build message rows
    message_rows = []
    cost_allocations_display_str = ", ".join(cost_allocations)
    message_rows.append(
        f"*Service Expenditure (USD) For Last {days_to_lookback} Days - {date_range}* ({cost_allocations_display_str})"
    )

    team_total = 0
    for key in service_to_costs:
        service_costs = service_to_costs[key]
        total_service_cost = 0
        for daily_cost in service_costs:
            total_service_cost += daily_cost.cost
        message_rows.append(f"`{key}` - ${'{:.2f}'.format(total_service_cost)}")
        team_total += total_service_cost
    message_rows.append(f"Total: ${get_2dp_number_string(team_total)}")

    message = "\n".join(message_rows)
    send_slack_message_to_channels_with_blocks(message, channels)


def diff_cost(curr, prev) -> str:
    sign = "+" if curr > prev else ""
    return (
        f"${get_2dp_number_string(curr)} ({sign}{get_2dp_number_string(curr - prev)})"
    )


def send_diff_metrics_to_channel(
    prev_costs, curr_costs, channels, days_to_lookback, date_range, notebook_link
):
    diff_threshold = 5.00
    diffs = ""
    curr_total = 0
    prev_total = 0
    for key in curr_costs:
        curr_cost = 0
        for daily_cost in curr_costs[key]:
            curr_cost += daily_cost.cost
        curr_total += curr_cost

        prev_cost = 0
        if key in prev_costs:
            for daily_cost in prev_costs[key]:
                prev_cost += daily_cost.cost
            prev_total += prev_cost
        if abs(curr_cost - prev_cost) >= diff_threshold:
            diffs += f"`{key}` - {diff_cost(curr_cost, prev_cost)}\n"
    total_msg = f"Total - {diff_cost(curr_total, prev_total)}"
    link_message = f"<{notebook_link}|Edit these metrics>"
    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Global Service Expenditure (USD) For Last {days_to_lookback} Days - {date_range}*\n{total_msg}",
            },
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Service Changes Over ${get_2dp_number_string(diff_threshold)}*\n{diffs}\n{link_message}",
            },
        },
    ]
    err = send_slack_message_to_channels_with_blocks("", channels, blocks)


# COMMAND ----------


class CostParams:
    """
    CostParams is the main class used to configure what service cost
    metrics you want to send to slack.
    """

    # days_to_lookback specifies the number of days prior that you want to look over to calculate costs fr
    days_to_lookback = 7

    # services contains the list of services that you want to view the costs for.
    services = []

    # teams contains a list of teams that you want to see all the costs tagged with.
    # If your services are tagged with the correct team, you don't need to add those into
    # the services list above.
    teams = []

    # slack_channels contains the list of slack channels that you want to send these metrics to.
    # Note that if you want to get the graph, you'll need to make sure that the AlertsBot is an
    # app that has been added to the channel you want to receive the metrics in
    slack_channels = []

    # metrics_types is a list of types that we currently support to render different service cost
    # information.
    # We currently support the following
    # - text
    # - graph
    metrics_types = []

    # cost_allocations is a list of types of cost allocations we want to pull costs for
    cost_allocations = []

    # notebook_link, if specified, will add a link to the notebook that is emitting these metrics.
    # This is helpful for other viewers to get quick access to the source code that is emitting
    # these metrics.
    notebook_link = ""

    def __init__(
        self,
        days_to_lookback=7,
        services=[],
        teams=[],
        slack_channels=[],
        metrics_types=[],
        cost_allocations=[],
        notebook_link="",
        tableau_link="",
    ):
        self.days_to_lookback = days_to_lookback
        self.services = services
        self.teams = teams
        self.cost_allocations = cost_allocations
        self.slack_channels = slack_channels
        self.metrics_types = metrics_types
        self.notebook_link = notebook_link
        self.tableau_link = tableau_link


# COMMAND ----------


class Costs:
    """
    Costs is the main wrapper class that can help send metrics to slack
    depending on the params struct is specified.

    These cost metrics are pulled from the bigquery#aws_cost_v2 table, which
    should be the same data source that the cost dashboards in tableau
    are powered by.

    Tableau dashboard - https://10az.online.tableau.com/#/site/samsaradashboards/views/CloudCostBETA/CostDashboardBeta

    This Costs class wrapper is meant for a more push-based approach
    (compared to going to check tableau) to getting cost updates pushed
    to slack for members of your team to get a rough sense on how much
    your services are costing your team.
    """

    params = None

    def __init__(self, params):
        # TODO: do some validation on params for early returning.
        self.params = params

    def convert_query_results_to_service_to_costs(self, query_results):
        service_to_costs = {}
        for cost in query_results:
            current_costs = service_to_costs.get(cost.service, [])
            current_costs.append(cost)
            service_to_costs[cost.service] = current_costs
        return service_to_costs

    def send_metrics(self):
        # validate inputs
        support_string = "We only support 'text', 'graph', and 'markdown' right now."
        if not len(self.params.metrics_types):
            print(f"No metric types specified. {support_string}")
            return

        # build query using params
        today = get_today()
        curr_days_ago = get_date_x_days_ago(self.params.days_to_lookback)
        curr_range = get_query(
            self.params.services,
            curr_days_ago,
            today,
            self.params.teams,
            self.params.cost_allocations,
        )
        prev_days_ago = get_date_x_days_ago(self.params.days_to_lookback * 2)
        prev_range = get_query(
            self.params.services,
            prev_days_ago,
            curr_days_ago,
            self.params.teams,
            self.params.cost_allocations,
        )
        date_range = get_date_range_string(curr_days_ago, today)

        # run query
        curr_x_days_cost = spark.sql(curr_range).rdd.collect()
        prev_x_days_cost = spark.sql(prev_range).rdd.collect()

        # format results
        curr_costs = self.convert_query_results_to_service_to_costs(curr_x_days_cost)
        prev_costs = self.convert_query_results_to_service_to_costs(prev_x_days_cost)

        for metric_type in self.params.metrics_types:
            send_diff_metrics_to_channel(
                prev_costs,
                curr_costs,
                self.params.slack_channels,
                self.params.days_to_lookback,
                date_range,
                self.params.notebook_link,
            )


# COMMAND ----------

# See https://samsara-dev-us-west-2.cloud.databricks.com/?o=5924096274798303#notebook/3870991835386917/command/3870991835386918 for params docstring.
params = CostParams(
    days_to_lookback=7,
    services=[],
    teams=[
        '"mdmsdk"',
    ],
    slack_channels=["metrics-mdmsdk"],
    cost_allocations=['"COGS"'],
    metrics_types=["markdown"],
    notebook_link="https://github.com/samsara-dev/backend/blob/master/dataplatform/notebooks/mem/service_costs_weekly.py",
)

costs_metrics = Costs(params)
costs_metrics.send_metrics()
