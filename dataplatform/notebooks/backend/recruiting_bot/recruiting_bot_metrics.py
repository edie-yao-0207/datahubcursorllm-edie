# Databricks notebook source
# MAGIC %run /Shared/Shared/slack-tables

# COMMAND ----------

# MAGIC %run /backend/backend/databricks_data_alerts/slack_utility

# COMMAND ----------

# MAGIC %run /backend/dataplatform/boto3_helpers

# COMMAND ----------


import base64
from datetime import date, datetime, timedelta

import boto3
import requests

SLACK_MESSAGE_LIMIT = 3500


# After getting the token from the parameter store, we will create the Basic authentication header
# https://developers.greenhouse.io/harvest.html#authentication
def _create_gh_header():
    ssm_client = get_ssm_client("greenhouse-api-token-ssm", "us-west-2")
    token = get_ssm_parameter(ssm_client, "GREENHOUSE_HARVEST_TOKEN") + ":"
    b_token = base64.b64encode(token.encode("ascii")).decode("ascii")
    headers = {"Authorization": f"Basic {b_token}"}
    return headers


# Using the Greenhouse API, get all of the Jobs tied to the specific departments
# and return the list of Job IDs
def get_jobs(department_ids):
    headers = _create_gh_header()

    jobs = []
    open_jobs = []
    for department in department_ids:
        job_response = requests.get(
            f"https://harvest.greenhouse.io/v1/jobs?per_page=500&department_id={department}",
            headers=headers,
        )
        job_page = job_response.json()

        for job in job_page:
            if (
                "test" not in job["name"].lower()
                and "vp" not in job["name"].lower()
                and "director" not in job["name"].lower()
                and "internal" not in job["name"].lower()
                and job["confidential"] == False
            ):
                jobs.append(job["id"])
                if job["status"] == "open":
                    open_jobs.append(job["id"])

        if "next" in job_response.links:
            while (
                job_response.links["next"]["url"] != job_response.links["last"]["url"]
            ):
                job_response = requests.get(
                    job_response.links["next"]["url"], headers=headers
                )
                job_page = job_response.json()
                for job in job_page:
                    if (
                        "test" not in job["name"].lower()
                        and "vp" not in job["name"].lower()
                        and "director" not in job["name"].lower()
                        and "internal" not in job["name"].lower()
                        and job["confidential"] == False
                    ):
                        jobs.append(job["id"])
                        if job["status"] == "open":
                            open_jobs.append(job["id"])
    return jobs, open_jobs


# Using the Greenhouse API, get all of the applications created after the created_after date
# and return the total number of applications tied to the specific Job IDs
def get_application_count(job_ids, created_after):
    headers = _create_gh_header()

    applications = []
    for job in job_ids:
        application_response = requests.get(
            f"https://harvest.greenhouse.io/v1/applications?per_page=500&created_after={created_after}&job_id={job}",
            headers=headers,
        )
        application_page = application_response.json()

        for application in application_page:
            if application["prospect"] == False and application["status"] == "active":
                applications.append(application)

        if "next" in application_response.links:
            while (
                application_response.links["next"]["url"]
                != application_response.links["last"]["url"]
            ):
                application_response = requests.get(
                    application_response.links["next"]["url"], headers=headers
                )
                application_page = application_response.json()
                for application in application_page:
                    if (
                        application["prospect"] == False
                        and application["status"] == "active"
                    ):
                        applications.append(application)

    num_applications = len(applications)

    return num_applications


# Using the Greenhouse API, get all of the scheduled interviews created after the created_after date
# and before the created_before date. If the interview is tied to a Job in the Job IDs list and
# and the interview is completed, get the corresponding candidate ID from the corresponding application.
# Last, return the number of completed interviews, number of candidates of the completed interviews,
# and number of scheduled interviews (after the current date).
def get_candidate_and_interview_count(job_ids, created_after, created_before):
    headers = _create_gh_header()

    interviews = []
    candidates = []
    num_complete_interviews = 0

    interview_response = requests.get(
        f"https://harvest.greenhouse.io/v1/scheduled_interviews?per_page=500&starts_after={created_after}&starts_before={created_before}",
        headers=headers,
    )
    interview_page = interview_response.json()

    for interview in interview_page:
        app_id = interview["application_id"]
        app = requests.get(
            f"https://harvest.greenhouse.io/v1/applications/{app_id}", headers=headers
        ).json()
        app_jobs = app["jobs"]
        for job in app_jobs:
            if job["id"] in job_ids:
                interviews.append(interview)
                interview_date = datetime.strptime(
                    interview["start"]["date_time"], "%Y-%m-%dT%H:%M:%S.%fZ"
                ).date()
                if interview_date <= date.today():
                    num_complete_interviews += 1
                    if app["candidate_id"] not in candidates:
                        candidates.append(app["candidate_id"])

    if "next" in interview_response.links:
        while (
            interview_response.links["next"]["url"]
            != interview_response.links["last"]["url"]
        ):
            interview_response = requests.get(
                interview_response.links["next"]["url"], headers=headers
            )
            interview_page = interview_response.json()
            for interview in interview_page:
                app_id = interview["application_id"]
                app = requests.get(
                    f"https://harvest.greenhouse.io/v1/applications/{app_id}",
                    headers=headers,
                ).json()
                app_jobs = app["jobs"]
                for job in app_jobs:
                    if job["id"] in job_ids:
                        interviews.append(interview)
                        interview_date = datetime.strptime(
                            interview["start"]["date_time"], "%Y-%m-%dT%H:%M:%S.%fZ"
                        ).date()
                        if interview_date <= date.today():
                            num_complete_interviews += 1
                            if app["candidate_id"] not in candidates:
                                candidates.append(app["candidate_id"])

    num_future_interviews = len(interviews) - num_complete_interviews
    num_candidates = len(candidates)

    return num_complete_interviews, num_candidates, num_future_interviews


# Using the GH API, get all unresolved (open) offers that are associated to Jobs
# in the Job IDs list. Sort the offers by created_at, and return a full list of
# the offer objects
def get_open_offers(job_ids):
    headers = _create_gh_header()

    open_offers = []

    offer_response = requests.get(
        f"https://harvest.greenhouse.io/v1/offers?status=unresolved&per_page=500",
        headers=headers,
    )
    offer_page = offer_response.json()
    for offer in offer_page:
        if offer["sent_at"] != None and datetime.strptime(
            offer["sent_at"], "%Y-%m-%d"
        ).date() >= datetime.date(datetime.today() - timedelta(days=60)):
            candidate_id = offer["candidate_id"]
            candidate = requests.get(
                f"https://harvest.greenhouse.io/v1/candidates/{candidate_id}",
                headers=headers,
            ).json()
            if offer["job_id"] in job_ids and len(candidate["linked_user_ids"]) == 0:
                open_offers.append(offer)
    if "next" in offer_response.links:
        while (
            offer_response.links["next"]["url"] != offer_response.links["last"]["url"]
        ):
            offer_response = requests.get(
                offer_response.links["next"]["url"], headers=headers
            )
            offer_page = offer_response.json()
            for offer in offer_page:
                if offer["sent_at"] != None and datetime.strptime(
                    offer["sent_at"], "%Y-%m-%d"
                ).date() >= datetime.date(datetime.today() - timedelta(days=60)):
                    candidate_id = offer["candidate_id"]
                    candidate = requests.get(
                        f"https://harvest.greenhouse.io/v1/candidates/{candidate_id}",
                        headers=headers,
                    ).json()
                    if (
                        offer["job_id"] in job_ids
                        and len(candidate["linked_user_ids"]) == 0
                    ):
                        open_offers.append(offer)

    open_offers = sorted(open_offers, key=lambda k: k["sent_at"])
    return open_offers


# Using the GH API, get all offers resolved after the resolved_after date that are associated to Jobs
# in the Job IDs list. Sort the offers by created_at, and return a full list of the offer objects
def get_accepted_offers(
    job_ids, resolved_after, created_after, include_all_accepted_offers
):
    headers = _create_gh_header()
    num_full_time_jobs = 0
    num_contractor_jobs = 0

    signed_offers = []
    accepted_offers = []

    offer_response = requests.get(
        f"https://harvest.greenhouse.io/v1/offers?status=accepted&resolved_after={resolved_after}&per_page=500",
        headers=headers,
    )
    offer_page = offer_response.json()

    for offer in offer_page:
        candidate_id = offer["candidate_id"]
        candidate = requests.get(
            f"https://harvest.greenhouse.io/v1/candidates/{candidate_id}",
            headers=headers,
        ).json()
        if offer["job_id"] in job_ids and len(candidate["linked_user_ids"]) == 0:
            start_date = datetime.strptime(offer["starts_at"], "%Y-%m-%d").date()
            if start_date > date.today():
                resolved_date = datetime.strptime(
                    offer["resolved_at"], "%Y-%m-%dT%H:%M:%S.%fZ"
                ).date()
                created_after_date = datetime.strptime(
                    created_after, "%Y-%m-%dT%H:%M:%S"
                ).date()
                if (
                    offer["custom_fields"]["employment_type"].lower()
                    == "Full-Time".lower()
                    or offer["custom_fields"]["employment_type"].lower()
                    == "Intern".lower()
                ):
                    num_full_time_jobs += 1
                elif (
                    offer["custom_fields"]["employment_type"].lower()
                    == "Contractor".lower()
                ):
                    num_contractor_jobs += 1
                if (
                    include_all_accepted_offers == False
                    and resolved_date >= created_after_date
                ) or (
                    include_all_accepted_offers == True and start_date > date.today()
                ):
                    accepted_offers.append(offer)
                else:
                    signed_offers.append(offer)

    if "next" in offer_response.links:
        while (
            offer_response.links["next"]["url"] != offer_response.links["last"]["url"]
        ):
            offer_response = requests.get(
                offer_response.links["next"]["url"], headers=headers
            )
            offer_page = offer_response.json()
            for offer in offer_page:
                candidate_id = offer["candidate_id"]
                candidate = requests.get(
                    f"https://harvest.greenhouse.io/v1/candidates/{candidate_id}",
                    headers=headers,
                ).json()
                if (
                    offer["job_id"] in job_ids
                    and len(candidate["linked_user_ids"]) == 0
                ):
                    start_date = datetime.strptime(
                        offer["starts_at"], "%Y-%m-%d"
                    ).date()
                    if start_date > date.today():
                        resolved_date = datetime.strptime(
                            offer["resolved_at"], "%Y-%m-%dT%H:%M:%S.%fZ"
                        ).date()
                        created_after_date = datetime.strptime(
                            created_after, "%Y-%m-%dT%H:%M:%S"
                        ).date()
                        if (
                            offer["custom_fields"]["employment_type"].lower()
                            == "Full-Time".lower()
                            or offer["custom_fields"]["employment_type"].lower()
                            == "Intern".lower()
                        ):
                            num_full_time_jobs += 1
                        elif (
                            offer["custom_fields"]["employment_type"].lower()
                            == "Contractor".lower()
                        ):
                            num_contractor_jobs += 1
                        if (
                            include_all_accepted_offers == False
                            and resolved_date >= created_after_date
                        ) or (
                            include_all_accepted_offers == True
                            and start_date > date.today()
                        ):
                            accepted_offers.append(offer)
                        else:
                            signed_offers.append(offer)

    accepted_offers = sorted(accepted_offers, key=lambda k: k["created_at"])
    return accepted_offers, signed_offers, num_full_time_jobs, num_contractor_jobs


# From the open_offers list of offers object, get the corresponding candidate metadata
# using the GET candidate endpoint and create a table using the slack-tables notebook
def make_open_offers_table(open_offers, include_candidate_name_in_open_offers_table):
    headers = _create_gh_header()

    num_new_open_offers = len(open_offers)
    open_offer_data = []
    for offer in open_offers:
        job_id = offer["job_id"]
        candidate_id = offer["candidate_id"]
        job = requests.get(
            f"https://harvest.greenhouse.io/v1/jobs/{job_id}", headers=headers
        ).json()
        candidate = requests.get(
            f"https://harvest.greenhouse.io/v1/candidates/{candidate_id}",
            headers=headers,
        ).json()
        if len(candidate["linked_user_ids"]) == 0:
            job_name = job["name"]
            candidate_name = candidate["first_name"] + " " + candidate["last_name"]
            if len(job["hiring_team"]["hiring_managers"]) == 1:
                hiring_manager = (
                    job["hiring_team"]["hiring_managers"][0]["first_name"]
                    + " "
                    + job["hiring_team"]["hiring_managers"][0]["last_name"]
                )
            else:
                hiring_manager = "Multiple"

            offer_date = offer["sent_at"]
            if len(job_name) > 47:
                job_name = job_name[0:44] + "..."
            if include_candidate_name_in_open_offers_table:
                open_offer_data.append(
                    [job_name, candidate_name, hiring_manager, offer_date]
                )
            else:
                open_offer_data.append([job_name, hiring_manager, offer_date])
        else:
            num_new_open_offers = num_new_open_offers - 1

    if include_candidate_name_in_open_offers_table:
        open_offer_columns = [
            "Job name",
            "Candidate Name",
            "Hiring manager",
            "Offer date",
        ]
    else:
        open_offer_columns = ["Job name", "Hiring manager", "Offer date"]
    open_offer_table = make_table("", open_offer_columns, open_offer_data)
    return open_offer_table


# From the open_offers list of offers object, get the corresponding candidate metadata
# using the GET candidate enpoint and create a table using the slack-tables notebook
def make_accepted_offers_table(accepted_offers, email_flag=True, employment_type=False):
    headers = _create_gh_header()

    accepted_offer_data = []
    for offer in accepted_offers:
        job_id = offer["job_id"]
        candidate_id = offer["candidate_id"]

        job = requests.get(
            f"https://harvest.greenhouse.io/v1/jobs/{job_id}", headers=headers
        ).json()
        candidate = requests.get(
            f"https://harvest.greenhouse.io/v1/candidates/{candidate_id}",
            headers=headers,
        ).json()

        if len(candidate["linked_user_ids"]) == 0:
            job_name = job["name"]
            candidate_name = candidate["first_name"] + " " + candidate["last_name"]

            if len(job["hiring_team"]["hiring_managers"]) == 1:
                hiring_manager = (
                    job["hiring_team"]["hiring_managers"][0]["first_name"]
                    + " "
                    + job["hiring_team"]["hiring_managers"][0]["last_name"]
                )
            else:
                hiring_manager = "Multiple"

            offer_date = offer["resolved_at"][0:10]
            starts_at = offer["starts_at"]
            email = candidate["email_addresses"][0]["value"]

            if len(job_name) > 47:
                job_name = job_name[0:44] + "..."

            accepted_offer_list = [
                job_name,
                candidate_name,
                email,
                hiring_manager,
                offer_date,
                starts_at,
            ]
            if not email_flag:
                accepted_offer_list = [
                    job_name,
                    candidate_name,
                    hiring_manager,
                    offer_date,
                    starts_at,
                ]

            if employment_type:
                accepted_offer_list.append(job["custom_fields"]["employment_type"])

            accepted_offer_data.append(accepted_offer_list)
        else:
            num_accepted_offers = num_accepted_offers - 1

    accepted_offer_columns = [
        "Job name",
        "New Hire Name",
        "Email",
        "Hiring manager",
        "Offer sign date",
        "Start Date",
    ]

    if not email_flag:
        accepted_offer_columns = [
            "Job name",
            "New Hire Name",
            "Hiring manager",
            "Offer sign date",
            "Start Date",
        ]
    if employment_type:
        accepted_offer_columns.append("Employment Type")
    accepted_offer_table = make_table("", accepted_offer_columns, accepted_offer_data)
    return accepted_offer_table


def break_table(table, character_limit_initially):
    table_partitions = []
    remaining_count = character_limit_initially
    header = ("\n").join(table[0:5])
    # if the header does not fit in the first message, then put it in the next message
    if len(header) > remaining_count:
        table_partitions.append([])
        remaining_count = SLACK_MESSAGE_LIMIT - len(header)
    cur_partition = []
    cur_partition += table[0:5]
    for row in table[5:]:
        # +1 for the \n that gets added in the join --> '\n'.join(table)
        if len(row) + 1 > remaining_count:
            if row != "```":
                cur_partition.append("```")
            table_partitions.append(cur_partition)
            cur_partition = []
            cur_partition.append("```")
            cur_partition.append(row)
            remaining_count = SLACK_MESSAGE_LIMIT - len(row) - 1
        else:
            cur_partition.append(row)
            remaining_count -= len(row) - 1
    if cur_partition[-1] != "```":
        cur_partition.append("```")
    table_partitions.append(cur_partition)
    return table_partitions


# Using all of the associated recruting metrics, create a formatted Slack message
# with the metrics / parameters inputted into the function
def send_recruiting_slack_message(
    num_jobs,
    num_applications,
    num_complete_interviews,
    num_candidates,
    num_new_open_offers,
    num_open_offers,
    num_accepted_offers,
    num_signed_not_started,
    num_future_interviews,
    slack_channel,
    team_name,
    role_name,
    accepted_offer_table="",
    open_offer_table="",
    interview_comment="",
    email_flag=True,
    employment_type=False,
    num_full_time_jobs=None,
    num_contractor_jobs=None,
    include_open_offer_table=True,
    include_accepted_offer_table=True,
    allow_closing_candidates_thread=True,
):

    slack_message = f"""
  :wave: Hi {team_name}! Just a friendly, neighborhood alert bot here with your weekly hiring updates.

  Last week was another busy one for {team_name} recruiting! Here are a few stats across our {role_name} roles to get excited about:
  • *{num_applications} applications received* which our hiring teams are busy reviewing! Shoutout to our recruiters and coordinators for all they do
  • *Conducted {num_complete_interviews} total interviews*, spanning across *{num_candidates} unique candidates!* {interview_comment}"""
    if num_new_open_offers > 0 or num_open_offers > 0:
        if num_new_open_offers > 0:
            open_offer_message = f"""
  • *Extended {num_new_open_offers} new offers, with {num_open_offers} total open offers!*"""
        elif num_open_offers > 0:
            open_offer_message = f"""
  • Hoping to close *{num_open_offers} open offers!*"""
        if allow_closing_candidates_thread:
            open_offer_message += " Hiring managers - please post in thread if you would like help closing these candidates :envelope_with_arrow:"
        if include_open_offer_table:
            if (
                len(slack_message)
                + len(open_offer_message)
                + len("\n".join(open_offer_table))
                <= SLACK_MESSAGE_LIMIT
            ):
                open_offer_message += "\n".join(open_offer_table)
                slack_message += open_offer_message
                send_slack_message_to_channels(slack_message, slack_channel)
            else:
                slack_message += open_offer_message
                open_offer_tables = break_table(
                    open_offer_table, SLACK_MESSAGE_LIMIT - len(slack_message)
                )
                slack_message += "\n".join(open_offer_tables[0])
                send_slack_message_to_channels(slack_message, slack_channel)
                for table in open_offer_tables[1:]:
                    send_slack_message_to_channels("\n".join(table), slack_channel)
        else:
            slack_message += open_offer_message
            send_slack_message_to_channels(slack_message, slack_channel)
    else:
        send_slack_message_to_channels(slack_message, slack_channel)

    final_message = f"""
  This week you can look forward to *{num_future_interviews} scheduled interviews across our {num_jobs} open jobs!* You can see more details in Greenhouse or in this <https://10az.online.tableau.com/#/site/samsaradashboards/views/Samsararecruitingdashboard-Allstaff/HiringOverview?:iid=1|hiring overview dashboard>. If you do not have access to Tableau, please contact <#C6ZDQER9U|it> for help.

  *Hiring great people ain't easy - thanks for all you do* :pray:
  --
  *Questions about these metrics?* Reach out in <#C024N9JJ1CN|ask-data-engineering>
  """

    if num_accepted_offers > 0:
        accepted_offer_message = f"""
  • *Hired {num_accepted_offers} new Samsarian(s)*, which makes a total of *{num_signed_not_started + num_accepted_offers} new people who have signed and not yet started!*"""
        if employment_type:
            accepted_offer_message += " We are excited to have"
            if num_full_time_jobs > 0:
                accepted_offer_message += (
                    f""" {num_full_time_jobs} full-time employees"""
                )
            if num_full_time_jobs > 0 and num_contractor_jobs > 0:
                accepted_offer_message += " and"
            if num_contractor_jobs > 0:
                accepted_offer_message += " {num_contractor_jobs} contractors"
            accepted_offer_message += " join the Samsara family!"
        accepted_offer_message += " Hooray!"
        if email_flag:
            accepted_offer_message += " Send your congrats using the e-mail below"
        if include_accepted_offer_table:
            if (
                len(accepted_offer_message)
                + len("\n".join(accepted_offer_table))
                + len(final_message)
                <= SLACK_MESSAGE_LIMIT
            ):
                accepted_offer_message += "\n".join(accepted_offer_table)
                slack_message = accepted_offer_message
                slack_message += final_message
                send_slack_message_to_channels(slack_message, slack_channel)
            else:
                slack_message = accepted_offer_message
                accepted_offer_tables = break_table(
                    accepted_offer_table, SLACK_MESSAGE_LIMIT - len(slack_message)
                )
                slack_message += "\n".join(accepted_offer_tables[0])
                send_slack_message_to_channels(slack_message, slack_channel)
                for table in accepted_offer_tables[1:]:
                    send_slack_message_to_channels("\n".join(table), slack_channel)
                send_slack_message_to_channels(final_message, slack_channel)
        else:
            slack_message = accepted_offer_message
            slack_message += final_message
            send_slack_message_to_channels(slack_message, slack_channel)

    elif num_signed_not_started > 0:
        accepted_offer_message = f"""
  • *Have a total of {num_signed_not_started} new hire(s) who have signed and not yet started!*"""
        slack_message = accepted_offer_message
        if employment_type:
            accepted_offer_message += " We are excited to have"
            if num_full_time_jobs > 0:
                accepted_offer_message += (
                    f""" {num_full_time_jobs} full-time employees"""
                )
            if num_full_time_jobs > 0 and num_contractor_jobs > 0:
                accepted_offer_message += " and"
            if num_contractor_jobs > 0:
                accepted_offer_message += " {num_contractor_jobs} contractors"
            accepted_offer_message += " join the Samsara family!"
        accepted_offer_message += " Hooray!\n"
        slack_message = accepted_offer_message
        slack_message += final_message
        send_slack_message_to_channels(slack_message, slack_channel)

    else:
        send_slack_message_to_channels(final_message, slack_channel)


# This is the final user defined function that individuals can use to trigger the
# recruting bot for their specific department(s). The user will input the department_ids,
# and associated slack channel, with some additional parameters specific for the slack message.
def recruiting_bot(
    slack_channel,
    team_name,
    role_name,
    departments=[],
    jobs=[],
    interview_comment="",
    num_days=9,
    email_flag=True,
    include_candidate_name_in_open_offers_table=False,
    employment_type=False,
    include_open_offer_table=True,
    include_accepted_offer_table=True,
    allow_closing_candidates_thread=True,
    include_all_accepted_offers=False,
):
    delta = timedelta(days=num_days)
    min_time = datetime.min.time()

    created_after_date = date.today() - delta
    created_after_datetime = datetime.combine(created_after_date, min_time)
    created_after = created_after_datetime.isoformat()

    created_before_date = date.today() + delta
    created_before_datetime = datetime.combine(created_before_date, min_time)
    created_before = created_before_datetime.isoformat()

    num_jobs = len(jobs)
    if len(jobs) == 0:
        department_ids = departments

        jobs, open_jobs = get_jobs(department_ids)
        num_jobs = len(open_jobs)

    num_applications = get_application_count(jobs, created_after)
    (
        num_complete_interviews,
        num_candidates,
        num_future_interviews,
    ) = get_candidate_and_interview_count(jobs, created_after, created_before)
    open_offers = get_open_offers(jobs)

    num_new_open_offers = 0
    for offer in open_offers:
        created_date = offer["created_at"]
        create_datetime = datetime.strptime(created_date, "%Y-%m-%dT%H:%M:%S.%fZ")
        if create_datetime.date() >= created_after_date:
            num_new_open_offers += 1

    resolved_delta = timedelta(days=60)
    resolved_after_date = date.today() - resolved_delta
    resolved_after_datetime = datetime.combine(resolved_after_date, min_time)
    resolved_after = resolved_after_datetime.isoformat()

    (
        accepted_offers,
        signed_offers,
        num_full_time_jobs,
        num_contractor_jobs,
    ) = get_accepted_offers(
        jobs, resolved_after, created_after, include_all_accepted_offers
    )
    open_offer_table = make_open_offers_table(
        open_offers, include_candidate_name_in_open_offers_table
    )
    accepted_offers_table = make_accepted_offers_table(
        accepted_offers, email_flag=email_flag, employment_type=employment_type
    )

    send_recruiting_slack_message(
        num_jobs=num_jobs,
        num_applications=num_applications,
        num_complete_interviews=num_complete_interviews,
        num_candidates=num_candidates,
        num_new_open_offers=num_new_open_offers,
        num_open_offers=len(open_offers),
        num_accepted_offers=len(accepted_offers),
        num_signed_not_started=len(signed_offers),
        num_future_interviews=num_future_interviews,
        slack_channel=slack_channel,
        team_name=team_name,
        role_name=role_name,
        accepted_offer_table=accepted_offers_table,
        open_offer_table=open_offer_table,
        interview_comment=interview_comment,
        email_flag=email_flag,
        employment_type=employment_type,
        num_full_time_jobs=num_full_time_jobs,
        num_contractor_jobs=num_contractor_jobs,
        include_open_offer_table=include_open_offer_table,
        include_accepted_offer_table=include_accepted_offer_table,
        allow_closing_candidates_thread=allow_closing_candidates_thread,
    )
