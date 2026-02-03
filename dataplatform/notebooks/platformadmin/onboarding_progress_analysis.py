# Databricks notebook source
# MAGIC %md
# MAGIC ## Onboarding Progress Analysis
# MAGIC This notebook analyses the onboarding progresses as per https://paper.dropbox.com/doc/Onboarding-metrics--A6478ejLDV2Q3~83l~LH0nhhAg-lwFsCDxYQsOwEdumdHA4G.
# MAGIC
# MAGIC The set of users analysed will be restricted once we start the experiment.

# COMMAND ----------

# imports
import pandas as pd
from pyspark.sql.functions import *

from matplotlib import rcParams
import matplotlib.pyplot as plt

# COMMAND ----------

# graphing
# make sure that the graph fits within a window and that x axis tickers are rotated for visibility.
plt.tight_layout()
rcParams.update({"figure.autolayout": True})
plt.xticks(rotation=45)

# COMMAND ----------

# consts / helpers
modules = [
    "ACTIVATE_ACCOUNT",
    "GETTING_AROUND",
    "CREATE_DRIVER",
    "SEND_MESSAGE",
    "CREATE_DOCUMENT",
    "CREATE_ALERT_CONTACT",
    "CREATE_SCHEDULED_REPORT",
    "CREATE_ALERT",
]
num_modules = len(modules)


def get_completed_column_name(i):
    return modules[i] + "_completed"


def get_completed_at_column_name(i):
    return modules[i] + "_completed_at_ms"


def get_completed_column_name_all():
    return [get_completed_column_name(i) for i in range(num_modules)]


def get_completed_at_column_name_all():
    return [get_completed_at_column_name(i) for i in range(num_modules)]


# COMMAND ----------

users_table = spark.sql("select id, email from clouddb.users")
onboarding_progresses_table = spark.sql(
    "select * from userpreferencesdb.onboarding_progresses"
)

# COMMAND ----------

# filter out null rows
onboarding_progresses_table = onboarding_progresses_table.where(
    col("progress").isNotNull()
)

# COMMAND ----------

# join the users and userpreferecnes table
users_onboarding_progresses = onboarding_progresses_table.join(
    users_table, onboarding_progresses_table.user_id == users_table.id
)

# COMMAND ----------

# transform to pandas for analysis
df = users_onboarding_progresses.toPandas()

# COMMAND ----------

# filter out to selected users and / or to non-samsara users once experiment starts.
df = df[~df["email"].str.endswith("@samsara.com")]
df = df[
    ~df["email"].str.match(
        "judychau07@gmail.com|Samsaraplattest1@yahoo.com|samsaraplattest4@gmail.com|Samsaraplattest@yahoo.com|samsaraplattest3@gmail.com|alanaan\+onboarding@gmail.com|alanaan@gmail.com|samsaraonboardingtest@gmail.com"
    )
]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Users with onboarding guide - raw data

# COMMAND ----------

df

# COMMAND ----------

# Drop unneeded columns
df = df.drop(["id", "user_id", "_timestamp", "_filename", "_rowid"], axis=1)

# COMMAND ----------

# Extract the progress proto into pandas columns
def t(x):
    x_dict = x.asDict()
    return pd.Series(
        {
            "completed_at_ms": x_dict["completed_at_ms"],
            "completed_modules": x_dict["completed_modules"],
            "completed_modules_completed_at_ms": x_dict[
                "completed_modules_completed_at_ms_map"
            ]
            if x_dict["completed_modules_completed_at_ms_map"] is not None
            else {},
        }
    )


df = df.merge(df.progress.apply(t), left_index=True, right_index=True)
df.head()

# COMMAND ----------

# Make the completed_modules column a set for further transformation
df.completed_modules = df.completed_modules.transform(lambda x: set(x))

# COMMAND ----------

# Preprocessing
# Extract all modules into their own completed, completed_at columns
for i in range(len(modules)):

    def t_module_completed(x):
        return i in x

    def t_module_completed_at_ms(x):
        if x is None:
            return None
        if str(i) in x:
            return x[str(i)]
        return None

    module_name = modules[i]
    df[get_completed_column_name(i)] = df.completed_modules.transform(
        t_module_completed
    )
    df[
        get_completed_at_column_name(i)
    ] = df.completed_modules_completed_at_ms.transform(t_module_completed_at_ms)

# Add num_completed_modules column to df
def t_num_completed_modules(x):
    if x is None:
        return 0
    return len(x)


df["num_completed_modules"] = df["completed_modules"].transform(t_num_completed_modules)

df.head()

# COMMAND ----------

# convert all module completion millis to datetimes
for i in range(len(modules)):
    df[get_completed_at_column_name(i)] = pd.to_datetime(
        df[get_completed_at_column_name(i)], unit="ms"
    )

df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Completion — Are users able to complete the suggested set of modules within 30 days of activating their account?
# MAGIC <ul>
# MAGIC <li> What percentage of users complete the suggested set of modules?
# MAGIC <li> After 30 days, what % of users have completed all modules in the guide?
# MAGIC </ul>

# COMMAND ----------

percent_guide_completed = (df["completed"].sum() / df.shape[0]) * 100
print(percent_guide_completed)

# COMMAND ----------

# MAGIC %md
# MAGIC ### What percentage of steps are completed on average?
# MAGIC <ul>
# MAGIC <li> After 30 days, what is the average number of modules completed, per user?
# MAGIC </ul>

# COMMAND ----------

percent_modules_completed = (df["num_completed_modules"] / num_modules * 100).mean()
print(percent_modules_completed)

# COMMAND ----------

# MAGIC %md
# MAGIC ### What modules are abandoned?
# MAGIC <ul>
# MAGIC   <li> After 30 days, what is the count of each module that is left uncompleted?
# MAGIC </ul>

# COMMAND ----------

abandoned_modules_per_module = (~df[get_completed_column_name_all()]).sum()
ax = abandoned_modules_per_module.plot.bar()
display(ax)

# COMMAND ----------

abandoned_modules_per_module

# COMMAND ----------

# MAGIC %md
# MAGIC ### Where do users get stuck?
# MAGIC <ul>
# MAGIC <li> What is the module with the max count above?
# MAGIC </ul>

# COMMAND ----------

abandoned_modules_per_module = abandoned_modules_per_module.rename("num_omitted")
abandoned_modules_per_module_frame = abandoned_modules_per_module.to_frame()
abandoned_modules_per_module_frame.max()

# COMMAND ----------

# MAGIC %md
# MAGIC ### What is the average time to perform each module?
# MAGIC <ul>
# MAGIC <li> After how many days is a given module marked complete?
# MAGIC </ul>

# COMMAND ----------

module_completion_times = df[get_completed_at_column_name_all()]
module_completion_durations = module_completion_times.subtract(df["created_at"], axis=0)
module_completion_durations.mean()
