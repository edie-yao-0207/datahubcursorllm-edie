# Databricks notebook source
# MAGIC %pip install -U -qqqq mlflow>=3.1.3 databricks-agents>=1.2.0 databricks-langchain uv langgraph databricks-sql-connector[pyarrow]

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Define the agent in code
# MAGIC Define the single agent code with tools.
# MAGIC
# MAGIC #### Agent tools
# MAGIC This agent code adds the built-in Unity Catalog function `system.ai.python_exec` to the agent.
# MAGIC
# MAGIC For more examples of tools to add to your agent, see Databricks documentation ([AWS](https://docs.databricks.com/aws/generative-ai/agent-framework/agent-tool))
# MAGIC
# MAGIC #### Wrap the LangGraph agent using the ResponsesAgent interface
# MAGIC
# MAGIC For compatibility with Databricks AI features, the LangGraphResponsesAgent class implements the ResponsesAgent interface to wrap the LangGraph agent.
# MAGIC
# MAGIC Databricks recommends using ResponsesAgent as it simplifies authoring multi-turn conversational agents using an open source standard. See MLflow's ResponsesAgent documentation.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test the agent
# MAGIC
# MAGIC Interact with the agent to test its output and tool-calling abilities. Since this notebook called `mlflow.langchain.autolog()`, you can view the trace for each step the agent takes.
# MAGIC
# MAGIC Replace this placeholder input with an appropriate domain-specific example for your agent.

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from model.agent import AGENT
import os

os.environ["QUERY_AGENT_CLIENT_ID"] = dbutils.secrets.get(scope="dbx-agents", key="dbx-query-agent-client-id")
os.environ["QUERY_AGENT_CLIENT_SECRET"] = dbutils.secrets.get(scope="dbx-agents", key="dbx-query-agent-client-secret")


result = AGENT.predict({"input": [{"role": "user", "content": "How many customers are there?"}]})
print(result.model_dump(exclude_none=True))

# COMMAND ----------

from model.agent import AGENT
import os

os.environ["QUERY_AGENT_CLIENT_ID"] = dbutils.secrets.get(scope="dbx-agents", key="dbx-query-agent-client-id")
os.environ["QUERY_AGENT_CLIENT_SECRET"] = dbutils.secrets.get(scope="dbx-agents", key="dbx-query-agent-client-secret")

for chunk in AGENT.predict_stream(
    {"input": [{"role": "user", "content": "What is 6*7 in Python?"}]}
):
    print(chunk.model_dump(exclude_none=True))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log the agent as an MLflow model
# MAGIC
# MAGIC Log the agent as code from the `agent.py` file. See [MLflow - Models from Code](https://mlflow.org/docs/latest/models.html#models-from-code).
# MAGIC
# MAGIC ### Enable automatic authentication for Databricks resources
# MAGIC For the most common Databricks resource types, Databricks supports and recommends declaring resource dependencies for the agent upfront during logging. This enables automatic authentication passthrough when you deploy the agent. With automatic authentication passthrough, Databricks automatically provisions, rotates, and manages short-lived credentials to securely access these resource dependencies from within the agent endpoint.
# MAGIC
# MAGIC To enable automatic authentication, specify the dependent Databricks resources when calling `mlflow.pyfunc.log_model().`
# MAGIC
# MAGIC   - **TODO**: If your Unity Catalog tool queries a [vector search index](docs link) or leverages [external functions](docs link), you need to include the dependent vector search index and UC connection objects, respectively, as resources. See docs ([AWS](https://docs.databricks.com/generative-ai/agent-framework/log-agent.html#specify-resources-for-automatic-authentication-passthrough) | [Azure](https://learn.microsoft.com/azure/databricks/generative-ai/agent-framework/log-agent#resources)).
# MAGIC
# MAGIC

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------


from model.agent import AGENT, tools, LLM_ENDPOINT_NAME

import mlflow
from databricks_langchain import VectorSearchRetrieverTool
from mlflow.models.resources import DatabricksFunction, DatabricksServingEndpoint, DatabricksUCConnection, DatabricksSQLWarehouse
from unitycatalog.ai.langchain.toolkit import UnityCatalogTool
from pkg_resources import get_distribution

###########################
# Specify your model info
###########################
MODEL_NAME = "dbxquery-agent-dev"
MODEL_PATH = "model/agent.py"
CATALOG = "default"
SCHEMA = "datamodel_dev"

resources = [
    DatabricksServingEndpoint(endpoint_name=LLM_ENDPOINT_NAME),
    ]
for tool in tools:
    if isinstance(tool, VectorSearchRetrieverTool):
        resources.extend(tool.resources)
    elif isinstance(tool, UnityCatalogTool):
        resources.append(DatabricksFunction(function_name=tool.uc_function_name))

with mlflow.start_run():
    logged_agent_info = mlflow.pyfunc.log_model(
        name=MODEL_NAME,
        python_model=MODEL_PATH,
        resources=resources,
        artifacts={
            "datahub": "dbfs:/Volumes/s3/datahub-metadata/root/metadata/hacksara2025/datahub.json"
        },
        code_paths=["model"],
        pip_requirements=[
            f"databricks-connect=={get_distribution('databricks-connect').version}",
            f"mlflow=={get_distribution('mlflow').version}",
            f"databricks-langchain=={get_distribution('databricks-langchain').version}",
            f"langgraph=={get_distribution('langgraph').version}",
            f"databricks-sql-connector",
            f"seaborn>=0.12.0",
            f"matplotlib>=3.7.0",
            f"pandas>=2.0.0",
            f"numpy>=1.24.0"
        ],
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-deployment agent validation
# MAGIC Before registering and deploying the agent, perform pre-deployment checks using the [mlflow.models.predict()](https://mlflow.org/docs/latest/python_api/mlflow.models.html#mlflow.models.predict) API. See Databricks documentation ([AWS](https://docs.databricks.com/en/machine-learning/model-serving/model-serving-debug.html#validate-inputs) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/machine-learning/model-serving/model-serving-debug#before-model-deployment-validation-checks)).

# COMMAND ----------

mlflow.models.predict(
    model_uri=f"runs:/{logged_agent_info.run_id}/{MODEL_NAME}",
    input_data= {"input": [{"role": "user", "content": "What is 6*7 in Python?"}]},
    env_manager="uv",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register the model version to Unity Catalog
# MAGIC
# MAGIC Before you deploy the agent, you must register the agent to Unity Catalog. If the model already exists, the version will be auto-incremented.
# MAGIC

# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")

UC_MODEL_NAME = f"{CATALOG}.{SCHEMA}.{MODEL_NAME}"

# Register the model to UC
uc_registered_model_info = mlflow.register_model(
    model_uri=logged_agent_info.model_uri, name=UC_MODEL_NAME
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy the agent

# COMMAND ----------

from databricks import agents
agents.deploy(
    model_name=UC_MODEL_NAME,
    model_version=uc_registered_model_info.version,
    tags = {
        "team": "dataplatform",
        "product-group": "aianddata",
        "service": "databricks-serving-endpoint",
        "rnd-allocation": "1"
    },
    environment_vars={
        "QUERY_AGENT_CLIENT_ID": f"{{{{secrets/dbx-agents/dbx-query-agent-client-id}}}}",
        "QUERY_AGENT_CLIENT_SECRET": f"{{{{secrets/dbx-agents/dbx-query-agent-client-secret}}}}"
    },
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next steps
# MAGIC
# MAGIC After your agent is deployed, you can chat with it in AI playground to perform additional checks, share it with SMEs in your organization for feedback, or embed it in a production application. See Databricks documentation ([AWS](https://docs.databricks.com/en/generative-ai/deploy-agent.html) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/generative-ai/deploy-agent)).
