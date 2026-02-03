import base64
import io
from typing import Any, Dict, Optional

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import seaborn as sns
from langchain_core.tools import tool
from model.datahub_data import DATAHUB_DATA
from pandas import DataFrame


@tool
def get_table_metadata(table_name: str) -> str:
    """
    Get metadata for a specific table from datahub.json.

    Args:
        table_name (str): The name of the table to retrieve metadata for.

    Returns:
        dict: Table metadata dictionary or None if not found.
    """
    import json
    import logging

    try:

        datahub_data = DATAHUB_DATA

        logging.debug(f"Loaded datahub_data type: {type(datahub_data)}")
        logging.debug(f"Looking for table: {table_name}")
        logging.debug(
            f"Available keys: {list(datahub_data.keys())[:5]}..."
        )  # Show first 5 keys

        table = datahub_data.get(table_name)

        if table:

            return json.dumps(table, indent=2, default=str)
        else:
            logging.warning(f"Table {table_name} not found in datahub.json")
            datahub_data_without_dbs = {
                k.split(".")[-1]: v for k, v in datahub_data.items()
            }

            logging.debug(
                f"datahub_data_without_dbs type: {type(datahub_data_without_dbs)}"
            )
            logging.debug(
                f"datahub_data_without_dbs keys: {list(datahub_data_without_dbs.keys())[:5]}..."
            )  # Show first 5 keys

            table = datahub_data_without_dbs.get(table_name)
            if table:

                return json.dumps(table, indent=2, default=str)
            else:
                logging.warning(f"Table {table_name} not found in datahub.json")
                return f"Table '{table_name}' not found in available metadata."

    except Exception as e:
        logging.error(f"Error retrieving table metadata for {table_name}: {str(e)}")
        return f"Error retrieving table metadata for {table_name}: {str(e)}"


@tool
def get_tables(search_str=None) -> str:
    """
    Read in datahub.json and return all table metadata that matches a search string.

    Args:
        search_str (str, optional): String to search for in table names. If None, returns all tables.

    Returns:
        list: List of table metadata dictionaries that match the search criteria.
    """
    import json
    import logging

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    try:

        datahub_data = DATAHUB_DATA

        # Ensure datahub_data is a dict of tables
        if not isinstance(datahub_data, dict):
            logging.error("datahub.json does not contain a list of tables")
            return "Error: datahub.json does not contain a list of tables"

        # Filter tables based on search string if provided

        if search_str:
            filtered_tables = [
                table
                for table, schema in datahub_data.items()
                if search_str.lower() in table.lower()
                or search_str.lower() in str(schema.get("description", "")).lower()
            ]
            if not filtered_tables:
                return f"No tables found matching search term: '{search_str}'"
            return json.dumps(filtered_tables, indent=2, default=str)

        # Return all table names as a formatted list
        table_list = list(datahub_data.keys())
        return json.dumps(table_list, indent=2, default=str)

    except Exception as e:
        logging.error(f"Error reading datahub.json: {str(e)}")
        return f"Error reading datahub.json: {str(e)}"


@tool
def execute_query(query: str) -> str:
    """
    Execute a SQL query using Databricks SQL Warehouse.

    Args:
        query (str): A SQL query for fetching data from Databricks Unity Catalog tables..

    Returns:
        str: Query results as a markdown string or error message.
    """
    import logging
    import os

    from databricks import sql
    from databricks.sdk.core import Config, oauth_service_principal

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    def credential_provider():
        config = Config(
            host=f"https://samsara-dev-us-west-2.cloud.databricks.com",
            client_id=os.getenv("QUERY_AGENT_CLIENT_ID"),
            client_secret=os.getenv("QUERY_AGENT_CLIENT_SECRET"),
        )
        return oauth_service_principal(config)

    logger.info("Establish connection")
    try:
        with sql.connect(
            server_hostname="samsara-dev-us-west-2.cloud.databricks.com",
            http_path=f"/sql/1.0/warehouses/9fb6a34db2b0bbde",
            credentials_provider=credential_provider,
        ) as connection:
            logger.info(f"Executing query: {query}")
            with connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchall_arrow()
                logger.info(f"Query result: {result}")

        # Convert Arrow table to pandas DataFrame
        df = result.to_pandas()

        if df.empty:
            return "Query returned no results."

        # Convert DataFrame to a markdown string for pretty printing
        result = df.to_markdown(index=False)
        return result

    except Exception as e:
        logger.error(f"Error executing query: {str(e)}")
        return f"Error executing query: {str(e)}"


@tool
def create_visualization(
    data_dict: Dict[str, Any],
    chart_type: str,
    x_column: str,
    y_column: Optional[str] = None,
    title: Optional[str] = None,
    figsize: Any = (10, 6),
    y_axis_title: Optional[str] = None,
    **kwargs,
) -> str:
    """
    Create a visualization using seaborn and return as base64 data URL.

    Style contract:
    - Months displayed like "Aug 2024" when x is a datetime series
    - No x-axis title
    - Include y-axis title (if provided)
    - No gridlines
    - Use Samsara blue #0369EA for lines
    - Label the last point on time-series charts
    """

    import pandas as pd

    SAMSARA_BLUE = "#0369EA"

    def _format_month_ticks(ax):
        # Format x-axis ticks as "Aug 2024" when the x axis holds datetimes
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
        for label in ax.get_xticklabels():
            label.set_rotation(45)
            label.set_ha("right")

    def _ensure_datetime_month(df: pd.DataFrame, xcol: str) -> pd.DataFrame:
        # If x is not datetime, try to parse. If it already is, leave it.
        if not pd.api.types.is_datetime64_any_dtype(df[xcol]):
            try:
                df[xcol] = pd.to_datetime(df[xcol])
            except Exception:
                # Leave as-is if truly categorical; user may already pass month strings like "Aug 2024"
                pass
        return df

    def _annotate_last_point(ax, x, y, color=SAMSARA_BLUE):
        try:
            ax.annotate(
                f"{y[-1]:,}",
                xy=(x[-1], y[-1]),
                xytext=(6, 0),
                textcoords="offset points",
                va="center",
                color=color,
                fontsize=9,
                fontweight="medium",
            )
        except Exception:
            pass

    try:
        df = pd.DataFrame(data_dict).copy()
        if isinstance(figsize, list):
            figsize = tuple(figsize)

        # Filter out keys that shouldn't reach seaborn directly
        invalid = {
            "kwargs",
            "index",
            "columns",
            "values",
            "pivot_index",
            "pivot_columns",
            "pivot_values",
        }
        valid_kwargs = {k: v for k, v in kwargs.items() if k not in invalid}

        # Theme and figure/axes
        sns.set_theme(style="white")  # clean base; we'll control grid/spines
        fig, ax = plt.subplots(figsize=figsize, dpi=150)

        # Route chart types
        ctype = chart_type.lower()
        if ctype == "line":
            df = _ensure_datetime_month(df, x_column)
            sns.lineplot(
                data=df,
                x=x_column,
                y=y_column,
                ax=ax,
                color=SAMSARA_BLUE,
                linewidth=3,
                **valid_kwargs,
            )
            # Format months if datetime
            if pd.api.types.is_datetime64_any_dtype(df[x_column]):
                _format_month_ticks(ax)
            _annotate_last_point(
                ax, df[x_column].to_numpy(), df[y_column].to_numpy(), SAMSARA_BLUE
            )

        elif ctype == "bar":
            sns.barplot(
                data=df,
                x=x_column,
                y=y_column,
                ax=ax,
                color=SAMSARA_BLUE,
                **valid_kwargs,
            )

        elif ctype == "scatter":
            sns.scatterplot(
                data=df,
                x=x_column,
                y=y_column,
                ax=ax,
                color=SAMSARA_BLUE,
                **valid_kwargs,
            )

        elif ctype == "histogram":
            sns.histplot(data=df, x=x_column, ax=ax, color=SAMSARA_BLUE, **valid_kwargs)

        elif ctype == "box":
            sns.boxplot(
                data=df,
                x=x_column,
                y=y_column,
                ax=ax,
                color=SAMSARA_BLUE,
                **valid_kwargs,
            )

        elif ctype == "heatmap":
            # Pull pivot arguments only for pivoting, not for seaborn
            p_index = kwargs.get("index", x_column)
            p_columns = kwargs.get("columns", y_column)
            p_values = kwargs.get("values", None)
            if p_values is None:
                # pick a numeric col if not provided
                numeric_cols = df.select_dtypes(include="number").columns.tolist()
                if not numeric_cols:
                    raise ValueError("Heatmap requires a numeric 'values' column.")
                p_values = numeric_cols[0]
            pivot = df.pivot_table(index=p_index, columns=p_columns, values=p_values)
            sns.heatmap(pivot, annot=True, cmap="viridis", ax=ax, **valid_kwargs)

        else:
            raise ValueError(f"Unsupported chart type: {chart_type}")

        # Titles / labels
        if title:
            ax.set_title(title, fontsize=12, weight="bold")

        # Enforce style rules
        ax.set_xlabel(None)  # No x-axis title
        if y_axis_title:
            ax.set_ylabel(y_axis_title)  # Always include y-axis title when provided

        # No gridlines, minimal spines
        ax.grid(False)
        sns.despine(ax=ax)

        # Ticks: improve readability
        ax.tick_params(axis="x", labelsize=9)
        ax.tick_params(axis="y", labelsize=9)

        # Layout & export
        plt.tight_layout()
        buf = io.BytesIO()
        fig.savefig(buf, format="png", bbox_inches="tight")
        buf.seek(0)
        image_base64 = base64.b64encode(buf.getvalue()).decode("utf-8")
        plt.close(fig)
        buf.close()

        return f"data:image/png;base64,{image_base64}"
    except Exception as e:
        import traceback

        return f"Error creating visualization: {e}\nDetails: {traceback.format_exc()}"
