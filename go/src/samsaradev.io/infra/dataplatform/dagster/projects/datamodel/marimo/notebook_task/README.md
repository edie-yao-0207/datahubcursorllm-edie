# Notebook Task Marimo

This Marimo notebook provides an interactive interface for running the `notebook_task_table` asset locally. It replicates the **exact same logic** as the Dagster asset and can execute PySpark DataFrames against a Databricks serverless cluster, displaying the resulting dataframe in the browser instead of writing to a table.

## Features

- **Interactive Configuration**: Set date ranges, names, and other parameters through a web UI
- **SQL Preview**: See the generated SQL query before execution
- **Exact Asset Replication**: Uses identical SQL generation and processing logic
- **PySpark Execution**: Execute PySpark DataFrames against Databricks serverless cluster
- **Multi-Mode Support**: PySpark → SQL → Simulation (automatic fallback)
- **Fallback Simulation**: Works without Databricks credentials using simulation mode
- **Partition Processing**: Processes each date partition individually (same as Dagster)
- **SQL Placeholder Replacement**: Replaces `{DATEID}` with actual partition keys
- **Data Quality Checks**: Validate the generated data
- **Real-time Sync**: Edit in web UI or code editor with automatic sync

## Quick Start

1. **Navigate to the project directory:**
   ```bash
   cd ~/co/backend/go/src/samsaradev.io/infra/dataplatform/dagster/projects/datamodel/marimo/notebook_task
   ```

2. **Run the setup script:**
   ```bash
   ./setup.sh
   ```

3. **Set up Databricks credentials (optional):**
   ```bash
   cp env.example .env
   # Edit .env with your DATABRICKS_TOKEN
   ```

4. **Start the Marimo development server:**
   ```bash
   uv run marimo edit marimo_notebook.py --watch
   ```

5. **Open your browser** to the URL shown in the terminal (typically `http://localhost:2719`)

## Usage

### Interactive Configuration

The notebook provides several interactive controls:

- **Date Range**: Select start and end dates for data generation
- **Names Input**: Add custom names to include in the table
- **Limit**: Set a limit for testing (useful for large datasets)

### Running the Asset

1. **Configure Parameters**: Use the web UI to set your desired parameters
2. **Preview SQL**: Review the generated SQL query with `{DATEID}` placeholders
3. **Execute Asset Logic**: Run the exact same logic as the Dagster asset
4. **Inspect Results**: View the generated dataframe (same as what would be written to Databricks)

### Development Workflow

- **Edit in Web UI**: Make changes directly in the browser
- **Edit in Code Editor**: Use Cursor or your preferred editor
- **Automatic Sync**: Changes sync bidirectionally between web UI and editor
- **Debug**: Use Marimo's built-in debugging capabilities

## Project Structure

```
marimo/notebook_task/
├── notebook_task_simple.py    # Simple Marimo notebook (no Dagster deps)
├── notebook_task_marimo.py    # Full Marimo notebook (with Dagster deps)
├── pyproject.toml             # Dependencies and project config
├── setup.sh                   # Setup script
└── README.md                  # This file
```

## Dependencies

The notebook includes these dependencies:
- **Marimo**: Interactive notebook interface
- **Pandas**: Data manipulation and analysis
- **NumPy**: Numerical computing
- **PySpark**: Apache Spark Python API for big data processing
- **Databricks Connect**: Connect to Databricks serverless clusters with PySpark
- **Databricks SQL Connector**: SQL fallback for Databricks connectivity
- **Python-dotenv**: Environment variable management

**Note**: Databricks connection is optional - the notebook works in simulation mode without credentials!

## Configuration

### Environment Variables

For Databricks connection, set these environment variables in a `.env` file:

```bash
# Required for Databricks connection
DATABRICKS_TOKEN="your-databricks-token"

# Optional overrides
DATABRICKS_HOST="samsara-dev-us-west-2.cloud.databricks.com"
DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/9fb6a34db2b0bbde"
```

**Getting your Databricks token:**
1. Go to your Databricks workspace
2. Click your username → "User Settings"
3. Go to "Access Tokens" tab
4. Click "Generate New Token"
5. Copy the token to your `.env` file

### Execution Modes

The notebook automatically tries execution modes in this order:

1. **PySpark DataFrames** (preferred): Uses `databricks-connect` to run PySpark code in Databricks
2. **SQL Queries** (fallback): Uses `databricks-sql-connector` for SQL execution
3. **Simulation** (no credentials): Runs locally without Databricks connection

### Asset Parameters

The notebook allows you to configure:

- **Date Range**: Start and end dates for data generation
- **Names**: Custom names to include in the table
- **Database**: Target database (defaults to `datamodel_dev`)
- **Partitions**: Daily partitions for the date range

## Advanced Usage

### Running with Actual Dagster

To run the asset with actual database connections:

```python
from dagster import materialize
from datamodel.assets.examples.notebook_task import notebook_task_table

# Materialize for a specific partition
result = materialize([notebook_task_table], partition_key="2024-01-01")
```

### Custom SQL Generation

The notebook uses the same `generate_sql()` function as the production asset, ensuring consistency between local testing and production.

### Data Quality Validation

The notebook includes built-in data quality checks:
- Missing data detection
- Duplicate record identification
- Schema validation
- Data type verification

## Troubleshooting

### Import Errors

If you encounter import errors:

1. Ensure you're in the correct directory
2. Check that the datamodel project is properly installed
3. Verify Python path configuration

### Dependency Issues

If dependencies fail to install:

1. Update uv: `uv self update`
2. Clear cache: `uv cache clean`
3. Reinstall: `uv sync --reinstall`

### Marimo Issues

If Marimo doesn't start:

1. Check if port 2719 is available
2. Try a different port: `uv run marimo edit notebook_task_marimo.py --port 2720`
3. Check Marimo logs for error details

## Benefits

- **Exact Asset Replication**: Uses identical logic to the production Dagster asset
- **Rapid Iteration**: Test changes quickly without full pipeline runs
- **Interactive Development**: Combine notebook interactivity with professional code editing
- **Production Parity**: Use the exact same code as production
- **Data Preview**: See exactly what data the asset would produce
- **Partition Testing**: Test with different date ranges and partition keys
- **Data Validation**: Built-in quality checks and data preview
- **Easy Configuration**: Web UI for parameter adjustment
- **Debugging**: Full debugging capabilities in the notebook interface

## Integration with Cursor

This setup provides seamless integration with Cursor:

- **Two-way sync**: Edit in either Marimo web UI or Cursor
- **AI assistance**: Use Cursor's AI features while maintaining notebook interactivity
- **Version control**: Clean Python files without notebook artifacts
- **Professional editing**: Full IDE features with notebook benefits

## Next Steps

1. **Customize**: Modify the notebook for your specific use case
2. **Extend**: Add more assets or functionality
3. **Deploy**: Use the same code in production pipelines
4. **Share**: Collaborate with team members using the interactive interface

This setup provides a powerful development environment that combines the best of interactive notebooks with professional code editing tools.
