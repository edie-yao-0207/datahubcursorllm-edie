# Fleet Benchmarking Metrics Calculator Per Org

This notebook calculates various fleet benchmarking metrics (severe speeding, mobile usage, MPGe, idling) across organizations using Samsara's GraphQL API.

## ⚠️ **WARNING** ⚠️

**This notebook is NOT meant to be recurring or run on a pipeline** (for
potential performance reasons but also authentication reasons and
frankly this is a bit "weird"/non-intuitive)

## Setup Instructions

### Option 1: Databricks Repos Setup
1. In Databricks, go to "Repos" in the left sidebar
2. Click "Add Repo"
3. Connect to your git repository containing this code
4. Navigate to this notebook in the repo

### Option 2: Copy/Paste Setup
1. Create a new Databricks notebook
2. Copy the entire contents of `calculate_benchmark_report_metric_per_org.py` into your notebook
3. Set the notebook language to Python

### Required Secrets Setup

The notebook requires two secrets to authenticate with Samsara's GraphQL API. Follow these steps to set them up (or to update them):

1. Get the required authentication values:
   - Go to `cloud.samsara.com`
   - Open Developer Tools (F12 or right-click -> Inspect)
   - Go to the Network tab
   - Refresh the page
   - Look for any request that starts with `graphql?q=`
   - Click on the request and go to the "Headers" section
   - Under "Request Headers", find:
     - `Cookie:` (a very long string)
     - `X-CSRF-Token:`
   - Copy these values

2. Set up the secrets in Databricks using the CLI:
   ```bash
   databricks secrets put-secret calculate_benchmarks_metric_script_graphql_keys graphql_cookie
   # Paste the Cookie value when prompted
   
   databricks secrets put-secret calculate_benchmarks_metric_script_graphql_keys graphql_csrf
   # Paste the X-CSRF-Token value when prompted
   ```

   Note that only the data-tools-group and dataproducts-group have permission to update these secrets. If needed, modify the code to use a new secret scope.

## Running the Script

### Option 1: Run as a Databricks Job (Recommended)
1. Go to the Databricks Jobs UI
2. Create a new job
3. Add a notebook task and select this notebook
4. Configure the job parameters:
   - `RUN_MODE`: Choose from:
     - `TEST` - Runs against a single test organization (default: org_id 2000042)
     - `BENCHMARKS_BETA_ORGS_ONLY` - Runs against all organizations in the fleet benchmarks beta
     - `ALL_CUSTOMERS_ORGS` - Runs against all customer organizations
   - `END_TIME`: Set the end date (format: YYYY-MM-DD)
   - `DURATION`: Set a start date (format: YYYY-MM-DD) for a custom time range, or
5. Run the job. 

### Option 2: Run Interactively in a Notebook
1. Open the notebook in Databricks
2. Set the widget parameters:
   - Select your desired run mode from the `RUN_MODE` widget dropdown
   - Set the `END_TIME` widget (format: YYYY-MM-DD)
   - Set the `DURATION` widget with either a date or months lookback value
3. Run all cells

## Output

The notebook writes results to the Delta table `dataplatform_dev.benchmark_metrics_per_org` with the following metrics:
- Severe speeding percentage
- Mobile usage per 1000 miles
- MPGe (Miles Per Gallon equivalent)
- Idling percentage

Each run generates a unique UUID that can be used to query the specific results for that run.

## Error Handling

- The notebook handles authorization errors and will exit gracefully if authentication fails
- Failed organizations are logged and skipped
- Progress is displayed during processing
- A summary of successful and failed organizations is provided at the end of the run 