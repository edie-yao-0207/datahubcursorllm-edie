# GPS Jamming Static Locations

The end result of this notebook is visible on the fleet map as a smart maps feature: [docs](https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/4790746)

## How to make changes to this notebook

1. Go to the original notebook here: [databricks](https://samsara-dev-us-west-2.cloud.databricks.com/editor/notebooks/2940105912593137)
2. Make changes to the notebook
3. Run the notebook and test if it works as expected
   - note that results by default are saved in the `/Volumes/s3/databricks-workspace/fleetsec/gps_jamming_heatmap`,
     so production env is not affected
4. File -> Export -> Source file\
5. Replace `gps_jamming_static_locations_notebook.py` with the exported file
6. Reformat code, as it may lack things like multiple lines between commands and linting on the CI may fail

## How data lands on the fleet map

The notebook that this notebook is based on contains this little code right after config:
```
# COMMAND ----------

# MAGIC %run /backend/fleetsecurity/gps_jamming_static_locations/config_overrides

# COMMAND ----------
```

It executes the `config_overrides.py` which changes the S3 target, from dev to prod.
