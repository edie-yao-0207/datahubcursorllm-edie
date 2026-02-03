import boto3

REGION = boto3.session.Session().region_name

# Save data to Delta function
def save_to_delta(df, name):
    df.write.format("delta").mode("overwrite").saveAsTable(f"fleetsec_dev.`{name}`")
