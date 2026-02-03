# Databricks notebook source
# MAGIC %md
# MAGIC **Description: **
# MAGIC This notebook is intended to process the most recent XLSX file at the Operations SFTP location /samsara-jdm-ftp/wnc/thor/Shipping/ for all the country folders. Currently we have NA, EU and FN. It will identify all devices that are variant gateways (any product version except 1) and put them into a json. It will then write this json file to the `samsara-thor-variants` s3 bucket where it will be processed further.
# MAGIC
# MAGIC Known Limitations:
# MAGIC * Cannot process '.xls' files (only '.xlsx') - this affects 4 files in (2 x EU, 2 x FN)

# COMMAND ----------

# MAGIC %run /backend/dataplatform/boto3_helpers

# COMMAND ----------

from datetime import datetime, timedelta
import io
import json
import re
import time

import boto3
import openpyxl
import pandas as pd
import paramiko
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, StructField, StructType

# DBTITLE 1,SFTP constants
host = "s-09b9f0b38ea74363a.server.transfer.us-west-1.amazonaws.com"
user = "samsaraftp"
ssm_client = get_ssm_client("jdm-ftp-ssh-private-key-ssm", use_region=False)
pkey = get_ssm_parameter(ssm_client, "SAMSARA_JDM_FTP_SSH_PRIVATE_KEY")

# COMMAND ----------

# DBTITLE 1,Setup SFTP Connection
ssh_private_key = io.StringIO(pkey)
k = paramiko.RSAKey.from_private_key(ssh_private_key)
ssh_private_key.close()

c = paramiko.SSHClient()
c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
print("connecting")
c.connect(hostname=host, username=user, pkey=k)
print("connected")

# COMMAND ----------

# DBTITLE 1,Define func to parse the product_version column for variant version
def get_product_version_variant(string):
    matched = re.search(r"([?:\da-zA-Z]){2}-[\da-zA-Z]{3}", string)
    if matched != None:
        return (True, matched.group(1))
    else:
        return (False, "1")


# COMMAND ----------

# DBTITLE 1,Pull Data
t = c.open_sftp()
base_folder = "/samsara-jdm-ftp/wnc/thor/Shipping/"
countries = t.listdir(base_folder)
print(countries)

log_df = pd.DataFrame()
df = pd.DataFrame()

current_time = datetime.now()
thirty_days_ago = current_time - timedelta(days=30)

# Iterate through all the country folders and grab the files.
for country in countries:
    c_path = base_folder + country + "/"
    for fileattr in t.listdir_attr(c_path):
        print("\r\nFile: {}".format(fileattr.filename))
        # For each file in each folder, grab the spreadsheet information.
        try:
            path = c_path + fileattr.filename
            # Grab date from filename. If greater than 30 days ago, skip the file.
            match = re.search(r"\d{8}", path)
            if match:
                date_string = match.group()
                date_object = datetime.strptime(date_string, "%Y%m%d")
                if date_object < thirty_days_ago:
                    print("skipped due to more than 30 days ago")
                    continue
            else:
                continue

            with t.open(path) as fh:
                wb = openpyxl.load_workbook(fh)
                for sheet in wb.sheetnames:
                    print(" sheet: {}".format(sheet))
                    sheet_df = pd.DataFrame(wb[sheet].values)
                    if len(sheet_df.columns) < 2:
                        log_df = log_df.append(
                            {
                                "file": path,
                                "sheet": sheet,
                                "success": "No",
                                "details": "not enough columns",
                            },
                            ignore_index=True,
                        )
                        print("Fail: Not enough columns")
                        continue

                    # Set the column headers.
                    sheet_df.rename(columns=sheet_df.iloc[0], inplace=True)
                    sheet_df.drop(0, inplace=True)

                    # Clean up column header text for consistency.
                    # List of columns available in the spreadsheet found here:
                    # https://paper.dropbox.com/doc/RFC-Thor-Variant-FW-Rollout--BVDPI9WxHseEMyGrfKRTDZ3BAg-go9mQP8ieX62elLGStH1V#:uid=241114941161813924338482&h2=Shipping-Manifest-Information-
                    sheet_df.columns = [
                        each_string.lower() for each_string in sheet_df.columns
                    ]
                    sheet_df.columns = [
                        each_string.replace(" ", "_")
                        for each_string in sheet_df.columns
                    ]

                    sheet_df["country"] = country
                    sheet_df["file"] = path
                    sheet_df["sheet"] = sheet

                    # If the product_version matches the format "AB-CDE", we want to grab B and store that in the product_version_variant_id column.
                    # If it doesn't match or is 1, it is not a variant and we will ignore these files below.
                    sheet_df["product_version_variant_id"] = [
                        get_product_version_variant(each_string)[1]
                        if get_product_version_variant(each_string)[0]
                        else "1"
                        for each_string in sheet_df["product_version"]
                    ]

                    # Format the serial number column from XXXX-XXX-XXX to XXXXXXXXXX as that's how its stored in the gateways table.
                    sheet_df["sn"] = [
                        each_string.replace("-", "") for each_string in sheet_df["sn"]
                    ]

                    df = df.append(sheet_df)
                    log_df = log_df.append(
                        {"file": path, "success": "Yes", "details": None},
                        ignore_index=True,
                    )
                print("Pass")
        except Exception as e:
            print("Fail: {}".format(e))
            log_df = log_df.append(
                {"file": path, "success": "No", "details": e}, ignore_index=True
            )


# COMMAND ----------

# DBTITLE 1,Show log information
log_df

# COMMAND ----------

# DBTITLE 1,Join spreadsheet data with productsdb.gateways table
# Define schema
dfSchema = StructType(
    [
        StructField("build_sku", StringType(), True),
        StructField("attendee", StringType(), True),
        StructField("etd", StringType(), True),
        StructField("sn", StringType(), True),
        StructField("sku_original_plan", StringType(), True),
        StructField("note", StringType(), True),
        StructField("imei", StringType(), True),
        StructField("at&t_imsi", StringType(), True),
        StructField("at&t_iccid", StringType(), True),
        StructField("vodafone_imsi", StringType(), True),
        StructField("vodafone_iccid", StringType(), True),
        StructField("board_revision", StringType(), True),
        StructField("product_version", StringType(), True),
        StructField("carton_id", StringType(), True),
        StructField("pallet_id", StringType(), True),
        StructField("tracking_number", StringType(), True),
        StructField("po#", StringType(), True),
        StructField("country", StringType(), True),
        StructField("file", StringType(), True),
        StructField("sheet", StringType(), True),
        StructField("product_version_variant_id", StringType(), True),
    ]
)

if df.empty:
    print("No CSV files to process - skipping DataFrame creation and subsequent steps")
    dbutils.notebook.exit("No data to process")

t1 = spark.createDataFrame(df, schema=dfSchema)

# Grab all the gateways from the spreadsheet data for analysis purposes.
t1 = t1.select(
    col("build_sku"),
    col("sn"),
    col("product_version"),
    col("product_version_variant_id"),
    col("attendee"),
    col("etd"),
    col("sku_original_plan"),
    col("note"),
    col("imei"),
    col("at&t_imsi"),
    col("at&t_iccid"),
    col("vodafone_imsi"),
    col("vodafone_iccid"),
    col("board_revision"),
    col("tracking_number"),
    col("po#"),
    col("country"),
    col("file"),
    col("sheet"),
    col("carton_id"),
    col("pallet_id"),
)

# Only grab the gateways which don't have a product_version_variant_id of 1. A version of 1 is not a variant.
t2 = t1.filter(col("product_version_variant_id") != "1").select(
    col("build_sku"),
    col("sn"),
    col("product_version"),
    col("product_version_variant_id"),
    col("attendee"),
    col("etd"),
    col("sku_original_plan"),
    col("note"),
    col("imei"),
    col("at&t_imsi"),
    col("at&t_iccid"),
    col("vodafone_imsi"),
    col("vodafone_iccid"),
    col("board_revision"),
    col("tracking_number"),
    col("po#"),
    col("country"),
    col("file"),
    col("sheet"),
    col("carton_id"),
    col("pallet_id"),
)

t3 = table("productsdb.gateways").select(
    col("id").alias("gateway_id"), col("serial"), col("product_id"), col("variant_id")
)

# Inner join the filtered spreadsheet data with the gateways table on the serial column.
variant_gateways = t2.join(t3, t2.sn == t3.serial)
variant_gateways.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable("dataprep_firmware.variant_gateways")

# Inner join all the spreadsheet data with the gateways table on the serial column.
all_gateways = t1.join(t3, t1.sn == t3.serial)
all_gateways.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable("dataprep_firmware.manufacturing_shipping_gateways")

# COMMAND ----------

# DBTITLE 1,Create JSON from data in the variant_gateways table
values = variant_gateways.rdd.collect()
# Dictionary format -- gateway_id: {build_sku, serial, product_version, product_version_variant_id}
gateway_dict = {}
for item in values:
    gateway_dict[item["gateway_id"]] = {
        "build_sku": item["build_sku"],
        "serial": item["serial"],
        "product_version": item["product_version"],
        "product_version_variant_id": item["product_version_variant_id"],
        "product_id": item["product_id"],
        "variant_id": item["variant_id"],
    }

json_data = json.dumps(gateway_dict)
print(json_data)


# COMMAND ----------

# DBTITLE 1,Write JSON data to S3
s3 = get_s3_resource("samsara-thor-variants-readwrite")
region = boto3.session.Session().region_name
s3prefix = None
if region == "us-west-2":
    s3prefix = "samsara-"
elif region == "eu-west-1":
    s3prefix = "samsara-eu-"
else:
    raise Exception("bad region: " + region)

bucket = s3prefix + "thor-variants"

# Update the file `most_recent.json` which is read downstream by the service
# which will create thor variant overrides in firmwaredb.
obj = s3.Object(bucket, "most_recent.json")
obj.put(Body=json_data, ACL="bucket-owner-full-control")

# Write another copy of the file which will never be overwritten for historical purposes.
millis = int(round(time.time() * 1000))
year_month = datetime.utcnow().date().strftime("%Y-%m")
obj2 = s3.Object(bucket, "month={0}/run={1}/output.json".format(year_month, millis))
obj2.put(Body=json_data, ACL="bucket-owner-full-control")
