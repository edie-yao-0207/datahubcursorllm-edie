# Databricks notebook source
# This notebook formats ifta segment data into a monthly file as described
# by third parties Ryder and Penske, and then uploads said file to S3.
# This is done for every org with an integration in fuel.fuel_integrations
# To do this, the notebook creates 3 subfiles for ~10 day ranges, uploads them
# to S3, and then combines them into one file afterwards.
# This is done to avoid timeouts from manipulating the full month of data at once
import gzip
import logging
import os
import tempfile
from datetime import date, datetime, timedelta
from itertools import groupby

import boto3
from botocore.errorfactory import ClientError
from pyspark.sql.functions import (
    coalesce,
    col,
    concat,
    date_format,
    from_utc_timestamp,
    length,
    lit,
    lower,
    lpad,
    round,
    rpad,
    substring,
    when,
)
from pyspark.sql.types import IntegerType, TimestampType

# If user specifies a month in the format MM-YYYY use that, if not we will default to last month
dbutils.widgets.text("TARGET_MONTH_MM-YYYY", "")

# When testing this notebook in Databricks we will need to use non-prod buckets
# If IS_SCHEDULE_JOB is True, we can assume the notebook is running in prod as we pass that
# value in our metadata.json file
dbutils.widgets.dropdown("IS_SCHEDULED_JOB", "False", ["False", "True"])
IS_SCHEDULED_JOB = dbutils.widgets.get("IS_SCHEDULED_JOB") == "True"
S3_BUCKET = (
    "samsara-detailed-ifta-reports"
    if IS_SCHEDULED_JOB
    else "samsara-databricks-workspace"
)
S3_PREFIX = "" if IS_SCHEDULED_JOB else "fleetops/ifta-integrations/"

METERS_PER_MILE = 1609.344

# These are the enum values that represent ifta integrations
# as defined in ifta.proto
PENSKE = 1
RYDER = 2

# These are the files that we break a month of data into to avoid timeouts
FILE_PARTS_NAMES = ["01-to-10", "11-to-20", "21-to-end"]

tmp_dir = spark.sparkContext._jvm.java.lang.System.getProperty(
    "java.io.tmpdir", tempfile.gettempdir()
)

# Set up our logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logging.basicConfig(format="%(message)s")
logger.info("Running script..")

# getFilePartName returns the file name we should use for a given day of data
def getFilePartName(day):
    fileSlug = ""
    if day >= "01" and day <= "10":
        fileSlug = FILE_PARTS_NAMES[0]
    elif day > "10" and day <= "20":
        fileSlug = FILE_PARTS_NAMES[1]
    else:
        fileSlug = FILE_PARTS_NAMES[2]
    return fileSlug


def getHeader(row):
    # Use the first segment start which is the first day of the month
    startDate = queryRanges[0][0].replace("-", "")
    endDate = throughDate.replace("-", "")

    if row.integration_type == PENSKE:
        header = "H{space: <10}{customerName: <10}KG{startDate}{throughDate}{space: <6}\n".format(
            customerName=row.integration_customer_id,
            startDate=startDate,
            throughDate=endDate,
            space="",
        )
    elif row.integration_type == RYDER:
        header = "H{customerName: <10}{customer: <10}{space: <2}{startDate}{throughDate}0000\n".format(
            customerName=row.integration_customer_id,
            space="",
            customer="CUSTOMER",
            startDate=startDate,
            throughDate=endDate,
        )
    return header


# concatenateS3Files combines the three files that make up the monthly data into one file
def concatenateS3Files(row):
    base_filename = f"{row.integration_customer_id}-{fileYear}-{fileMonth}.txt"
    filename = f"/tmp/{base_filename}"
    gz_filename = f"/tmp/{base_filename}.gz"
    client = boto3.client("s3")
    s3FullFilePath = f"{row.org_id}/{row.integration_type}/{fileYear}/{fileMonth}/{row.integration_customer_id}-{fileYear}-{fileMonth}.txt"
    gzS3FullFilePath = f"{s3FullFilePath}.gz"

    # If this is a scheduled job, check if the file already exists.
    # If it does not exist, head_object will throw an AccessDenied error and execution will continue
    # If it does exist, this function will return and no changes will be made
    if IS_SCHEDULED_JOB:
        try:
            client.head_object(Bucket=S3_BUCKET, Key=S3_PREFIX + s3FullFilePath)
            logger.info(
                f"Combined file {S3_PREFIX + s3FullFilePath} already exists for scheduled job. Skipping..."
            )
            return
        except ClientError:
            logger.info(
                f"Combined file {S3_PREFIX + s3FullFilePath} does not yet exist for scheduled job. Generating..."
            )
            pass

    for subfile in FILE_PARTS_NAMES:
        subFilename = (
            f"{row.integration_customer_id}-{fileYear}-{fileMonth}-{subfile}.txt"
        )
        s3path = f"{row.org_id}/{row.integration_type}/{fileYear}/{fileMonth}/file-parts/{subFilename}"
        dailyContent = ""
        try:
            obj = client.get_object(Bucket=S3_BUCKET, Key=S3_PREFIX + s3path)
            dailyContent = obj["Body"].read().decode("utf-8")
        # The subfile will not exist if no Ryder vehicles were driven in the file's time range
        # Don't exit with an error if this is the case
        except ClientError:
            logger.info(f"Subfile {S3_PREFIX + s3path} does not exist. Continuing...")
            continue

        with open(filename, "a") as filehandle:
            filehandle.write(f"{dailyContent}\n")

    # Ingest the combined file and sort the lines based on device -> date -> time as per Ryder's reqs
    try:
        fullFile = open(filename)
        lines = fullFile.readlines()
        lines.sort()
        with open(filename, "w") as filehandle:
            header = getHeader(row)
            filehandle.write(f"{header}")
            filehandle.write("".join(lines))

        with open(filename, "r") as f, gzip.open(gz_filename, "w") as gz:
            gz.write(f.read().encode())

    # If no vehicles drove for this org for this month, no file will be generated with 'filename'
    # In that case, we do not want to throw an error when trying to open the file since we still
    # want to create the remaining org files
    # If having no file for an org is something we need to look into, our Datadog monitors should alert us when
    # no file exists for a given integration during the cron run in iftadetailupload
    except FileNotFoundError:
        logger.info(f"Combined file {filename} does not exist. Skipping...")
        return

    logger.info(f"Uploading combined file {S3_PREFIX + s3FullFilePath}")
    client.upload_file(
        filename,
        S3_BUCKET,
        S3_PREFIX + s3FullFilePath,
        {"ACL": "bucket-owner-full-control"},
    )

    logger.info(f"Uploading compressed combined file {S3_PREFIX + gzS3FullFilePath}")
    client.upload_file(
        gz_filename,
        S3_BUCKET,
        S3_PREFIX + gzS3FullFilePath,
        {"ACL": "bucket-owner-full-control"},
    )
    os.remove(filename)
    os.remove(gz_filename)


# getQueryDates returns an array of three query ranges that divide the previous month
# This allows us to manipulate a month's worth of data without timing out
# It also returns the first and last day of last month, which is needed for the header
# It also returns the first day of this month, as we need the added day to query since we deliver data in EST
def getQueryDates():
    today = date.today()
    # If a custom date is provided, parse that month, and then add 31 days (1 month)
    # so that we handle that month as if it was last month
    if dbutils.widgets.get("TARGET_MONTH_MM-YYYY") != "":
        dateInput = dbutils.widgets.get("TARGET_MONTH_MM-YYYY")
        monthInput = int(dateInput.split("-")[0])
        yearInput = int(dateInput.split("-")[1])
        today = date(yearInput, monthInput, 1)
        today = today + timedelta(days=31)
    firstOfThisMonth = today.replace(day=1)
    lastOfLastMonth = firstOfThisMonth - timedelta(days=1)
    firstOfLastMonth = lastOfLastMonth.replace(day=1)

    segment1Start = lastOfLastMonth.replace(day=1).strftime("%Y-%m-%d")
    segment1End = lastOfLastMonth.replace(day=11).strftime("%Y-%m-%d")
    segment2End = lastOfLastMonth.replace(day=21).strftime("%Y-%m-%d")
    segment3End = firstOfThisMonth.strftime("%Y-%m-%d")

    return (
        [
            [segment1Start, segment1End],
            [segment1End, segment2End],
            [segment2End, segment3End],
        ],
        firstOfLastMonth.strftime("%Y-%m-%d"),
        lastOfLastMonth.strftime("%Y-%m-%d"),
        firstOfThisMonth.strftime("%Y-%m-%d"),
    )


# formatSegmentsDataFrame returns a dataframe that is formatted to Ryder and Penske's standards
def formatSegmentsDataFrame(df, startDate, cutoffDate):
    # Create columns for the spacings we'll need
    df = df.withColumn("15space", rpad(lit(""), 15, " "))
    df = df.withColumn("20space", rpad(lit(""), 20, " "))
    df = df.withColumn("40space", rpad(lit(""), 40, " "))

    df = df.withColumn("C", lit("C"))
    # Penske prefers we supply the last 10 characters of device pretty name
    df = df.withColumn(
        "penskeDeviceName",
        rpad(
            coalesce(
                substring(col("devices.name"), -10, 10),
                substring(col("devices.serial"), -10, 10),
                substring(col("devices.id"), -10, 10),
            ),
            10,
            " ",
        ),
    )
    df = df.withColumn("deviceId", rpad(col("device_id"), 20, " "))

    # Return all vehicles for Penske
    # For Ryder only return a subset of vehicles as specified by ryder_id from external ids
    # or if the vehicle is named "RYDER <...>"
    df = df.filter(
        (col("integration_type") == PENSKE)
        | (
            (col("ryder_id").isNotNull())
            | (substring(col("name"), 1, 6) == lit("RYDER "))
        )
    )

    # Use the ryder_id if we have it, if not use the device name after the "RYDER " prefix
    df = df.withColumn(
        "ryderVehicle",
        rpad(coalesce(col("ryder_id"), substring(col("name"), 7, 17)), 10, " "),
    )

    # Ryder requires us to use a US timezone, and Penske requires
    # a consistent timezone, so just use EST
    df = df.withColumn(
        "easternTzDate",
        date_format(
            from_utc_timestamp(
                (col("start_ms") / 1000).cast(dataType=TimestampType()), "EST"
            ),
            "yyyyMMdd",
        ),
    )

    # Drop segments converted to EST from UTC that are no longer in our date range
    df = df.filter(
        (
            from_utc_timestamp(
                (col("start_ms") / 1000).cast(dataType=TimestampType()), "EST"
            )
            >= datetime.strptime(startDate, "%Y-%m-%d")
        )
        & (
            from_utc_timestamp(
                (col("start_ms") / 1000).cast(dataType=TimestampType()), "EST"
            )
            < datetime.strptime(cutoffDate, "%Y-%m-%d")
        )
    )

    # Reformat dates to EST with proper string format
    df = df.withColumn(
        "startDate",
        date_format(
            from_utc_timestamp(
                (col("start_ms") / 1000).cast(dataType=TimestampType()), "EST"
            ),
            "yyyyMMdd",
        ),
    )
    df = df.withColumn(
        "endDate",
        date_format(
            from_utc_timestamp(
                (col("end_ms") / 1000).cast(dataType=TimestampType()), "EST"
            ),
            "yyyyMMdd",
        ),
    )
    df = df.withColumn(
        "startTime",
        date_format(
            from_utc_timestamp(
                (col("start_ms") / 1000).cast(dataType=TimestampType()), "EST"
            ),
            "HHmm",
        ),
    )
    df = df.withColumn(
        "endTime",
        date_format(
            from_utc_timestamp(
                (col("end_ms") / 1000).cast(dataType=TimestampType()), "EST"
            ),
            "HHmm",
        ),
    )

    # Penske prefers lat/lng separated by :
    df = df.withColumn(
        "colonStartLatLng",
        rpad(
            concat(
                round(col("start_value.latitude"), 7),
                lit(":"),
                round(col("start_value.longitude"), 7),
            ),
            25,
            " ",
        ),
    )
    df = df.withColumn(
        "colonEndLatLng",
        rpad(
            concat(
                round(col("end_value.latitude"), 7),
                lit(":"),
                round(col("end_value.longitude"), 7),
            ),
            25,
            " ",
        ),
    )
    # Ryder prefers lat/lng separated by ,
    df = df.withColumn(
        "commaStartLatLng",
        rpad(
            concat(
                round(col("start_value.latitude"), 7),
                lit(","),
                round(col("start_value.longitude"), 7),
            ),
            25,
            " ",
        ),
    )
    df = df.withColumn(
        "commaEndLatLng",
        rpad(
            concat(
                round(col("end_value.latitude"), 7),
                lit(","),
                round(col("end_value.longitude"), 7),
            ),
            25,
            " ",
        ),
    )

    # Penske and Ryder don't accept jurisdictions of more than two characters
    df = df.filter(length(col("start_value.jurisdiction")) <= 2)
    df = df.withColumn("jurisdiction", rpad(col("start_value.jurisdiction"), 2, " "))

    # In some case start or end odo will be null (but usually not both)
    # In these cases the best we can do is can assume the leg distance was 0
    # and copy the other odo value
    df = df.withColumn(
        "nonNullStartOdo",
        coalesce(col("start_odo_meters"), col("end_odo_meters"), lit(0)),
    )
    df = df.withColumn(
        "nonNullEndOdo",
        coalesce(col("end_odo_meters"), col("start_odo_meters"), lit(0)),
    )

    df = df.withColumn(
        "startOdoKm",
        rpad(
            round(col("nonNullStartOdo") / 1000).cast(IntegerType()),
            7,
            " ",
        ),
    )
    df = df.withColumn(
        "endOdoKm",
        rpad(
            round(col("nonNullEndOdo") / 1000).cast(IntegerType()),
            7,
            " ",
        ),
    )

    # Return all segments for Ryder
    # For Penske, filter out segments that have < 500 meters as they will round to 0 km in our ouput
    # or filter out segments that have no difference in odometer
    df = df.filter(
        (col("integration_type") == RYDER)
        | (coalesce(col("canonical_distance_meters"), lit(0)) > lit(500))
        | (
            round(col("nonNullEndOdo") / 1000).cast(IntegerType())
            - round(col("nonNullStartOdo") / 1000).cast(IntegerType())
            > lit(0)
        )
    )

    df = df.withColumn(
        "legDistKm",
        rpad(
            when(col("canonical_distance_meters").isNull(), 0)
            .otherwise(round(col("canonical_distance_meters") / 1000))
            .cast(IntegerType()),
            5,
            " ",
        ),
    )

    # For Ryder we report distance in Canada in km and distance in US in miles
    # Ryder also prefers odo padded with zeros (ie. 00300)
    df = df.withColumn(
        "startOdoLocalized",
        lpad(
            when(
                lower(col("start_value.revgeo_country")) == "ca",
                round(col("nonNullStartOdo") / 1000).cast(IntegerType()),
            ).when(
                lower(col("start_value.revgeo_country")) == "us",
                round(col("nonNullStartOdo") / METERS_PER_MILE).cast(IntegerType()),
            ),
            7,
            "0",
        ),
    )

    df = df.withColumn(
        "endOdoLocalized",
        lpad(
            when(
                lower(col("start_value.revgeo_country")) == "ca",
                round(col("nonNullEndOdo") / 1000).cast(IntegerType()),
            ).when(
                lower(col("start_value.revgeo_country")) == "us",
                round(col("nonNullEndOdo") / METERS_PER_MILE).cast(IntegerType()),
            ),
            7,
            "0",
        ),
    )

    df = df.withColumn(
        "legDistLocalized",
        lpad(
            when(col("canonical_distance_meters").isNull(), 0)
            .when(
                lower(col("start_value.revgeo_country")) == "ca",
                round(col("canonical_distance_meters") / 1000).cast(IntegerType()),
            )
            .when(
                lower(col("start_value.revgeo_country")) == "us",
                round(col("canonical_distance_meters") / METERS_PER_MILE).cast(
                    IntegerType()
                ),
            ),
            5,
            "0",
        ),
    )

    df = df.withColumn("isLegEnd", when(col("leg_end"), "Y").otherwise("N"))
    # We currently only track (T)oll or (I)nterstate
    df = df.withColumn("roadType", when(col("start_value.toll"), "T").otherwise("I"))

    # We currently don't get road number information, so leave blank
    df = df.withColumn("roadNumber", rpad(lit(""), 4, " "))

    # Concatenate our formatted data columns into a single column,
    # with order and columns dependent on integration type
    df = df.withColumn(
        "formatted_string",
        when(
            col("integration_type") == PENSKE,
            concat(
                col("C"),
                col("penskeDeviceName"),
                col("deviceId"),
                col("20space"),
                col("startDate"),
                col("startTime"),
                col("endDate"),
                col("endTime"),
                col("colonStartLatLng"),
                col("colonEndLatLng"),
                col("jurisdiction"),
                col("startOdoKm"),
                col("endOdoKm"),
                col("legDistKm"),
                col("15space"),
                col("isLegEnd"),
                col("roadType"),
                col("roadNumber"),
            ),
        ).when(
            col("integration_type") == RYDER,
            concat(
                col("C"),
                col("ryderVehicle"),
                col("40space"),
                col("startDate"),
                col("startTime"),
                col("endDate"),
                col("endTime"),
                col("commaStartLatLng"),
                col("commaEndLatLng"),
                col("jurisdiction"),
                col("startOdoLocalized"),
                col("endOdoLocalized"),
                col("legDistLocalized"),
                col("15space"),
                col("isLegEnd"),
                col("roadType"),
                col("roadNumber"),
            ),
        ),
    )

    # Filter out any null formatted_string. Rows where the start or end proto value is null will cause this
    df = df.filter(col("formatted_string").isNotNull())

    # Drop all columns we no longer need
    df = df.select(
        col("org_id"),
        col("integration_type"),
        col("integration_customer_id"),
        col("start_ms"),
        col("formatted_string"),
        col("easternTzDate"),
    )
    return df


# writeFilesToS3 takes a partition of multiple orgs, sorted by org_id + integration_type + start_ms
# and writes files for each group of org_id + integration_type to S3
def writeFilesToS3(partition):
    # Further partition the partition into groups of org_id + integration_type
    partition_iterator = groupby(
        partition, key=lambda row: tuple(row[x] for x in ["org_id", "integration_type"])
    )

    for _, iterator in partition_iterator:
        # Get the first row which we use to create our filename
        first = next(iterator)
        org_id = first.org_id
        integ_id = first.integration_customer_id
        integ_type = first.integration_type
        day = first.easternTzDate[6:]
        fileSlug = getFilePartName(day)

        s3file = f"{org_id}/{integ_type}/{fileYear}/{fileMonth}/file-parts/{integ_id}-{fileYear}-{fileMonth}-{fileSlug}.txt"
        with tempfile.NamedTemporaryFile(dir=tmp_dir, delete=True, mode="w") as f:
            f.write(f"{first.formatted_string}")
            for row in iterator:
                f.write(f"\n{row.formatted_string}")
            f.flush()
            # If this is a scheduled job, check if the file exists before uploading so we don't overwrite
            # If the file does not exist, head_object will throw a 404 error and the file will upload
            # If the file does exist, the program will continue on without writing the files
            if IS_SCHEDULED_JOB:
                try:
                    boto3.client("s3").head_object(
                        Bucket=S3_BUCKET, Key=S3_PREFIX + s3file
                    )
                    logger.info(
                        f"Subfile {S3_PREFIX + s3file} already exists for scheduled job. Skipping..."
                    )
                except ClientError:
                    boto3.client("s3").upload_file(
                        f.name,
                        S3_BUCKET,
                        S3_PREFIX + s3file,
                        {"ACL": "bucket-owner-full-control"},
                    )
                    logger.info(f"Uploading subfile {S3_PREFIX + s3file}")
            else:
                boto3.client("s3").upload_file(
                    f.name,
                    S3_BUCKET,
                    S3_PREFIX + s3file,
                    {"ACL": "bucket-owner-full-control"},
                )
                logger.info(f"Uploading subfile {S3_PREFIX + s3file}")


# getDeviceRyderIds joins the devices table with external_keys/values
# to get the external ids representing ryder vehicles (ryderId) if they exist
# We only do this for Ryder, as Penske is okay with receiving all vehicles in an org
def getDeviceRyderIds(devices):
    external_vals = spark.table("productsdb.core_external_values")
    external_keys = spark.table("productsdb.core_external_keys")
    external_keys = external_keys.filter(col("name") == lit("ryderId"))

    external_vals = (
        external_vals.join(
            external_keys,
            (external_keys.org_id == external_vals.org_id)
            & (external_keys.uuid == external_vals.external_uuid),
            how="inner",
        )
        .select(
            external_keys.org_id, external_vals.object_id, external_vals.external_value
        )
        .withColumnRenamed("external_value", "ryder_id")
    )

    devices = devices.join(
        external_vals,
        (devices.org_id == external_vals.org_id)
        & (devices.id == external_vals.object_id),
        how="left",
    ).select(
        devices.org_id, devices.id, devices.name, devices.serial, external_vals.ryder_id
    )
    return devices


queryRanges, startDate, throughDate, queryThroughDate = getQueryDates()

# Get year and month for use in our files
fileDate = queryRanges[0][0].replace("-", "")
fileYear = fileDate[0:4]
fileMonth = fileDate[4:6]

integrations = spark.table("fueldb_shards.ifta_integrations")
# Filter devices down to only include VGs in our integration orgs
devices = spark.sql(
    """
    SELECT * FROM productsdb.devices
    WHERE product_id IN (SELECT product_id FROM definitions.products WHERE name LIKE '%VG%')
    AND org_id IN (SELECT org_id FROM fueldb_shards.ifta_integrations)
    """
)
devices = getDeviceRyderIds(devices)
devices = devices.withColumnRenamed("org_id", "devices_org_id")
devices.createOrReplaceTempView("devices")

# Ryder requires that all start lat/lngs match the previous segments end lat/lng
# LAG back for the previous segment's end value to ensure this
# If there is no previous end_value, then this is the first segment, so just use this segment's start_value
query = f"""
   SELECT
    seg.date,
    seg.org_id,
    seg.device_id,
    seg.start_ms,
    seg.end_ms,
    COALESCE(LAG(seg.end_value) OVER (PARTITION BY seg.org_id, seg.device_id ORDER BY seg.end_ms), seg.start_value) AS start_value,
    seg.end_value,
    seg.start_odo_meters,
    seg.end_odo_meters,
    seg.canonical_distance_meters,
    seg.leg_end
   FROM ifta_report.annotated_location_segments_leg_end seg
   WHERE seg.date >= "{startDate}" AND seg.date <= "{queryThroughDate}"
    AND seg.org_id IN (
        SELECT org_id FROM fueldb_shards.ifta_integrations
    )
    AND seg.device_id IN (
        SELECT id FROM devices
    )
"""
connected_segments = spark.sql(query)

segmentRanges = []
for _ in range(0, 3):
    segmentRanges.append(connected_segments)

# Break the month of data into 3 ~10 day chunks so that we don't timeout when processing
for idx, df in enumerate(segmentRanges):
    newDf = df.filter((col("date").between(queryRanges[idx][0], queryRanges[idx][1])))
    newDf = newDf.join(
        devices,
        (newDf.org_id == devices.devices_org_id) & (newDf.device_id == devices.id),
        how="inner",
    ).select(
        [col("devices.id"), col("devices.name"), col("devices.serial"), col("ryder_id")]
        + [col(x) for x in newDf.columns]
    )
    # Rename column before joining so we don't have duplicate column names
    newDf = newDf.withColumnRenamed("org_id", "data_org_id")
    newDf = integrations.join(
        newDf, integrations.org_id == newDf.data_org_id, how="left"
    )
    newDf = formatSegmentsDataFrame(newDf, queryRanges[idx][0], queryRanges[idx][1])
    newDf = newDf.repartition("org_id")
    newDf = newDf.sortWithinPartitions(["org_id", "integration_type", "start_ms"])
    segmentRanges[idx] = newDf
    newDf.foreachPartition(writeFilesToS3)

integrations.foreach(concatenateS3Files)
