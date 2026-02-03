# Databricks notebook source
# MAGIC %run /backend/safety/speed_limits/utils_calling_conventions

# COMMAND ----------

"""
This notebook takes in the following args:
* ARG_TOMTOM_VERSION:
Pass in "" to return the latest downloaded version.
Pass in KEYWORD_FETCH_LATEST to redownload the newest dataset.
Pass in "YYYYMM000" to return skip downloading and return an older version.
"""
dbutils.widgets.text(ARG_TOMTOM_VERSION, "")
tomtom_version = dbutils.widgets.get(ARG_TOMTOM_VERSION)
print(f"{ARG_TOMTOM_VERSION}: '{tomtom_version}'")

DEFAULT_REGIONS = [CAN, EUR, MEX, USA]

"""
* ARG_REGIONS:
Pass in "" to generate for DEFAULT_REGIONS
Pass in a comma-separated list of regions (ex: "EUR,USA")
"""
dbutils.widgets.text(ARG_REGIONS, serialize_regions(DEFAULT_REGIONS))
desired_regions = dbutils.widgets.get(ARG_REGIONS)
if len(desired_regions) == 0:
    desired_regions = DEFAULT_REGIONS
else:
    desired_regions = deserialize_regions(desired_regions)
print(f"{ARG_REGIONS}: {desired_regions}")

# COMMAND ----------

import os
import re

import geopandas
import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    FloatType,
)

# Specifying schema allows for optimized conversion between Pandas and Pyspark
nw_schema = StructType(
    [
        StructField("ID", LongType(), False),
        StructField("NAME", StringType(), True),
        StructField("FEATTYP", IntegerType(), False),
        StructField("FRC", IntegerType(), False),
        StructField("NET2CLASS", IntegerType(), False),
        StructField("wkt", StringType(), False),
    ]
)

sr_schema = StructType(
    [
        StructField("ID", LongType(), False),
        StructField("SPEED", IntegerType(), False),
        StructField("SPEEDTYP", StringType(), False),
        StructField("VT", IntegerType(), False),
        StructField("SEQNR", IntegerType(), False),
        StructField("VALDIR", IntegerType(), False),
    ]
)

st_schema = StructType(
    [
        StructField("ID", LongType(), False),
        StructField("SEQNR", IntegerType(), False),
        StructField("SUBSEQNR", IntegerType(), False),
        StructField("TIMEDOM", StringType(), False),
    ]
)

a1_schema = StructType(
    [
        StructField("ID", LongType(), False),
        StructField("ORDER01", StringType(), False),
        StructField("NAME", StringType(), False),
    ],
)

ta_schema = StructType(
    [
        StructField("ID", LongType(), False),
        StructField("AREID", LongType(), False),
        StructField("ARETYP", IntegerType(), False),
    ],
)


def multinet_to_sdf(nw_gdf, sr_gdf, st_gdf, a1_gdf, ta_gdf):
    nw_gdf["wkt"] = pd.Series(nw_gdf["geometry"], index=nw_gdf.index, dtype="string")
    nw_gdf = nw_gdf.drop("geometry", axis=1)

    # FEATTYP 4110 = roads; use this filter to discard ferry connections
    nw_sdf = sqlContext.createDataFrame(nw_gdf, schema=nw_schema).filter(
        "FEATTYP = 4110"
    )

    # SPEEDTYP 1 = maxspeed; use this filter to discard recommended & lane-dependent speeds
    # VT 16 = taxi
    sr_sdf = (
        sqlContext.createDataFrame(sr_gdf, schema=sr_schema)
        .filter("SPEEDTYP = 1")
        .filter("VT != 16")
    )
    st_sdf = sqlContext.createDataFrame(st_gdf, schema=st_schema)

    # A1 Administrative Area Order 1, Geometry with Basic Attributes
    # There are 10 in total, A0 -> A9 but for specifying the US state just the A1 is needed which defines the US state boundaries.
    # As we move from A0 -> A9 more detailed boundaries are specified (country, US state, county, tract, block group, block, etc.)
    a1_sdf = sqlContext.createDataFrame(a1_gdf, schema=a1_schema)

    # TA Transportation Element Belonging to Area. The Transportation Element belonging to Area describes the relation between the
    # Transportation Elements and the areas in which they belong.
    # ARETYP = 1112; use this filter to only select Elements belonging to the A1 administrative Area Order 1
    ta_sdf = sqlContext.createDataFrame(ta_gdf, schema=ta_schema).filter(
        "ARETYP = 1112"
    )

    nw_sdf = nw_sdf.select(
        col("ID").alias("id"),
        col("NAME").alias("name"),
        col("FRC").alias("frc"),
        col("NET2CLASS").alias("n2c"),
        col("wkt"),
    )
    sr_sdf = sr_sdf.select(
        col("ID").alias("id"),
        col("SPEED").alias("maxspeed"),
        col("VT").alias("vehicle_type"),
        col("SEQNR").alias("seqnum"),
        col("VALDIR").alias("val_dir"),
    )
    st_sdf = st_sdf.select(
        col("ID").alias("id"),
        col("SEQNR").alias("seqnum"),
        col("SUBSEQNR").alias("subseqnum"),
        col("TIMEDOM").alias("timedom"),
    )

    # ORDER01 is the state name code/abbreviation e.g. CA for California, RI for Rhode Island, etc.
    a1_sdf = a1_sdf.select(
        col("ID").alias("areid"),
        col("ORDER01").alias("state_code"),
    )

    # AREID is the area id matching the ID of the A1 table
    ta_sdf = ta_sdf.select(
        col("ID").alias("id"),
        col("AREID").alias("areid"),
    )

    # NW: ID → TA: ID → A1: AREID relationship will provide the US state name to the basic geometry
    joined_nw_ta = nw_sdf.join(ta_sdf, on="id", how="left")
    joined_nw_ta_a1 = joined_nw_ta.join(a1_sdf, on="areid", how="left")

    joined_sr = sr_sdf.join(st_sdf, on=["id", "seqnum"], how="left")
    grouped_sr = joined_sr.groupBy("id").agg(
        collect_list(
            struct(
                col("seqnum"),
                col("maxspeed"),
                col("vehicle_type"),
                col("subseqnum"),
                col("timedom"),
                col("val_dir"),
            )
        ).alias("speed_restrictions")
    )
    joined_nw = joined_nw_ta_a1.join(grouped_sr, on="id", how="left")
    return joined_nw


# COMMAND ----------


# Specifying schema allows for optimized conversion between Pandas and Pyspark
lrs_schema = StructType(
    [
        StructField("ID", LongType(), False),
        StructField("SEQNR", IntegerType(), False),
        StructField("FEATTYP", IntegerType(), False),
        StructField("RESTRTYP", StringType(), False),
        StructField("VT", IntegerType(), False),
        StructField("RESTRVAL", FloatType(), False),
        StructField("LIMIT", FloatType(), False),
    ]
)

ltd_schema = StructType(
    [
        StructField("ID", LongType(), False),
        StructField("SEQNR", IntegerType(), False),
        StructField("SUBSEQNR", IntegerType(), False),
        StructField("TIMEDOM", StringType(), False),
    ]
)

lvc_schema = StructType(
    [
        StructField("ID", LongType(), False),
        StructField("SEQNR", IntegerType(), False),
        StructField("SUBSEQNR", IntegerType(), False),
        StructField("VT_CLASS", IntegerType(), False),
        StructField("VALUE", LongType(), False),
        StructField("UNIT_MEAS", IntegerType(), False),
    ]
)


def logistics_to_sdf(lrs_gdf, ltd_gdf, lvc_gdf):
    # FEATTYP 4110 = roads; use this filter to discard ferry connections
    # RESTRTYP = SP; speed restrictions only
    # TODO: Filter out advisory limits
    lrs_sdf = (
        sqlContext.createDataFrame(lrs_gdf, schema=lrs_schema)
        .filter("FEATTYP = 4110")
        .filter("RESTRTYP = 'SP'")
    )
    ltd_sdf = sqlContext.createDataFrame(ltd_gdf, schema=ltd_schema)

    # TODO: Filter out advisory limits
    lvc_sdf = sqlContext.createDataFrame(lvc_gdf, schema=lvc_schema)

    grouped_ltd = ltd_sdf.groupBy("ID", "SEQNR").agg(
        collect_list(struct(col("SUBSEQNR"), col("TIMEDOM"))).alias("time_domains")
    )
    grouped_lvc = lvc_sdf.groupBy("ID", "SEQNR").agg(
        collect_list(
            (struct(col("SUBSEQNR"), col("VT_CLASS"), col("VALUE"), col("UNIT_MEAS")))
        ).alias("vehicle_chars")
    )
    joined_lrs = lrs_sdf.join(grouped_ltd, on=["ID", "SEQNR"], how="left").join(
        grouped_lvc, on=["ID", "SEQNR"], how="left"
    )
    grouped_lrs = joined_lrs.groupBy("ID").agg(
        collect_list(
            struct(
                col("SEQNR"),
                col("VT"),
                col("RESTRVAL"),
                col("LIMIT"),
                col("time_domains"),
                col("vehicle_chars"),
            )
        ).alias("logistics_restrictions")
    )
    return grouped_lrs


def speed_unit_for_tomtom_region(region: str, tomtom_region: str):
    """
    returns the proper speed unit determined by the major region, then the tomtom
    region for country-specific.
    """
    MPH_UNIT = "mph"
    KPH_UNIT = "kph"
    if region == USA:
        return MPH_UNIT
    # TOMTOM regions that use mph
    mph_tomtom_regions = ["gbr"]  # UK
    # Remove numbers for tomtom region
    # reformat a country/subregion path like "gbr/g01" to "gbr"
    tomtom_region = re.sub(r"\d+", "", tomtom_region).split("/")[0]
    if tomtom_region in mph_tomtom_regions:
        return MPH_UNIT

    # most countries use KPH
    return KPH_UNIT


def get_country_code(region: str, tomtom_region: str):
    if region in ["CAN", "MEX", "USA"]:
        return region

    if region == "EUR":
        return tomtom_region.split("/")[0].upper()
    raise Exception(f"Region {region} is not supported!")


# COMMAND ----------


def get_tomtom_regions_list(version_id: str, region: str):
    logistics_mnt_prefix = TOMTOM_S3.make_volume_path(version_id, "logistics", region)
    multinet_mnt_prefix = TOMTOM_S3.make_volume_path(version_id, "multinet", region)
    tomtom_logistics_regions = []
    for root, dirs, files in os.walk(logistics_mnt_prefix):
        # For eur, print the full path (ex. alb/alb or gbr/g21).
        if not dirs:
            tomtom_logistics_regions.append(
                os.path.relpath(os.path.join(root), logistics_mnt_prefix)
            )

    tomtom_multinet_regions = []
    for root, dirs, files in os.walk(multinet_mnt_prefix):
        # For eur, print the full path (ex. alb/alb or gbr/g21).
        if not dirs:
            tomtom_multinet_regions.append(
                os.path.relpath(os.path.join(root), multinet_mnt_prefix)
            )

    if len(tomtom_logistics_regions) != len(tomtom_multinet_regions):
        print(
            f"Warning: logistics and multinet differ: \nlogistics: {tomtom_logistics_regions}\nmultinet: {tomtom_multinet_regions}"
        )
    return tomtom_logistics_regions, tomtom_multinet_regions


def run_tomtom_to_table(version_id: str, region: str):
    table_name = DBX_TABLE.tomtom_full_ways(version_id, region)
    logistics_table_name = DBX_TABLE.tomtom_logistics(version_id, region)
    multinet_table_name = DBX_TABLE.tomtom_multinet(version_id, region)

    if DBX_TABLE.is_table_exists(table_name):
        print(f"table {table_name} already exists... Skipping extraction for {region}.")
        return

    spark.sql(f"DROP TABLE IF EXISTS {logistics_table_name}")
    spark.sql(f"DROP TABLE IF EXISTS {multinet_table_name}")

    tomtom_logistics_regions, tomtom_multinet_regions = get_tomtom_regions_list(
        version_id, region
    )
    logistics_mnt_prefix = TOMTOM_S3.make_volume_path(version_id, "logistics", region)
    multinet_mnt_prefix = TOMTOM_S3.make_volume_path(version_id, "multinet", region)

    # Convert logistics data
    for logistics in tomtom_logistics_regions:
        print(f"logistics: {logistics}")
        lrs_gdf = geopandas.read_file(
            os.path.join(logistics_mnt_prefix, logistics, "lrs.dbf")
        )[["ID", "SEQNR", "FEATTYP", "RESTRTYP", "VT", "RESTRVAL", "LIMIT"]].dropna()
        lrs_gdf["RESTRVAL"] = lrs_gdf["RESTRVAL"].astype(float)
        ltd_gdf = geopandas.read_file(
            os.path.join(logistics_mnt_prefix, logistics, "ltd.dbf")
        )[["ID", "SEQNR", "SUBSEQNR", "TIMEDOM"]].dropna()
        lvc_gdf = geopandas.read_file(
            os.path.join(logistics_mnt_prefix, logistics, "lvc.dbf")
        )[["ID", "SEQNR", "SUBSEQNR", "VT_CLASS", "VALUE", "UNIT_MEAS"]].dropna()
        logistics_sdf = logistics_to_sdf(lrs_gdf, ltd_gdf, lvc_gdf)
        logistics_sdf.write.mode("append").option(
            "overwriteSchema", "True"
        ).saveAsTable(logistics_table_name)

    # Convert multinet data
    for multinet in tomtom_multinet_regions:
        # Get the unit associated with speedlimits based on the multinet region name.
        speed_unit = speed_unit_for_tomtom_region(region, multinet)
        country_code = get_country_code(region, multinet)

        print(f"multinet: {multinet}")
        nw_gdf = geopandas.read_file(
            os.path.join(multinet_mnt_prefix, multinet, "nw.shp")
        )[["ID", "NAME", "FEATTYP", "FRC", "NET2CLASS", "geometry"]]
        nw_gdf = nw_gdf.dropna(subset=["ID", "FEATTYP", "FRC", "NET2CLASS", "geometry"])
        sr_gdf = geopandas.read_file(
            os.path.join(multinet_mnt_prefix, multinet, "sr.dbf")
        )[["ID", "SPEED", "SPEEDTYP", "VT", "SEQNR", "VALDIR"]].dropna()
        st_gdf = geopandas.read_file(
            os.path.join(multinet_mnt_prefix, multinet, "st.dbf")
        )[["ID", "SEQNR", "SUBSEQNR", "TIMEDOM"]].dropna()
        a1_gdf = geopandas.read_file(
            os.path.join(multinet_mnt_prefix, multinet, "a1.shp")
        )[["ID", "ORDER01", "NAME"]].dropna()
        ta_gdf = geopandas.read_file(
            os.path.join(multinet_mnt_prefix, multinet, "ta.dbf")
        )[["ID", "AREID", "ARETYP"]].dropna()
        multinet_sdf = (
            multinet_to_sdf(nw_gdf, sr_gdf, st_gdf, a1_gdf, ta_gdf)
            .withColumn("maxspeed_unit", lit(speed_unit))
            .withColumn("country_code", lit(country_code))
        )
        multinet_sdf.write.mode("append").option("overwriteSchema", "True").saveAsTable(
            multinet_table_name
        )

    multinet_table = spark.table(multinet_table_name)
    logistics_table = spark.table(logistics_table_name)
    mn_log_joined_sdf = (
        multinet_table.alias("multinet")
        .join(logistics_table.alias("logistics"), on="ID", how="left")
        .select(
            col("multinet.id").alias("id"),
            col("multinet.name").alias("name"),
            col("multinet.frc").alias("frc"),
            col("multinet.n2c").alias("n2c"),
            col("multinet.wkt").alias("wkt"),
            col("multinet.speed_restrictions").alias("speed_restrictions"),
            col("multinet.maxspeed_unit").alias("maxspeed_unit"),
            col("multinet.country_code").alias("country_code"),
            col("multinet.state_code").alias("state_code"),
            col("logistics.logistics_restrictions").alias("logistics_restrictions"),
        )
    )

    print(f"outputting to {table_name}")
    mn_log_joined_sdf.write.mode("overwrite").option(
        "overwriteSchema", "True"
    ).saveAsTable(table_name)


# COMMAND ----------


for region in desired_regions:
    print(f"starting conversion for {region}")
    run_tomtom_to_table(tomtom_version, region)

# COMMAND ----------


exit_notebook()

# COMMAND ----------
