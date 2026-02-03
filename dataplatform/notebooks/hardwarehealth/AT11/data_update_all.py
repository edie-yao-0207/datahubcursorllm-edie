# Databricks notebook source
import pandas as pd
from datetime import datetime, timedelta, date
from delta.tables import *
from pyspark.sql.functions import col, when
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, coalesce
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    LongType,
    ByteType,
    DoubleType,
    FloatType,
    DecimalType,
)
from decimal import Decimal
import struct

datelist = pd.date_range(date.today() - timedelta(days=5), date.today())
datelist = datelist.strftime("%Y-%m-%d").to_list()

# COMMAND ----------

###### PROTOCOL 1

# Define the Spark schema corresponding to the crux_adv_ver_alpha_01_t struct
schema1 = StructType(
    [
        StructField("protocol_version", IntegerType(), True),
        StructField("flags", IntegerType(), True),
        StructField("reset_reason", IntegerType(), True),
        StructField("tx_power_dbm", IntegerType(), True),
        StructField("boot_count", LongType(), True),
        StructField("adv_interval_us", IntegerType(), True),
        StructField("temp_c", IntegerType(), True),
        StructField("batt", IntegerType(), True),
        StructField("fw_version", IntegerType(), True),
    ]
)


# Define a UDF to parse the byte array `adv_packet`
# Update the function to match the actual structure of your byte array and desired output
def parse_crux_adv_1(byte_array):
    # Unpack the byte array according to the struct
    # Check if the input is None
    if byte_array is None or len(byte_array) < struct.calcsize("<BBbBQIbBi"):
        # Return None or a tuple of default values for each field in the schema
        return (None,) * len(schema1.fields)
    # Note: Python struct format string needs to be aligned with the C struct
    # '<' indicates little-endian, 'B' unsigned byte, 'b' signed byte, 'Q' unsigned long long, 'I' unsigned int, 'i' signed int
    values = struct.unpack("<BBbBQIbBi", byte_array)
    return values


# Register the UDF
parse_crux_adv_1_udf = udf(parse_crux_adv_1, schema1)


# COMMAND ----------

###### PROTOCOL 2

# Define the Spark schema corresponding to the crux_adv_ver_alpha_01_t struct
schema2 = StructType(
    [
        StructField("protocol_version", IntegerType(), True),
        StructField("flags", IntegerType(), True),
        StructField("reset_reason", IntegerType(), True),
        StructField("tx_power_dbm", IntegerType(), True),
        StructField("boot_count", LongType(), True),
        StructField("temp_c", FloatType(), True),
        StructField("batt", DoubleType(), True),
        StructField("fw_version", IntegerType(), True),
        StructField("error_code", IntegerType(), True),
        StructField("peripheral_asset_id", DecimalType(20, 0), True),
    ]
)

# Define a UDF to parse the byte array `adv_packet`
# Update the function to match the actual structure of your byte array and desired output
def parse_crux_adv_2(byte_array, mac):
    ###### PROTOCOL 2
    try:
        if byte_array[1] & 0x01:
            (
                _,
                flags,
                tx_power_dbm,
                _,
                fw_version,
                _,
                _,
            ) = struct.unpack("<BBBBHQQ", byte_array)

            protocol_version = byte_array[0] & 0x0F

            time_since_conn = byte_array[3] & 0x0F

            error_code = (byte_array[3] >> 4) & 0x0F

            # Get the actual field from MAC
            mac_hex = "{:012x}".format(int(mac))

            # Calculate battery voltage
            digi_batt = int(mac_hex[4:6], 16)
            batt = 1.6 + (digi_batt / 256) * 2.4

            byte_sequence = bytes.fromhex(mac_hex)
            reversed_bytes = byte_sequence[::-1]
            bits_decode = struct.unpack_from("<H", reversed_bytes, 1)[0]

            digital_temp_c = (bits_decode >> 11) & 0x1F
            temp_c = -40 + (float(digital_temp_c) / 32) * (85 - -40)

            reset_reason = (bits_decode >> 6) & 0x1F

            boot_count = bits_decode & 0x3F

            mac_byte = byte_array[6:12]
            peripheral_asset_id = Decimal(
                int.from_bytes(mac_byte, byteorder="little", signed=False)
            ).quantize(Decimal("1"))

        else:
            (
                _,
                flags,
                tx_power_dbm,
                _,
                fw_version,
                _,
                _,
                digi_batt,
                rr2,
                _,
                _,
            ) = struct.unpack("<BBBBHBHBHQh", byte_array)

            # Calculate battery voltage
            batt = 1.6 + (digi_batt / 256) * 2.4

            protocol_version = byte_array[0] & 0x0F

            time_since_conn = byte_array[3] & 0x0F

            error_code = (byte_array[3] >> 4) & 0x0F

            bits_decode = struct.unpack_from("<H", byte_array, 7)[0]

            digital_temp_c = (bits_decode >> 11) & 0x1F
            temp_c = -40 + (float(digital_temp_c) / 32) * (85 - -40)

            reset_reason = (bits_decode >> 6) & 0x1F

            boot_count = bits_decode & 0x3F

            peripheral_asset_id = mac

        return (
            protocol_version,
            flags,
            reset_reason,
            tx_power_dbm,
            boot_count,
            temp_c,
            batt,
            fw_version,
            error_code,
            peripheral_asset_id,
        )
    except struct.error:
        return (None,) * len(schema2.fields)


# Register the UDF
parse_crux_adv_2_udf = udf(parse_crux_adv_2, schema2)

# COMMAND ----------

at11_dev = spark.sql(
    f"""
    select gateway_id, device_id, serial, org_id, time as first_association_time, (lead(time) over(partition by gateway_id order by time)) - 1 as last_association_time
    from (select gh.gateway_id, gh.device_id, g.serial, d.org_id, cast(round(cast(gh.`timestamp` as double)*1000, 0) as long) as time
        from productsdb.gateway_device_history as gh
        join productsdb.gateways as g
        on gh.gateway_id = g.id
        join productsdb.devices as d
        on gh.device_id = d.id
        where g.product_id = 172)
    """
)
at11_dev.createOrReplaceTempView("at11_devices")


# COMMAND ----------

# NOTE: The following queries have been commented out because kinesisstats.osdcruxproxylocationdebug
# and crux_proxy_location_debug have been removed and reserved. The new osdcruxdebug table has a
# different schema that doesn't include scanner_data or adv_data fields. These queries need to be
# rewritten to use the new data sources (e.g., osdcruxdebug, generic_proxy_readings_debug, or
# datastreams.crux_unaggregated_locations) with appropriate joins to get scanner location data.

# for metrics_date in datelist:
#     ## Protocol 1
#     df = spark.sql(
#         f"""
#     select c.date, c.time,
#         c.object_id as scanner_object_id,
#         c.org_id as scanner_org_id,
#         h.product_id as scanner_product_id,
#         c.value.proto_value.crux_proxy_location_debug.adv_data.mac as peripheral_asset_id,
#         g.serial as peripheral_serial,
#         d.org_id as peripheral_org_id,
#         d.device_id as peripheral_device_id,
#         c.value.proto_value.crux_proxy_location_debug.scanner_data.latitude_nd / 1000000000 as lat,
#         c.value.proto_value.crux_proxy_location_debug.scanner_data.longitude_nd / 1000000000 as lon,
#         c.value.proto_value.crux_proxy_location_debug.scanner_data.rssi_dbm as rssi,
#         c.value.proto_value.crux_proxy_location_debug.adv_data.org_id as bootCount,
#         c.value.proto_value.crux_proxy_location_debug.scanner_data.speed_milliknots * 1.15077945 / 1000 as scanner_speed_mph,
#         CAST(c.value.proto_value.crux_proxy_location_debug.adv_data.adv_packet_verbatim AS BINARY)  as adv_packet
#     from kinesisstats.osdcruxproxylocationdebug as c
#     left join hardware.gateways_heartbeat as h -- need to replace this with an extended version that accurately captures range
#     on h.object_id = c.object_id
#     and date between h.first_heartbeat_date and h.last_heartbeat_date
#     and c.time between h.first_heartbeat_time and h.last_heartbeat_time
#     join productsdb.gateways as g
#     on c.value.proto_value.crux_proxy_location_debug.adv_data.mac = g.id
#     join (select gateway_id, device_id, serial, org_id, first_association_time, coalesce(last_association_time, unix_timestamp()*1000) as last_association_time
#         from at11_devices) as d -- to find the right org and device_id
#     on c.value.proto_value.crux_proxy_location_debug.adv_data.mac = d.gateway_id
#     and g.serial = d.serial
#     and c.time between d.first_association_time and d.last_association_time
#     where c.date == '{metrics_date}'
#     and g.product_id = 172
#     and value.proto_value.crux_proxy_location_debug.adv_data.protocol_version == 1
#     and (value.proto_value.crux_proxy_location_debug.adv_data.uuid = 2
#         or value.proto_value.crux_proxy_location_debug.adv_data.uuid is null)
#     """
#     ).select("*")

#     # Apply the UDF to transform the `adv_packet` column into new columns
#     df_final_1 = df.withColumn(
#         "parsed_values", parse_crux_adv_1_udf(col("adv_packet"))
#     ).select("*", "parsed_values.*")

#     # Assuming you want to drop the original byte array column
#     df_final_1 = df_final_1.drop("adv_packet")
#     df_final_1 = df_final_1.drop("parsed_values")

#     df_final_1 = df_final_1.withColumn(
#         "boot_count", coalesce(df_final_1["boot_count"], df["bootCount"])
#     )
#     df_final_1 = df_final_1.drop("bootCount")

#     ## Protocol 2
#     df = spark.sql(
#         f"""
#     select c.date, c.time,
#         c.object_id as scanner_object_id,
#         c.org_id as scanner_org_id,
#         h.product_id as scanner_product_id,
#         c.value.proto_value.crux_proxy_location_debug.adv_data.mac,
#         c.value.proto_value.crux_proxy_location_debug.scanner_data.latitude_nd / 1000000000 as lat,
#         c.value.proto_value.crux_proxy_location_debug.scanner_data.longitude_nd / 1000000000 as lon,
#         c.value.proto_value.crux_proxy_location_debug.scanner_data.rssi_dbm as rssi,
#         c.value.proto_value.crux_proxy_location_debug.adv_data.org_id as bootCount,
#         c.value.proto_value.crux_proxy_location_debug.scanner_data.speed_milliknots * 1.15077945 / 1000 as scanner_speed_mph,
#         c.value.proto_value.crux_proxy_location_debug.adv_data.adv_interval_us,
#         cast(c.value.proto_value.crux_proxy_location_debug.adv_data.adv_packet_verbatim as binary) as adv_packet
#     from kinesisstats.osdcruxproxylocationdebug as c
#     left join hardware.gateways_heartbeat as h -- need to replace this with an extended version that accurately captures range
#     on h.object_id = c.object_id
#     and date between h.first_heartbeat_date and h.last_heartbeat_date
#     and c.time between h.first_heartbeat_time and h.last_heartbeat_time
#     where c.date == '{metrics_date}'
#     and c.date between h.first_heartbeat_date and h.last_heartbeat_date
#     and value.proto_value.crux_proxy_location_debug.adv_data.protocol_version == 2
#     and (value.proto_value.crux_proxy_location_debug.adv_data.uuid = 2
#         or value.proto_value.crux_proxy_location_debug.adv_data.uuid is null)
#     and c.value.proto_value.crux_proxy_location_debug.adv_data.mac is not null
#     and c.value.proto_value.crux_proxy_location_debug.adv_data.adv_packet_verbatim is not null
#     """
#     ).select("*")

#     # Apply the UDF to transform the `adv_packet` column into new columns
#     df_parsed = df.withColumn(
#         "parsed_values", parse_crux_adv_2_udf(col("adv_packet"), col("mac"))
#     ).select("*", "parsed_values.*")

#     # Assuming you want to drop the original byte array column
#     df_parsed = df_parsed.drop("adv_packet")
#     df_parsed = df_parsed.drop("parsed_values")
#     df_parsed.createOrReplaceTempView("protocol_2_parsed_data")

#     df_final_2 = spark.sql(
#         f"""
#     select c.*,
#         g.serial as peripheral_serial,
#         d.org_id as peripheral_org_id,
#         d.device_id as peripheral_device_id
#     from protocol_2_parsed_data as c
#     left join hardware.gateways_heartbeat as h -- need to replace this with an extended version that accurately captures range
#     on h.object_id = c.scanner_object_id
#     and date between h.first_heartbeat_date and h.last_heartbeat_date
#     and c.time between h.first_heartbeat_time and h.last_heartbeat_time
#     join productsdb.gateways as g
#     on c.peripheral_asset_id = g.id
#     join (select gateway_id, device_id, serial, org_id, first_association_time, coalesce(last_association_time, unix_timestamp()*1000) as last_association_time
#         from at11_devices) as d -- to find the right org and device_id
#     on c.peripheral_asset_id = d.gateway_id
#     and g.serial = d.serial
#     and c.time between d.first_association_time and d.last_association_time
#     where g.product_id = 172
#     and c.date between h.first_heartbeat_date and h.last_heartbeat_date
#     """
#     ).select("*")

#     dff = df_final_1.selectExpr(
#         "date",
#         "time",
#         "scanner_org_id",
#         "scanner_product_id",
#         "scanner_object_id",
#         "peripheral_device_id",
#         "peripheral_asset_id",
#         "peripheral_serial",
#         "peripheral_org_id",
#         "lat",
#         "lon",
#         "rssi",
#         "scanner_speed_mph",
#         "protocol_version",
#         "flags",
#         "reset_reason",
#         "tx_power_dbm",
#         "boot_count",
#         "adv_interval_us",
#         "cast(temp_c as float) as temp_c",
#         "batt/10 as batt",
#         "fw_version",
#         "null as error_code",
#     ).union(
#         df_final_2.select(
#             "date",
#             "time",
#             "scanner_org_id",
#             "scanner_product_id",
#             "scanner_object_id",
#             "peripheral_device_id",
#             "peripheral_asset_id",
#             "peripheral_serial",
#             "peripheral_org_id",
#             "lat",
#             "lon",
#             "rssi",
#             "scanner_speed_mph",
#             "protocol_version",
#             "flags",
#             "reset_reason",
#             "tx_power_dbm",
#             "boot_count",
#             "adv_interval_us",
#             "temp_c",
#             "batt",
#             "fw_version",
#             "error_code",
#         )
#     )

#     dff.write.option("replaceWhere", f"date = '{metrics_date}'").mode(
#         "overwrite"
#     ).format("delta").partitionBy("date").saveAsTable("hardware_analytics.at11_data")

# COMMAND ----------
