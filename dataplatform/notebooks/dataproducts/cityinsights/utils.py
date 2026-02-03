# Databricks notebook source
# MAGIC %python
# MAGIC from pyspark.sql.functions import *
# MAGIC from pyspark.sql.types import ArrayType, LongType, StringType
# MAGIC from delta.tables import *
# MAGIC import h3
# MAGIC import h3.api.basic_int as h3_int
# MAGIC import geopandas
# MAGIC import shapely
# MAGIC import geojson
# MAGIC import pandas as pd
# MAGIC import matplotlib.pyplot as plt
# MAGIC from datetime import datetime
# MAGIC import calendar
# MAGIC import numpy as np
# MAGIC import seaborn as sns
# MAGIC import math

# COMMAND ----------

# MAGIC %python
# MAGIC # H3 HELPERS
# MAGIC @udf("long")
# MAGIC def lat_lng_to_h3_int_udf(lat, lng, res):
# MAGIC   return h3_int.geo_to_h3(lat, lng, res)
# MAGIC
# MAGIC @udf("string")
# MAGIC def lat_lng_to_h3_str_udf(lat, lng, res):
# MAGIC   return h3.geo_to_h3(lat, lng, res)

# COMMAND ----------

# MAGIC %python
# MAGIC # H3 HELPERS
# MAGIC #NOTE: LINESTRING coordinates are in (long, lat)
# MAGIC def line_string_to_h3_int(geom, res):
# MAGIC   h3_indices = set()
# MAGIC   geom = shapely.wkt.loads(geom)
# MAGIC   if type(geom)==shapely.geometry.linestring.LineString:
# MAGIC     for (lng,lat) in geom.coords[:]:
# MAGIC       h3_idx = h3_int.geo_to_h3(lat,lng, res)
# MAGIC       h3_indices.add(h3_idx)
# MAGIC   elif type(geom)==shapely.geometry.multilinestring.MultiLineString:
# MAGIC       linestrings = list(geom)
# MAGIC       for linestring in linestrings:
# MAGIC         for (lng,lat) in linestring.coords[:]:
# MAGIC           h3_idx = h3_int.geo_to_h3(lat,lng, res)
# MAGIC           h3_indices.add(h3_idx)
# MAGIC   return list(h3_indices)
# MAGIC
# MAGIC line_string_to_h3_int_udf = udf(lambda geom, res: line_string_to_h3_int(geom,res), ArrayType(LongType()))
# MAGIC
# MAGIC def line_string_to_h3_str(geom, res):
# MAGIC   h3_indices = set()
# MAGIC   geom = shapely.wkt.loads(geom)
# MAGIC   if type(geom)==shapely.geometry.linestring.LineString:
# MAGIC     for (lng,lat) in geom.coords[:]:
# MAGIC       h3_idx = h3.geo_to_h3(lat,lng, res)
# MAGIC       h3_indices.add(h3_idx)
# MAGIC   elif type(geom)==shapely.geometry.multilinestring.MultiLineString:
# MAGIC       linestrings = list(geom)
# MAGIC       for linestring in linestrings:
# MAGIC         for (lng,lat) in linestring.coords[:]:
# MAGIC           h3_idx = h3.geo_to_h3(lat,lng, res)
# MAGIC           h3_indices.add(h3_idx)
# MAGIC   return list(h3_indices)
# MAGIC
# MAGIC line_string_to_h3_str_udf = udf(lambda geom, res: line_string_to_h3_str(geom,res), ArrayType(StringType()))
# MAGIC
# MAGIC def polygon_to_h3_int(geom, res):
# MAGIC   h3_indices = set()
# MAGIC   geom = shapely.wkt.loads(geom)
# MAGIC   if type(geom)==shapely.geometry.polygon.Polygon:
# MAGIC     for (lng, lat) in geom.exterior.coords[:]:
# MAGIC       h3_idx = h3_int.geo_to_h3(lat,lng, res)
# MAGIC       h3_indices.add(h3_idx)
# MAGIC     polygon_geojson= shapely.geometry.mapping(geom)
# MAGIC     h3_indices.update(list(h3_int.polyfill_geojson(polygon_geojson, res)))
# MAGIC   elif type(geom) == shapely.geometry.multipolygon.MultiPolygon:
# MAGIC     polygons = list(geom)
# MAGIC     for polygon in polygons:
# MAGIC       for (lng, lat) in polygon.exterior.coords[:]:
# MAGIC         h3_idx = h3_int.geo_to_h3(lat,lng, res)
# MAGIC         h3_indices.add(h3_idx)
# MAGIC       polygon_geojson= shapely.geometry.mapping(polygon)
# MAGIC       h3_indices.update(list(h3_int.polyfill_geojson(polygon_geojson, res)))
# MAGIC   return list(h3_indices)
# MAGIC
# MAGIC polygon_to_h3_int_udf = udf(lambda geom, res: polygon_to_h3_int(geom,res), ArrayType(LongType()))
# MAGIC
# MAGIC def polygon_to_h3_str(geom, res):
# MAGIC   h3_indices = set()
# MAGIC   geom = shapely.wkt.loads(geom)
# MAGIC   if type(geom)==shapely.geometry.polygon.Polygon:
# MAGIC     for (lng, lat) in geom.exterior.coords[:]:
# MAGIC       h3_idx = h3.geo_to_h3(lat,lng, res)
# MAGIC       h3_indices.add(h3_idx)
# MAGIC     polygon_geojson= shapely.geometry.mapping(geom)
# MAGIC     h3_indices.update(list(h3.polyfill_geojson(polygon_geojson, res)))
# MAGIC   elif type(geom) == shapely.geometry.multipolygon.MultiPolygon:
# MAGIC     polygons = list(geom)
# MAGIC     for polygon in polygons:
# MAGIC       for (lng, lat) in polygon.exterior.coords[:]:
# MAGIC         h3_idx = h3.geo_to_h3(lat,lng, res)
# MAGIC         h3_indices.add(h3_idx)
# MAGIC       polygon_geojson= shapely.geometry.mapping(polygon)
# MAGIC       h3_indices.update(list(h3.polyfill_geojson(polygon_geojson, res)))
# MAGIC   return list(h3_indices)
# MAGIC
# MAGIC polygon_to_h3_str_udf = udf(lambda geom, res: polygon_to_h3_str(geom,res), ArrayType(StringType()))

# COMMAND ----------

# MAGIC %python
# MAGIC # Linestring Geometry Helpers
# MAGIC def line_string_interp_points(geom):
# MAGIC   x = np.array(geom.xy[0])
# MAGIC   y = np.array(geom.xy[1])
# MAGIC
# MAGIC   indexes = x.argsort()
# MAGIC   y = y[indexes]
# MAGIC   x = np.sort(x)
# MAGIC
# MAGIC   #assuming either strictly increasing or strictly decreasing
# MAGIC   if not np.all(np.diff(x) > 0):
# MAGIC     x = x[::-1]
# MAGIC     y = y[::-1]
# MAGIC
# MAGIC   xvals = []
# MAGIC
# MAGIC   for i in range(len(x)-1):
# MAGIC     # we need to account for large y differences as well.
# MAGIC     # taking the larger of the 2
# MAGIC     point_count = np.max([math.ceil(np.abs((x[i+1] - x[i])) / 0.00008),math.ceil(np.abs((y[i+1] - y[i])) / 0.00008)])
# MAGIC     if point_count <= 1:
# MAGIC       point_count = 2
# MAGIC
# MAGIC     seg_x_vals = np.linspace(x[i], x[i+1], point_count)
# MAGIC     xvals.extend(seg_x_vals)
# MAGIC
# MAGIC   interp = np.interp(xvals, x, y)
# MAGIC   points = np.array((xvals, interp)).T
# MAGIC   return points
# MAGIC
# MAGIC def line_string_interp(geom):
# MAGIC   # 111 km in 1 degree longitude
# MAGIC   # res 12 is 0.009 km
# MAGIC   # 0.00008 longitude
# MAGIC   geom = shapely.wkt.loads(geom)
# MAGIC   if type(geom)==shapely.geometry.linestring.LineString:
# MAGIC      points = line_string_interp_points(geom)
# MAGIC   elif(type(geom)==shapely.geometry.multilinestring.MultiLineString):
# MAGIC     points = np.empty([0,2])
# MAGIC     linestrings = list(geom)
# MAGIC     for linestring in linestrings:
# MAGIC       p = line_string_interp_points(linestring)
# MAGIC       points = np.concatenate((points,p))
# MAGIC   return str(shapely.geometry.LineString(points))
# MAGIC
# MAGIC
# MAGIC line_string_interp_udf = udf(lambda geom: line_string_interp(geom), StringType())
# MAGIC
# MAGIC def line_string_to_polygon(line_string_geom, buffer_dist, res):
# MAGIC   line_string_geom = shapely.wkt.loads(line_string_geom)
# MAGIC   polygon_geom = line_string_geom.buffer(buffer_dist,res)
# MAGIC   return str(polygon_geom)
# MAGIC
# MAGIC line_string_to_polygon_udf = udf(lambda line_string_geom, buffer_dist, res: line_string_to_polygon(line_string_geom, buffer_dist, res), StringType())
# MAGIC
# MAGIC def linestring_to_gmap_url(linestring):
# MAGIC #   base_url = 'http://maps.google.com/maps?q={},{}'
# MAGIC   base_url = 'http://maps.google.com/maps?q=&layer=c&cbll={},{}'
# MAGIC   coords = linestring.split('(')[1].split(',')[0].split(' ')
# MAGIC   lng = coords[0]
# MAGIC   lat = coords[1]
# MAGIC   return base_url.format(lat,lng)
# MAGIC
# MAGIC linestring_to_gmap_url_udf = udf(lambda line_string_geom: linestring_to_gmap_url(line_string_geom), StringType())
# MAGIC _ = spark.udf.register("linestring_to_gmap_url_udf", linestring_to_gmap_url_udf)

# COMMAND ----------

# MAGIC %python
# MAGIC # Aggregation Helpers
# MAGIC @udf("long")
# MAGIC def snap_3_hour(ts, timezone):
# MAGIC   return int(pd.Timestamp(datetime.fromtimestamp(ts/1000), tz="UTC").tz_convert('US/{}'.format(timezone)).floor('3h').timestamp()*1000)
# MAGIC
# MAGIC @udf("long")
# MAGIC def snap_1_hour(ts, timezone):
# MAGIC   return int(pd.Timestamp(datetime.fromtimestamp(ts/1000), tz='UTC').tz_convert('US/{}'.format(timezone)).floor('1h', ambiguous=True).timestamp()*1000)
# MAGIC
# MAGIC @udf("long")
# MAGIC def snap_1_day(ts, timezone):
# MAGIC   return int(pd.Timestamp(datetime.fromtimestamp(ts/1000), tz='UTC').tz_convert('US/{}'.format(timezone)).floor('1d', ambiguous=True).timestamp()*1000)
# MAGIC
# MAGIC @udf("string")
# MAGIC def get_day(ts, timezone):
# MAGIC   return calendar.day_name[pd.Timestamp(datetime.fromtimestamp(ts/1000), tz='UTC').tz_convert('US/{}'.format(timezone)).dayofweek]
# MAGIC
# MAGIC @udf("string")
# MAGIC def get_month(ts, timezone):
# MAGIC   return calendar.month_name[pd.Timestamp(datetime.fromtimestamp(ts/1000), tz='UTC').tz_convert('US/{}'.format(timezone)).month]
# MAGIC
# MAGIC @udf("string")
# MAGIC def is_rush_hour(ts, timezone):
# MAGIC   hour = pd.Timestamp(datetime.fromtimestamp(ts/1000), tz='UTC').tz_convert('US/{}'.format(timezone)).hour
# MAGIC   if (hour >= 6 and hour <=9) or (hour >= 15 and hour <= 18):
# MAGIC     return 'y'
# MAGIC   return 'n'
# MAGIC
# MAGIC @udf("string")
# MAGIC def get_date(ts, timezone):
# MAGIC   return pd.Timestamp(datetime.fromtimestamp(ts/1000), tz='UTC').tz_convert('US/{}'.format(timezone)).strftime('%m-%d-%Y')
# MAGIC
# MAGIC @udf("string")
# MAGIC def get_post_covid(ts, timezone):
# MAGIC   date = pd.Timestamp(datetime.fromtimestamp(ts/1000), tz='UTC').tz_convert('US/{}'.format(timezone))
# MAGIC   return 1 if date > pd.Timestamp('2020-03-08', tz='UTC').tz_convert('US/{}'.format(timezone)) else 0

# COMMAND ----------

# MAGIC %python
# MAGIC # PLOTTING
# MAGIC # From Abhi's notebook
# MAGIC def plot_hist(data_sdf, col, prefix, bins=50):
# MAGIC   data_hist = data_sdf.select(col).rdd.flatMap(lambda x: x).histogram(bins)
# MAGIC   # Loading the Computed Histogram into a Pandas Dataframe for plotting
# MAGIC   hist_plot = pd.DataFrame(
# MAGIC       list(zip(*data_hist)),
# MAGIC       columns=['bin', 'frequency']
# MAGIC   ).round(decimals=3).set_index(
# MAGIC       'bin'
# MAGIC   ).plot(kind='bar',figsize=(20,10));
# MAGIC   hist_plot.set_title("{} {} Distribution".format(prefix, col))
# MAGIC   display(hist_plot)
