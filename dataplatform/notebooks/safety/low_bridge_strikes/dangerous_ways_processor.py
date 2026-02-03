# Databricks notebook source
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import array, col, explode, lit, row_number
from pyspark.sql.types import (
    ArrayType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window


class DangerousWaysProcessor:
    """
    This class processes OSM and HERE Maps road data to identify connections between roads
    and calculate attributes such as distance and way segments in between.
    """

    schema_ways = StructType(
        [
            StructField("way_id", LongType(), True),
            StructField("nodes", ArrayType(LongType()), True),
            StructField("max_height", StringType(), True),
            StructField("length", FloatType(), True),
        ]
    )

    schema_nodes = StructType(
        [
            StructField("node_id", LongType(), True),
            StructField("lat_lng", StringType(), True),
        ]
    )

    schema_dangerous_ways_tagging = StructType(
        [
            StructField("way_id", LongType(), True),
            StructField("bridge_way_id", LongType(), True),
            StructField("bridge_node_id", LongType(), True),
            StructField("parent_node_id", LongType(), True),
            StructField("ways_away", LongType(), True),
            StructField(
                "parent_node_to_bridge_node_distance_meters", FloatType(), True
            ),
        ]
    )

    def __init__(
        self,
        spark: SparkSession,
        df_all_roads: DataFrame,
        df_nodes: DataFrame,
        df_nodes_lat_lng: DataFrame,
    ):
        self.spark = spark
        self.df_all_roads = df_all_roads
        self.df_nodes = df_nodes
        self.df_nodes_lat_lng = df_nodes_lat_lng

        self.connections_dataset_ordered = None

    def convert_bridge_ways_nodes_to_connections(self, bridge_way_to_node):
        """
        Prepares bridge way nodes in a format suitable for recursion and connections search,
        setting each node as its own connection with a ways_away of zero.
        """
        return bridge_way_to_node.alias("wtn").select(
            col("way_id"),
            col("way_id").alias("bridge_way_id"),
            col("node").alias("bridge_node_id"),
            col("node").alias("parent_node_id"),
            lit(0).alias("ways_away"),
            lit(0).alias("length"),
            lit(0).alias("parent_node_to_bridge_node_distance_meters"),
            col("wtn.node"),
            array().alias(
                "way_nodes"
            ),  # Nodes of way_id. We don't need to calculate length out of way nodes in the first pass, so leaving it empty is fine.
            array().alias("parent_way_nodes"),  # Bridge ways don't have parent ways.
            array().alias("way_nodes_coordinates"),
            array().alias("parent_way_nodes_coordinates"),
            col("exit_node"),
        )

    def find_connections(
        self,
        connections,
        ways_df,
        way_to_node,
        min_depth=3,
        min_length=100,
        max_depth=5,
    ):
        dataset = self.spark.createDataFrame([], self.schema_dangerous_ways_tagging)
        current_depth = 1

        while current_depth <= max_depth:
            connections = (
                connections.alias("dc")
                .join(
                    way_to_node.alias("wtn"),
                    (col("dc.node") == col("wtn.node"))
                    & (col("wtn.way_id") != col("dc.way_id"))
                    & (col("wtn.way_id") != col("dc.bridge_way_id"))
                    & (
                        col("dc.exit_node").isNull()
                        | (col("dc.exit_node") != col("dc.node"))
                    ),  # Prevent connections on exit nodes of one-way roads, since vehicles cannot enter these roads from the exit side
                )
                .join(
                    ways_df.alias("ways_df"), col("wtn.way_id") == col("ways_df.way_id")
                )
                .select(
                    col("wtn.way_id").alias("way_id"),
                    col("dc.bridge_way_id"),
                    col("dc.bridge_node_id"),
                    col("wtn.node").alias("parent_node_id"),
                    (col("dc.ways_away") + 1).alias("ways_away"),
                    col("dc.parent_node_id").alias("parent_way_parent_node"),
                    col("ways_df.nodes").alias("way_nodes"),
                    col("dc.way_nodes").alias("parent_way_nodes"),
                    col("ways_df.coordinates").alias("way_nodes_coordinates"),
                    col("dc.way_nodes_coordinates").alias("parent_way_coordinates"),
                    col("dc.parent_node_to_bridge_node_distance_meters"),
                    col("ways_df.exit_node").alias("exit_node"),
                    explode(col("ways_df.nodes")).alias("node"),
                )
            )

            if len(connections.take(1)) == 0:
                break

            connections = connections.withColumn(
                "final_nodes",
                get_subset_by_edges_udf(
                    col("parent_way_nodes"),
                    col("parent_node_id"),
                    col("parent_way_parent_node"),
                ),
            )

            connections = connections.withColumn(
                "final_nodes_lat_lng",
                get_lat_lng_from_visited_nodes_udf(
                    col("final_nodes"),
                    col("parent_way_nodes"),
                    col("parent_way_coordinates"),
                ),
            )

            connections = connections.withColumn(
                "parent_node_to_bridge_node_distance_meters",
                col("parent_node_to_bridge_node_distance_meters")
                + calculate_length_from_coordinates_udf(col("final_nodes_lat_lng")),
            )

            dataset = dataset.union(
                connections.select(
                    [
                        "way_id",
                        "bridge_way_id",
                        "bridge_node_id",
                        "parent_node_id",
                        "ways_away",
                        "parent_node_to_bridge_node_distance_meters",
                    ]
                )
            )

            # Prevent going back to the same nodes
            connections = connections.filter(
                (col("parent_node_id") != col("node"))
                & (col("bridge_node_id") != col("node"))
            )

            # Continue finding connections for networks with insufficient length if we have sufficient depth coverage.
            # We check if current_depth < max_depth to avoid filtering out during the last iteration.
            if current_depth >= min_depth and current_depth < max_depth:
                connections = connections.filter(
                    col("parent_node_to_bridge_node_distance_meters") < min_length
                )

            current_depth += 1

        dataset = dataset.distinct()
        return dataset

    def dedupe_dataset(self, dataset):
        """
        For each pair of way_id and bridge_way_id, this function returns the single record
        with the shortest parent_node_to_bridge_node_distance_meters when there are multiple bridge_nodes
        connecting the same way_id and bridge_way_id pair.

        The method uses tie-breakers (bridge_node_id and parent_node_id) to ensure deterministic
        selection when metrics are identical.

        NOTE: We shouldn't ultimately dedupe the dataset, since we want to keep all the connections.
        Evaluate the change and the impact on the final dataset in a subsequent ticket.
        """
        windowSpec = Window.partitionBy("way_id", "bridge_way_id").orderBy(
            col("parent_node_to_bridge_node_distance_meters").asc(),
            col("ways_away").asc(),
            col("bridge_node_id").asc(),
            col("parent_node_id").asc(),
        )

        dataset = (
            dataset.withColumn("rank", row_number().over(windowSpec))
            .filter(col("rank") == 1)
            .drop("rank")
        )

        return dataset

    def process_data(self, min_depth=3, min_length=100, max_depth=5):
        if min_depth > max_depth:
            raise ValueError("min_depth cannot be greater than max_depth")

        ways_df = self.df_all_roads.alias("ways_df")

        ways_df_with_max_height_meters = ways_df.withColumn(
            "max_height_meters", col("max_height")
        )

        way_to_node = ways_df_with_max_height_meters.select(
            col("way_id"),
            explode(col("nodes")).alias("node"),
            col("max_height_meters"),
            col("coordinates"),
            col("exit_node"),
        )

        bridge_way_to_node = way_to_node.filter(
            col("max_height_meters").isNotNull()
        ).alias("bridges")

        bridge_way_to_node_connections = self.convert_bridge_ways_nodes_to_connections(
            bridge_way_to_node
        )

        connections_dataset = self.dedupe_dataset(
            self.find_connections(
                bridge_way_to_node_connections,
                ways_df_with_max_height_meters,
                way_to_node,
                min_depth,
                min_length,
                max_depth,
            )
        )

        # Order results and cache to ensure deterministic behavior and consistent results
        # across all subsequent operations (nodes file generation, etc.)
        self.connections_dataset_ordered = connections_dataset.orderBy(
            col("ways_away").asc(),
            col("parent_node_to_bridge_node_distance_meters").asc(),
            col("way_id").asc(),
            col("bridge_way_id").asc(),
        ).cache()
        print(f"Processed {self.connections_dataset_ordered.count()} connections")

    def get_connections_nodes(self):
        all_nodes = (
            self.connections_dataset_ordered.select(
                col("bridge_node_id").alias("node_id")
            )
            .union(
                self.connections_dataset_ordered.select(
                    col("parent_node_id").alias("node_id")
                )
            )
            .distinct()
        )

        df_nodes = self.df_nodes.join(all_nodes, on="node_id", how="inner")

        nodes_with_lat_lng = df_nodes.join(
            self.df_nodes_lat_lng,
            (df_nodes.way_id == self.df_nodes_lat_lng.way_id)
            & (df_nodes.index == self.df_nodes_lat_lng.node_lat_lng["ind"]),
        )

        return (
            nodes_with_lat_lng.select(
                col("node_id"),
                col("node_lat_lng.latitude").alias("lat_degrees"),
                col("node_lat_lng.longitude").alias("lng_degrees"),
            )
            .distinct()
            .cache()
        )
