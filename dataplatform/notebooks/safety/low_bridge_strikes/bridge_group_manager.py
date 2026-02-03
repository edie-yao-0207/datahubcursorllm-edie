import networkx as nx
from pyspark.sql.types import (
    LongType,
    ArrayType,
)
from pyspark.sql.functions import explode, col, collect_set, udf
import itertools
import networkx as nx


class BridgeGroupManager:
    def __init__(self, spark_session, df_osm_maps_spark):
        self.spark = spark_session
        self.df_osm_maps_spark = df_osm_maps_spark

    def create_bridge_groups(self):
        all_roads = self.df_osm_maps_spark

        # Filter out ways where max_height is null
        filtered_df = all_roads.filter(col("max_height").isNotNull())

        # Explode the nodes array to have one node per row
        exploded_df = filtered_df.select(
            "way_id", "max_height", explode("nodes").alias("node")
        )

        # Group by node and max_height, collect way_ids
        grouped_df = exploded_df.groupBy("node", "max_height").agg(
            collect_set("way_id").alias("way_ids")
        )

        # Define a UDF to generate pairs of bridge way_ids
        def generate_pairs(way_ids):
            if len(way_ids) <= 1:
                return []
            else:
                pairs = []
                for combo in itertools.combinations(sorted(way_ids), 2):
                    pairs.append(list(combo))
                return pairs

        generate_pairs_udf = udf(generate_pairs, ArrayType(ArrayType(LongType())))

        # Apply the UDF to generate pairs
        pairs_df = grouped_df.select(
            explode(generate_pairs_udf("way_ids")).alias("pair")
        ).select(col("pair")[0].alias("way_id1"), col("pair")[1].alias("way_id2"))

        # Remove duplicate pairs
        unique_pairs_df = pairs_df.dropDuplicates()

        # Collect the pairs
        pairs = unique_pairs_df.collect()
        pairs = [(row["way_id1"], row["way_id2"]) for row in pairs]

        # Use NetworkX to assign group_ids to bridge ways
        # Get all bridge way_ids
        way_ids_rows = filtered_df.select("way_id").distinct().collect()
        way_ids = [row["way_id"] for row in way_ids_rows]

        # Create the NetworkX graph
        G = nx.Graph()
        G.add_nodes_from(way_ids)
        G.add_edges_from(pairs)

        # Find connected components
        connected_components = list(nx.connected_components(G))

        # Assign group_ids
        group_mapping = {}
        for component in connected_components:
            group_id = min(
                component
            )  # Assign group_id as the minimal way_id in the component
            for way_id in component:
                group_mapping[way_id] = group_id

        # Create a DataFrame from the group_mapping
        group_mapping_df = spark.createDataFrame(
            [(int(k), int(v)) for k, v in group_mapping.items()],
            ["way_id", "bridge_group_id"],
        )

        # Join back to all_roads
        return all_roads.join(group_mapping_df, on="way_id", how="left")
