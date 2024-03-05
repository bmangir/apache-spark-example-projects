from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from spark_utility import read, create_spark_session


def find_last_changes(cdc):
    """
    Find the last updates by timestamp of cdc data
    :param cdc: the dataframe that contains cdc data
    :return:
    """

    cdc_window = Window.partitionBy("id").orderBy(col("timestamp").desc())
    return cdc \
        .withColumn("rnk", rank().over(cdc_window)) \
        .filter(col("rnk") == 1) \
        .drop("rnk")


def find_latest_snapshot_data(current_snapshot_data, final_cdc_data):
    """
    Anti Join the two tables, previous snapshot data and latest cdc data version by anti-left join
    Union the joined table with latest cdc data
    :param current_snapshot_data: the dataframe of last version of snapshot data
    :param final_cdc_data: the final version of cdc data after finding the changes
    :return: the dataframe that contains updated snapshot data
    """
    return current_snapshot_data.join(final_cdc_data, current_snapshot_data.id == final_cdc_data.id, "left_anti")\
        .union(final_cdc_data)


spark_session = create_spark_session("snapshot_data")

# General scheme for initial, snapshot and cdc data
snapshot_scheme = StructType([
    StructField('id', IntegerType(), False),
    StructField('name', StringType(), True),
    StructField('category', StringType(), True),
    StructField('brand', StringType(), True),
    StructField('color', StringType(), True),
    StructField('price', DecimalType(), True),
    StructField('timestamp', LongType(), True)
])

# Take initial data as snapshot data
current_snapshot_df = read(spark_session, "data/initial_data.json", snapshot_scheme)

# Take Change Data Capture
cdc_data_df = read(spark_session, "data/cdc_data.json")

latest_cdc_data = find_last_changes(cdc_data_df)

latest_snapshot_df = find_latest_snapshot_data(current_snapshot_df, latest_cdc_data)
latest_snapshot_df.show()
