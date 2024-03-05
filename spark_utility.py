from pyspark.sql import SparkSession


def create_spark_session(app_name):
    """
    :param app_name: name of app
    :return: spark session
    """
    spark = SparkSession.builder \
        .master("local") \
        .appName(app_name) \
        .getOrCreate()

    return spark


def read(spark, path, schema=None):
    """
    Read data json files
    :param spark: spark session
    :param schema: schema of dataframe
    :param path: path of json file
    :return:
    """
    return spark.read.json(path, schema=schema)
