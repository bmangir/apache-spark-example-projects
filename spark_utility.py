from pyspark.sql import SparkSession, DataFrame


def create_spark_session(app_name) -> SparkSession:
    """
    :param app_name: name of app
    :return: spark session
    """
    spark = SparkSession.builder \
        .master("local") \
        .appName(app_name) \
        .getOrCreate()

    return spark


def read_json(spark, path, schema=None) -> DataFrame:
    """
    Read json data files
    :param spark: spark session
    :param schema: schema of dataframe
    :param path: path of json file
    :return:
    """
    return spark.read.json(path, schema=schema)


def read_csv(spark, path, schema=None) -> DataFrame:
    """
    Read csv data files
    :param spark: spark session
    :param schema: schema of dataframe
    :param path: path of csv file
    :return:
    """
    return spark.read.option("header", True).csv(path, schema=schema)
