import pyspark
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from spark_utility import read_csv, create_spark_session


def read_data(path) -> DataFrame:
    """
    Read the data and convert some columns string to integer.
    :param path: path of the input data
    :return: read data
    """

    df = read_csv(youtube_session, path)
    df = df \
        .withColumn("subscribers", regexp_replace("subscribers", ',', '').cast(LongType())) \
        .withColumn("video_views", regexp_replace("video_views", ',', '').cast(LongType())) \
        .withColumn("video_count", regexp_replace("video_count", ',', '').cast(LongType()))

    return df


def find_coefficient_factor(df) -> DataFrame:
    """
    coefficient = total video views / video count * 10^9
    Find the coefficient where is not Null
    :param df: dataframe
    :return: updated dataframe with coefficient column
    """

    df = df \
        .withColumn("coefficient", round(col("video_count") / col("video_views") * pow(10, 9), 4)) \
        .where(col("coefficient").isNotNull())

    return df


def rank_by_category(df) -> DataFrame:
    """

    :param df:
    :return:
    """

    category_window = Window.partitionBy("category").orderBy(col("coefficient").asc())
    df = ranked_youtubers.withColumn('rnk', rank().over(category_window))

    return df


def show_top_n_all_channels(df, n, categories):
    """
    # Find the top n YouTube channels for each category
    :param df: dataframe
    :param n: number that find the tops
    :param categories: category list
    :return:
    """
    for c in categories:
        if type(c["category"]) is str:
            df.where(col("category") == c["category"]).show(n)
        else:
            df.where(col("category").isNull()).show(n)


def get_top_n_channels(df, category, n) -> DataFrame:
    if col(category).isNull:
        df = df.where(col(category).isNull()).take(n)
    else:
        df = df.where(col("category") == category).take(n)

    return df

# Create spark session
youtube_session = create_spark_session("Youtube Analysis")

# Read the input data
input_path = "data/most_subscribed_youtube_channels.csv"
ranked_youtubers = read_data(input_path)
ranked_youtubers = find_coefficient_factor(ranked_youtubers)


# Order by coefficient values for each category
ranked_by_category = rank_by_category(ranked_youtubers)

# Find the distinct categories
categories = ranked_youtubers.select("category").distinct().collect()

show_top_n_all_channels(ranked_by_category, 5, categories)