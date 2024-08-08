from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from spark_utility import read_csv, create_spark_session


def read_youtuber_details(path) -> DataFrame:
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

    df = find_coefficient_factor(df)

    return df


def find_coefficient_factor(df) -> DataFrame:
    """
    coefficient = total video views / video count * 10^(-11)
    Find the coefficient where is not Null
    :param df: dataframe
    :return: updated dataframe with coefficient column
    """

    df = df \
        .withColumn("video_views", coalesce(col("video_views"), lit(0))) \
        .withColumn("video_count", coalesce(col("video_count"), lit(0))) \
        .withColumn("coefficient", round(col("video_views") / col("video_count") * col("subscribers") * pow(10, -11), 4)) \
        .where(col("coefficient").isNotNull())
# TODO: kubernetes
    return df


def rank_by_coefficient(df) -> DataFrame:
    """

    :param df:
    :return:
    """

    category_window = Window.partitionBy("category").orderBy(col("coefficient").desc())
    df = df.withColumn('cf_rnk', dense_rank().over(category_window))

    return df


def rank_by_subscribers(df) -> DataFrame:
    """

    :param df:
    :return:
    """

    category_window = Window.partitionBy("category").orderBy(col("subscribers").asc())
    df = df.withColumn('subs_rnk', dense_rank().over(category_window))

    return df


def get_top_n_all_channels(df, n) -> DataFrame:
    """
    Find the top N YouTube channels for each category.
    (If category is null, also rank them and display)
    :param df: dataframe
    :param n: number of top channels to retrieve for each category
    :return: dataframe with top N channels for each category
    """

    return df.where(col("cf_rnk") <= n)


def get_top_n_channels_by_category(df, n, category=None) -> DataFrame:
    if category is None:
        return df.where(col("category").isNull()).limit(n)

    return get_top_n_all_channels(df.where(col("category") == category.capitalize()), n)


# Create spark session
youtube_session = create_spark_session("Youtube Analysis")

# Read the input data
input_path = "data/most_subscribed_youtube_channels.csv"
ranked_youtubers = read_youtuber_details(input_path)

# Order by coefficient values for each category
ranked_by_category = rank_by_coefficient(ranked_youtubers)
ranked_by_category = rank_by_subscribers(ranked_by_category)

# Find the top n channels for each category
top_channels_for_each_category = get_top_n_all_channels(ranked_by_category, n=1)
top_channels_for_each_category.show()

# Find the top 10 channels in Music category
top_music_channels = get_top_n_channels_by_category(ranked_by_category, 10, "Sports").orderBy("cf_rnk")
top_music_channels.show()
