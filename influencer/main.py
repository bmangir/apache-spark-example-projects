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


def get_top_n_all_channels(df, n) -> DataFrame:
    """
    # Find the top n YouTube channels for each category
    :param df: dataframe
    :param n: number that find the tops
    :return:
    """

    for c in categories:
        if isinstance(c["category"], str):
            df_filtered = df.where(col("category") == c["category"]).limit(n)
        else:
            df_filtered = df.where(col("category").isNull()).limit(n)

        # If it's the first iteration, create the union_df
        if 'union_df' not in locals():
            union_df = df_filtered
        else:
            union_df = union_df.union(df_filtered)

    return union_df


def get_top_n_channels_by_category(df, category, n) -> DataFrame:
    if category is None:
        return df.where(col("category").isNull()).limit(n)

    return df.where(col("category") == category).limit(n)


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

# Find the top 5 channels for each category
top_channels_for_each_category = get_top_n_all_channels(ranked_by_category, 5)
top_channels_for_each_category.show()

# Find the top 10 channels in Music category
top_music_channels = get_top_n_channels_by_category(ranked_by_category, "Music", 10)
top_music_channels.show()
