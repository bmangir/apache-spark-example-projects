from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, LongType, IntegerType

from spark_utility import create_spark_session, read_csv

spark = create_spark_session("spotify")

#schema = StructType([
#    StructField('id', StringType(), nullable=False),
#    StructField('name', StringType(), nullable=False),
#    StructField('artists', ArrayType(StringType()), nullable=False),
#    StructField('duration_ms', LongType(), nullable=False),
#    StructField('release_date', StringType(), nullable=True),
#    StructField('year', IntegerType(), nullable=True),
#    StructField('acousticness', S)
#])

spotify_df = read_csv(spark, 'data/spotify-data.csv')

print(spotify_df.count())
spotify_df.show()


