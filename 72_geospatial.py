# !pip3 install geopandas

from pyspark.sql import SparkSession
from sklearn.model_selection import train_test_split
import pandas as pd
import numpy as np

spark = SparkSession\
    .builder\
    .appName("Airline ML")\
    .config("spark.hadoop.fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")\
    .config("spark.executor.memory","16g")\
    .config("spark.executor.cores","4")\
    .config("spark.driver.memory","6g")\
    .config("spark.executor.instances","5")\
    .config("spark.hadoop.fs.s3a.metadatastore.impl","org.apache.hadoop.fs.s3a.s3guard.NullMetadataStore")\
    .config("spark.hadoop.fs.s3a.delegation.token.binding","")\
    .getOrCreate()

# Read the data into Spark
flight_df=spark.read.parquet(
  "s3a://ml-field/demo/flight-analysis/data/airline_parquet_2/",
)

trip_frequencies = flight_df.groupBy("ORIGIN", "DEST").count().toPandas()
origins = flight_df.select("ORIGIN").distinct()
dests = flight_df.select("DEST").distinct()
all_airports = origins.union(dests).distinct().toPandas()

airport_codes = pd.read_csv("airport-codes.csv")

def loc_of_coordinates(coordinates):
  return coordinates

def loc_of_code(airport_code):
  coords = airport_codes[airport_codes["ident"] == "K" + airport_code]["coordinates"].item()

locs_dict = {}
for airport in all_airports:
  locs_dict[airport] = loc_of_code(airport)
