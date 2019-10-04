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

# Pull a sample of the dataset into an in-memory
# Pandas dataframe
flight_df_sampled = flight_df.na.drop().sample(False, 0.00005) #this limit is here for the demo
flight_df_local = flight_df_sampled.toPandas()

# Put the data into the array format required by tpot.
# Use one-hot encoding for the categorical variables
tpot_X = np.vstack([
  np.asarray(pd.get_dummies(flight_df_local["OP_CARRIER"])).transpose(),
  np.asarray(pd.get_dummies(flight_df_local["ORIGIN"])).transpose(),
  np.asarray(pd.get_dummies(flight_df_local["DEST"])).transpose(),
  np.asarray([flight_df_local["DISTANCE"]]),
  np.asarray([flight_df_local["CRS_DEP_TIME"]]).astype('float').astype('int')
]).transpose()
tpot_y = flight_df_local["CANCELLED"].astype("bool")

# Use tpot to select and tune a prediction algorithm
from tpot import TPOTClassifier

tpot = TPOTClassifier(generations=5, population_size=20, verbosity=2)
classifier = tpot.fit(tpot_X, tpot_y)

# Export the best performing algorithm and parameter set
# to Python code
classifier.export('exported_classifier.py')