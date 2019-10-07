# !pip3 install geopandas

#Temporary workaround for MLX-975
#In utils/hive-site.xml edit hive.metastore.warehouse.dir and hive.metastore.warehouse.external.dir based on settings in CDP Data Lake -> Cloud Storage
if ( not os.path.exists('/etc/hadoop/conf/hive-site.xml')):
  !cp /home/cdsw/utils/hive-site.xml /etc/hadoop/conf/

#Data taken from http://stat-computing.org/dataexpo/2009/the-data.html
#!for i in `seq 1987 2008`; do wget http://stat-computing.org/dataexpo/2009/$i.csv.bz2; bunzip2 $i.csv.bz2; sed -i '1d' $i.csv; aws s3 cp $i.csv s3://ml-field/demo/flight-analysis/data/flights_csv/; rm $i.csv; done

from __future__ import print_function
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import Row, StructField, StructType, StringType, IntegerType

spark = SparkSession\
    .builder\
    .appName("GeoAnalysis")\
    .config("spark.executor.memory", "4g")\
    .config("spark.executor.instances", 5)\
    .config("spark.yarn.access.hadoopFileSystems","s3a://ml-field/demo/flight-analysis/data/")\
    .getOrCreate()

# Find the frequency of all trips (preserving direction)
trip_frequency = spark.sql("SELECT ORIGIN, DEST, COUNT(1), ORIGIN. AS cnt FROM flights GROUP BY ORIGIN, DEST").toPandas()

# Get the airports as a local dataframe
airports = spark.sql("SELECT * FROM airports").toPandas()

# Do this: http://vega.github.io/vega-editor/?spec=airports with altair

# Also have a variogram of proportion cancelled. 
# Do it with scikit-gstat maybe: https://mmaelicke.github.io/scikit-gstat/reference/variogram.html#skgstat.Variogram