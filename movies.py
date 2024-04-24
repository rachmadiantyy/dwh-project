from pyspark.sql import SparkSession
from pyspark.sql import functions as func

#Create a Sparksession
spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

# Load up movie data as dataframe
dataframe = spark.read.option("header",True).csv("rating.csv")

# SQL operations on dataframe to get the popular Movies
topMovieIds = dataframe.groupBy("movieID").count().orderBy(func.desc("count"))

# Grab the top 8
topMovieIds.show(8)

# Stop the session
spark.stop()