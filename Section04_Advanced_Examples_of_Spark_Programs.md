## Lecture 34 - [Activity] Find the Most Popular Movie

* Join: Attach movieNames with movieIDs
* Dictionary loaded in driver program
* Broadcast the object and retrieve dictionary from it!
* UDFs

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
import codecs

def loadMovieNames():
    movieNames = {}
    # Change this to PATH to your u.item file:
    with codecs.open("/Users/marshad/Desktop/SparkCourse/data/ml-100k/u.item", "r", encoding='ISO-8859-1', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

nameDict = spark.sparkContext.broadcast(loadMovieNames())

# create schema when reading u.data
schema = StructType([ \
                        StructField("userID", IntegerType(), True), \
                        StructField("movieID", IntegerType(), True), \
                        StructField("rating", IntegerType(), True), \
                        StructField("timestampuserID", LongType(), True)
                    ])

# Load up movie data as dataframe
moviesDF = spark.read.option("sep", "\t").schema(schema).csv("/Users/marshad/Desktop/SparkCourse/data/ml-100k/u.data")

movieCounts = moviesDF.groupBy("movieID").count()

# create a user-defined function to look up movie names from our broadcasted dictionary
def lookupName(movieID):
    return nameDict.value[movieID]

lookupNameUDF = func.udf(lookupName)

# Add a movieTitle column using our new udf
moviesWithNames = movieCounts.withColumn("movieTitle", lookupNameUDF(func.col("movieID")))

# Sort the results
sortedMoviesWithNames = moviesWithNames.orderBy(func.desc("count"))

# Grab the top 10
sortedMoviesWithNames.show(10, False)

# Stop the session
spark.stop()
```

***

## Lecture 35 -- [Activity] Use Broadcast Variables to Display Movie Names Instead of ID Numbers

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StrudctField, IntergerType, StringType

spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

schema = StructType([\
                        StructField("id", IntegerType(), True),
                        StructField("name", StringType(), True)
                    ])

name = spark.read.schema(schema).option("sep", " ").csv("/Users/marshad/Desktop/SparkCourse/data/Marvel-names.txt")

lines = spark.read.text("/Users/marshad/Desktop/SparkCourse/data/Marvel-graph.txt")

connections = lines.withColumn("id", func.split(func.trim(func.col("value"), " ")[0] \
.withColumn("connections", func.size(func.split(func.trim(func.col("value"), " ")) - 1) \
.groupBy("id").agg(func.sum("connections").alias("connections"))

mostPopular = connections.sort(func.col("connections").desc()).first()

mostPopularName = names.filter(func.col("id") == mostPopular[0]).select("name").first()

print(mostPopulrName[0] + " is the most popular superhero with " + str(mostPopular[1]) + " co-appearances.")
```
***

## Lecture 36 - Find the Most Popular Superhero in a Social Graph

***

## Lecture 37 - [Activity] Run the Script - Discover Who the Most Popular Superhero is!

***

## Lecture 38 - [Exercise] Find the Most Obscure Superheroes

***

## Lecture 39 - Exercise Solution: Most Obscure Superheroes

***

## Lecture 40 - Superhero Degrees of Separation: Introducing Breadth-First Search
* An accumulator allows many executors to increment a shared variable

***

## Lecture 41 - Superhero Degrees of Separation: Accumulators, and Implementing BFS in Spark

***

## Lecture 42 - [Activity] Superhero Degrees of Separation: Review the Code and Run it

***

## Lecture 43 - Item-Based Collaborative Filtering in Spark, cache(), and persist()

***

## Lecture 44 - [Activity] Running the Similar Movies Script using Spark's Cluster Manager

***

## Lecture 45 - [Exercise] Improve the Quality of Similar Movies

***
