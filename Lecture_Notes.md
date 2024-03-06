## Lecture 01

PATH: /usr/local/Cellar/apache-spark/3.3.0/bin

Install URL: https://sundog-education.com/spark-python/

http://localhost:4040

***

## Lecture 02

***

## Lecture 03

***

## Lecture 04

***

## Lecture 05
```
$ cd /usr/local/Cellar/apache-spark/3.3.0/bin
$ pyspark
$ rdd = sc.textFile(“README.md”)
$ rdd.count()
$ quit()
```
***

## Lecture 06

***

## Lecture 07

***

## Lecture 08

***

## Lecture 08
```
from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("RatingHistogram")
sc = SparkContext(conf = conf)

lines = sc.textFile("/Users/marshad/Desktop/SparkCourse/ml-100k/u.data")
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

soertedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
	print("%s, %i" % (key, value))
```

***

# Section 2: Spark Basics and the RDD Interface

## Lecture #9

### Deprecate

* Deprecate Python2 --> Python3
* Deprecate old MLLib (based on rdd interface) -> Data Frame / Data Set based

### Faster & Better Performance

* Spark3 is 17 times faster than Spark2
	* i) Adaptive execution,
	* ii) dynamic-partition pruning

* Better kubernetes integration (dynamic scaling)
* Deep Learning: Take advantage of GPUs clusters MLSpark / TensorFlow
* SparkGraph: Cypher query language (property graph model)
* Data Lake ACID support Delta Lake
* Binary File support

***

## Lecture #10

***

## Lecture #11
* RDD: Resilient Distributed "Dataset" object
* Developer uses RDD object for data manipulation

* The Spark shell creates a "sc" SparkContext object for you
```
sc = SparkContext(conf = conf)
```

### RDD operations
1. map
2. flatmap: multiple results per original entry
3. filter
4. distinct
5. sample
6. union, intersection, subtract, cartesian

### MAP Example:
```
rdd = sc.parallelize([1, 2, 3, 4])
rdd.map(lambda x: x*x)

Output: 1, 4, 9, 16

def squareIt(x):
   return x*x

rdd.map(squareIt)
```

### Actions on RDD
1. collect
2. count
3. countByValue (break down by unique value)
4. take
5. top
6. reduce (key, value based summation)
7. and more

* Lazy Evaluation!!!

***

## Lecture #13

* We can put complex structures like (key, value) pairs inside RDD. Then we can treat it like a simple DB.

### key / value RDDs
1. reduceByKey()
2. GroupByKey()
3. sortByKey()
4. keys()
5. values()

1. join()
2. rightOuterJoin()
3. leftOuterJoin()
4. cogroup()
5. subtractByKey()

* mapValues() / flatMapValues() -- if your transformation doesn't affect the keys.

***

## Lecture #14
```
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

def parseLine(line):
   fields = line.split(',')
   age = int(fields[2])
   numFriends = int(fields[3])
   return (age, numFriends)

lines = sc.textFile("/Users/marshad/Desktop/SparkCourse/fakefriends.csv")
rdd = lines.map(parseLine)
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
averageByAge = totalByAge.mapValues(lambda x: x[0] / x[1])
results = averageByAge.collect()
for result in results:
   print(result)
```

*** 

## Lecture #15
```
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

def parseLine(line):
   fields = line.split(',')
   stationID = fields[0]
   entryType = fields[2]
   temperature = float(fields[3])*0.1*(9.0 / 5.0) + 32.0
   return (stationID, entryType, temperature)

lines = sc.textFile("/Users/marshad/Desktop/SparkCourse/1800.csv")
parsedLines = lines.map(parseLine)
minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])
stationTemps = minTemps.map(lambda x: (x[0], x[2]))
minTemps = stationTemps.reduceByKey(lambda x, y: min(x, y))
results = minTemps.collect()

for result in results:
   print(result[0] + "\t{:.2f}F".format(result[1]))
```

***

## Lecture #17
```
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MaxTemperatures")
sc = SparkContext(conf = conf)

def parseLine(line):
   fields = line.split(',')
   stationID = fields[0]
   entryType = fields[2]
   temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
   return (stationID, entryType, temperature)

lines = sc.textFile("/Users/marshad/Desktop/SparkCourse/data/1800.csv")
parsedLines = lines.map(parseLine)
maxTemps = parsedLines.filter(lambda x: "TMAX" in x[1])
stationTemps = maxTemps.map(lambda x: (x[0], x[2]))
maxTemps = stationTemps.reduceByKey(lambda x, y: max(x, y))
results = maxTemps.collect()

for result in results:
   print(result[0] + "\t{:.2f}F".format(result[1]))
```

***

## Lecture #18

* MAP (1-1 RDD) vs flatMAP (1-many RDD)

```
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)
         
input = sc.textFile("/Users/marshad/Desktop/SparkCourse/data/Book")
words = input.flatMap(lambda x: x.split())
wordCounts = words.countByValue()

for word, count in wordCounts.items():
   cleanWord = word.encode('ascii', 'ignore')
   if (cleanWord):
      print(cleanWord.decode() + " " + str(count))
```

***

## Lecture #20
```
import re
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

def normalizeWords(text):
   return re.compile(r'\W+', re.UNICODE).split(text.lower());

input = sc.textFile("/Users/marshad/Desktop/SparkCourse/data/Book")
words = input.flatMap(normalizeWords)

wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x+y)
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()
results = wordCountsSorted.collect()

for result in results:
   count = str(result[0])
   word = result[1].encode('ascii', 'ignore')
   if (word):
      print(word.decode() + ":\t\t" + count)
```

***

## Lecture #22
```
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("SpendByCustomer")
sc = SparkContext(conf = conf)

def extractCustomerPricePairs(line):
   fields = line.split(',')
   return (int(fields[0]), float(fields[2]))

input = sc.textFile("/Users/marshad/Desktop/SparkCourse/data/customer-orders.csv")
mappedInput = input.map(extractCustomerPricePairs)
totalByCustomer = mappedInput.reduceByKey(lambda x, y: x + y)

results = totalByCustomer.collect()
for result in results:
   print(result)
```

***

# Section 3: SparkSQL, DataFrames, and DataSets

* RDD -> DataFrame Object

* DataFrames (like Big Database table):
	* Contain ROW objets
	* Can run SQL queries
	* Can have a schema
	* Read / Write to JSON, Hive, parquet, ...
	* Communicate with JDBC / ODBC, Tableau...

* SparkSession vs SparkContext

***

## Lecture #25
```
from pyspark.sql import SparkSession
from pyspark.sql import Row

# Create a SparkSession
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

def mapper(line):
    fields = line.split(',')
    return Row(ID=int(fields[0]), name=str(fields[1].encode("utf-8")), \
               age=int(fields[2]), numFriends=int(fields[3]))

lines = spark.sparkContext.textFile("/Users/marshad/Desktop/SparkCourse/data/fakefriends.csv")
people = lines.map(mapper)

# Infer the schema, and register the DataFrame as a table.
schemaPeople = spark.createDataFrame(people).cache()
schemaPeople.createOrReplaceTempView("people")

# SQL can be run over DataFrames that have been registered as a table.
teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

# The results of SQL queries are RDDs and support all the normal RDD operations.
for teen in teenagers.collect():
  print(teen)

# We can also use functions instead of SQL queries:
schemaPeople.groupBy("age").count().orderBy("age").show()

spark.stop()
```

***

## Lecture #26
```
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

people = spark.read.option("header", "true").option("inferschema", "true")\
        .csv("/Users/marshad/Desktop/SparkCourse/data/fakefriends-header.csv")

print("Here is our inferred schema:")
people.printSchema()

print("Let' display the name column")
people.select("name").show()

print("Filter out anyone over 21:")
people.filter(people.age < 21).show()

print("Group by age")
people.groupBy("age").count().show()

print("Make everyone 10 years older:")
people.select(people.name, people.age + 10).show()

spark.stop()
```
***

## Lecture #28
```
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

lines = spark.read.option("header", "true").option("inferschema", "true").csv("/Users/marshad/Desktop/SparkCourse/data/fakefriends-header.csv")

# select only age and numFriends columns
friendsByAge = lines.select("age", "friends")

# from friendsByAge we group by "age" and then compute average
friendsByAge.groupBy("age").avg("friends").show()

# sorted
friendsByAge.groupBy("age").avg("friends").sort("age").show()

# formatted more nicely
friendsByAge.groupBy("age").agg(func.round(func.avg("friends"), 2)).sort("age").show()

print("Make everyone 10 years older:")
friendsByAge.groupBy("age").agg(func.round(func.avg("friends"), 2).alias("friends_avg")).sort("age").show()

spark.stop()
```

## Lecture #30
```
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("MinTemperatures").getOrCreate()

schema = StructType([\
                        StructField("stationID",    StringType(),  True),\
                        StructField("date",         IntegerType(), True),\
                        StructField("measure_type", StringType(),  True),\
                        StructField("temperature",  FloatType(),   True)
                    ])

# // read file as dataframe
df = spark.read.schema(schema).csv("/Users/marshad/Desktop/SparkCourse/data/1800.csv")
df.printSchema()

# filter out all but TMIN entries
minTemps = df.filter(df.measure_type == "TMIN")

# select only stationID and temperature
stationTemps = minTemps.select("stationID", "temperature")

# aggregate to find minimum temperatures for every station
minTempsByStation = stationTemps.groupBy("stationID").min("temperature")
minTempsByStation.show()

# convert temperature to Fahrenheit and sort the dataset
minTempsBYStationF = minTempsBYStation.withColumn("temperature",
                                                func.round(func.col("min(temperature)") * 0.1 * (9.0 / 5.0) + 32.0, 2))\
                                                .select("stationID", "temperature").sort("temperature")

# collect, format, and print the results
results = minTempsBYStationF.collect()

for result in results:
   print(result[0] + "\t{:.2f}F".format(result[1]))

spark.stop()
```
***

## Lecture #32
```
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType 

spark = SparkSession.builder.appName("TotalSpentByCustomer").master("local[*]").getOrCreate()

# create the schema when reading customer-orders
customerOrderSchema = StructType([\
                                StructField("cust_id", IntegerType(), True),
                                StructField("item_id", IntegerType(), True),
                                StructField("amount_spent", FloatType(), True)
                                ])

# Load up the data into spark
customerDF = spark.read.schema(customerOrderSchema).csv("/Users/marshad/Desktop/SparkCourse/data/customer-orders.csv")

totalByCustomer = customerDF.groupBy("cust_id").agg(func.round(func.sum("amount_spent"), 2).alias("total_spent"))

totalByCustomerSorted = totalByCustomer.sort("total_spent")

totalByCustomerSorted.show(totalByCustomerSorted.count())

spark.stop()
```
***

# Section 4: Advanced Examples of Spark Programs

## Lecture #33
```
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType

spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

schema = StructType([ \
                        StructField("userID", IntegerType(), True), \
                        StructField("movieID", IntegerType(), True), \
                        StructField("rating", IntegerType(), True), \
                        StructField("timestampuserID", LongType(), True)
                ])

# Load up movie data as dataframe
moviesDF = spark.read.option("sep", "\t").schema(schema).csv("/Users/marshad/Desktop/SparkCourse/data/ml-100k/u.data")

# some SQL-style magic to sort all movies by popularity in one line!
topMovieIDs = moviesDF.groupBy("movieID").count().orderBy(func.desc("count"))

# Grab the top 10
topMovieIDs.show(10)

# Stop the session
spark.stop()
```

***

## Lecture #34

* Join: Attach movieNames with movieIDs
* Dictionary loaded in driver program
* Broadcast the object and retrieve dictionary from it!
* UDFs

```
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
            StructField("timestampuserID", LongType(), True)])

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

## Lecture #35
```
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StrudctField, IntergerType, StringType

spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

schema = StructType([\
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True)])

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

## Lecture #40

* An accumulator allows many executoers to increment a shared variable

***

# Section #6: Machine Learning with Spark ML

***

# Section #7: Spark Streaming, Structured Streaming, and GraphX
