## Lecture 25 - Introducing SparkSQL

* `SparkSQL`
	* `DataFrames` (Set of `Row` objects):
		* Contain `Row` objets
		* Can run SQL queries
		* Can have a schema
		* Read / Write to JSON, Hive, parquet, CSV, ...
		* Communicate with JDBC / ODBC, Tableau...
	* DateSet
 		* ...
   * `SparkSession` versus `SparkContext`
***

```python
from pyspark.sql import SparkSession
from pyspark.sql import Row

# Create a SparkSession
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

# mapper function returning `Row(ID, name, age, numFriends)` object
def mapper(line):
	fields = line.split(',')
	return Row(	ID=int(fields[0]), \
			name=str(fields[1].encode("utf-8")), \
        		age=int(fields[2]), \
			numFriends=int(fields[3]))

lines = spark.sparkContext.textFile('/Users/marshad/Desktop/SparkCourse/data/fakefriends.csv')
# pass mapper function as a parameter
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

## Lecture 26 - [Activity] Executing SQL commands and SQL-style functions on a DataFrame

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

people = spark.read.option("header", "true").option("inferschema", "true") \
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

## Lecture 27 - Using DataFrames instead of RDD's

***

## Lecture 28 - [Exercise] Friends by Age, with DataFrames

```python
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

***

## Lecture 29 - Exercise Solution: Friends by Age, with DataFrames

```python
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

## Lecture 30 - [Activity] Word Count, with DataFrames

```python
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

## Lecture 31 - [Activity] Minimum Temperature, with DataFrames (using a custom schema)

***

## Lecture 32 - [Exercise] Implement Total Spent by Customer with DataFrames

***

## Lecture 33 - Exercise Solution: Total Spent by Customer, with DataFrames

```python
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
