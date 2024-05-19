## Lecture 25 - Introducing SparkSQL

* `SparkSQL`
	* Extends RDDs to `DataFrmae` object
 		* DataFrames have more automatic optimization than with RDD's 
	* `DataFrames`:
		* Cotain `Row` objets
		* Can have a `Schema` leading to more efficient storage
   		* Can run `SQL queries`
		* Read / Write to JSON, Hive, parquet, CSV, ...
		* Communicate with JDBC / ODBC, Tableau, ...
  	* `DataFrame` is just a DataSet of Row objects (DataSet[Row])
  		* DataSets can explicity wrap a given struct or type (`DataSet[Person]`, `DataSet[(String, Double)]`)
  		* It knows whats its colums are from the get-go
  	 * `DataFrames` schema is inferred at runtime; but a `DataSet` can be inferred at compile time
  	 	* Faster detection of errors, and better optimization
  	  	* `DataSets` can only be used in compiled languages (Java, Scala - sorry Python)
  	   * RDDs can be converted to DataSets with `.toDS()`
***

* Create `SparkSession` object instead of a `SparkContext` when using Spark SQL / DataSets
	* You can get a `SparkContext` from this session, and use to issue SQL queries on your DataSets
	* Stop the sesssion once you're done

***

## Lecture 26 - [Activity] Executing SQL commands and SQL-style functions on a DataFrame

* File `spark-sql.py`
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

# Convert 'people' rdd into a DataFrame (Set of Row objects), infer the Schema
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

## Lecture 27 - Using DataFrames instead of RDD's

* File `spark-sql-dataframe.sql`
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

# We've a header row; Infer the Schema
people = spark.read.option("header", "true").option("inferSchema", "true") \
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

## Lecture 28 - [Exercise] Friends by Age, with DataFrames

***

## Lecture 29 - Exercise Solution: Friends by Age, with DataFrames

* File `friends-by-age-dataframe.py`

```python
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

# DataFrame lines
lines = spark.read.option("header", "true") \
			.option("inferSchema", "true") \
			.csv("/Users/marshad/Desktop/SparkCourse/data/fakefriends-header.csv")

# select only age and numFriends columns
friendsByAge = lines.select("age", "friends")

# from friendsByAge we group by "age" and then compute average
friendsByAge.groupBy("age").avg("friends").show()

# sorted
friendsByAge.groupBy("age").avg("friends").sort("age").show()

# formatted more nicely
friendsByAge.groupBy("age").agg(func.round(func.avg("friends"), 2)).sort("age").show()

# With a custom column name
friendsByAge.groupBy("age") \
	.agg(func.round(func.avg("friends"), 2) \
	.alias("friends_avg")).sort("age").show()

spark.stop()
```

***

## Lecture 30 - [Activity] Word Count, with DataFrames

* File `spark-submit word-count-better-sorted.py`

```python
import re
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

# 
def normalizeWords(text):
	return re.compile(r'\W+', re.UNICODE).split(text.lower());

input = sc.textFile("/Users/marshad/Desktop/SparkCourse/data/Book")
# passing normalizeWords function as a parameter
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

## Lecture 31 - [Activity] Minimum Temperature, with DataFrames (using a custom schema)

* File `min-termperatures-dataframe.py`

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

# read file as df dataframe
df = spark.read.schema(schema).csv('/Users/marshad/Desktop/SparkCourse/data/1800.csv')
df.printSchema()

# filter out all but TMIN entries
minTemps = df.filter(df.measure_type == "TMIN")

# select only stationID and temperature
stationTemps = minTemps.select("stationID", "temperature")

# aggregate to find minimum temperatures for every station
minTempsByStation = stationTemps.groupBy("stationID").min("temperature")
minTempsByStation.show()

# convert temperature to Fahrenheit and sort the dataset
minTempsBYStationF = minTempsBYStation.withColumn("temperature", \
	func.round(func.col("min(temperature)") * 0.1 * (9.0 / 5.0) + 32.0, 2)) \
	.select("stationID", "temperature").sort("temperature")

# collect, format, and print the results
results = minTempsBYStationF.collect()

for result in results:
	print(result[0] + "\t{:.2f}F".format(result[1]))

spark.stop()
```

***

## Lecture 32 - [Exercise] Implement Total Spent by Customer with DataFrames

***

## Lecture 33 - Exercise Solution: Total Spent by Customer, with DataFrames

* File `total-spent-by-customer-sorted-dataframe.py`

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType 

spark = SparkSession.builder.appName("TotalSpentByCustomer").master("local[*]").getOrCreate()

# create the schema when reading customer-orders
customerOrderSchema = StructType([\
                                    StructField("cust_id", IntegerType(), True),\
                                    StructField("item_id", IntegerType(), True),\
                                    StructField("amount_spent", FloatType(), True)
                                ])

# Load up the data into customerDF DataFrame
customerDF = spark.read.schema(customerOrderSchema) \
                .csv('/Users/marshad/Desktop/SparkCourse/data/customer-orders.csv')

totalByCustomer = customerDF.groupBy("cust_id")\
        .agg(func.round(func.sum("amount_spent"), 2).alias("total_spent"))

totalByCustomerSorted = totalByCustomer.sort("total_spent")

totalByCustomerSorted.show(totalByCustomerSorted.count())

spark.stop()
```

***
