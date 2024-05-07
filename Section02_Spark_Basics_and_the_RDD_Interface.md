## Lecture 10

***

## Lecture 11

* Resilient Distributed Dataset (`RDD`) object
* Developer uses RDD object for data manipulation

* The Spark shell creates a "`sc = SparkContext()` object for you
```
sc = SparkContext(conf = conf)
```

### RDD operations

1. `map()`
2. `flatMap()`  # multiple results per original entry
3. `filter()`
4. `distinct()`
5. `sample()`
6. `union()`
7. `intersection()`
8. `subtract()`
9. `cartesian()`

### MAP Example

```python
rdd = sc.parallelize([1, 2, 3, 4])
rdd.map(lambda x: x*x)

def squareIt(x):
   return x*x

rdd.map(squareIt)
```

### Actions on RDD
1. `collect()`
2. `count()`
3. `countByValue()` (break down by unique value)
4. take
5. top
6. `reduce()`: (key, value based summation)
7. and more

* Lazy Evaluation!!!

***

## Lecture 12

***

## Lecture 13

* We can put complex structures like `(key, value)` pairs inside RDD. 
    * Then we can treat it like a simple DB.

### `(key, value)` RDDs
1. `reduceByKey()`
2. `GroupByKey()`
3. `sortByKey()`
4. `keys()`
5. `values()`
6. `join()`
7. `rightOuterJoin()`
8. `leftOuterJoin()`
9. `cogroup()`
10. `subtractByKey()`

* `mapValues()` vs `flatMapValues()`    # if your transformation doesn't affect the keys.

***

## Lecture 14

```python
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

## Lecture 15

```python
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

## Lecture 16

***

## Lecture 17

```python
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

## Lecture 18

* `map()` 
    * (1-1 RDD)
* `flatMap()` 
    * (1-many RDD)

```python
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

## Lecture 19

***

## Lecture 20

```python
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

## Lecture 21

***

## Lecture 22

```python
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

* RDD -> DataFrame Object

* DataFrames (like Big Database table):
	* Contain ROW objets
	* Can run SQL queries
	* Can have a schema
	* Read / Write to JSON, Hive, parquet, ...
	* Communicate with JDBC / ODBC, Tableau...

### SparkSession vs SparkContext

***

## Lecture 23

***

## Lecture 24

***