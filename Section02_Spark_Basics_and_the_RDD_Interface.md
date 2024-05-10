## Lecture 10

### Deprecate

* Deprecate Python2 -> `Python3`
* Deprecate old MLLib (based on RDD interface)
    * `DataFrame` / DataSet based

### Faster & Better Performance

* `Spark3` versus `Sprak2` 
	* 17 times faster than Spark2
	* Adaptive execution
	* Dynamic-partition pruning

* Better k8s Integration
	* Dynamic scaling
* Deep Learning
	* Take advantage of GPUs clusters `MLSpark` / `TensorFlow`
* SparkGraph
	* Cypher query language (property graph model)
* Data Lake ACID support `DeltaLake`
* Binary File support

***

## Lecture 11
* Spark Core
	* `Spark Streaming`
	* `Spark SQL`
	* `MLLib`
	* `GraphX`

***

## Lecture 12
* `Resilient Distributed Dataset (RDD)` object
* Developer uses RDD object for data manipulation
* The Spark shell creates a `sc = SparkContext()` object
```
sc = SparkContext(conf = conf)
```

### RDD operations
1. `map()`
2. `flatMap()`  # multiple results per original entry
3. `distinct()`
4. `filter()`
5. `sample()`
6. `cartesian()`
7. `union()`
8. `intersection()`
9. `subtract()`

#### `map()` Example

```python
rdd = sc.parallelize([1, 2, 3, 4])
rdd.map(lambda x: x*x)


def squareIt(x):
	return x*x

# passing a function as a parameter
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

## Lecture 13 - Ratings Histogram Walkthrough

***

## Lecture 14 - `(Key, Value)` RDD's, and the Average Friends by Age Example

* We can put complex structures like `(key, value)` pairs inside RDD. 
	* Then we can treat it like a simple DB.

#### `(key, value)` RDDs
01. `rdd.reduceByKey(lambda x, y : x + y)`
02. `GroupByKey()`
03. `sortByKey()`
04. `keys()`
05. `values()`
06. `join()`
07. `rightOuterJoin()`
08. `leftOuterJoin()`
09. `cogroup()`
10. `subtractByKey()`

* `mapValues()` versus `flatMapValues()`    # If your transformation doesn't affect the keys.
* `Tuple: (ID, name, age, #friends)`

***

## Lecture 15. [Activity] Running the Average Friends by Age Example
* File `friends-by-age.py`
```python
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

def parseLine(line):
	fields = line.split(',')
	age = int(fields[2])
	numFriends = int(fields[3])
	return (age, numFriends)

lines = sc.textFile('/Users/marshad/Desktop/SparkCourse/data/fakefriends.csv')
# passing parseLine function as a parameter
rdd = lines.map(parseLine)
totalsByAge = rdd.mapValues(lambda x: (x, 1)) \
		 .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
averageByAge = totalByAge.mapValues(lambda x: x[0] / x[1])
results = averageByAge.collect()
for result in results:
	print(result)
```

*** 

## Lecture 16 - Filtering RDD's, and the Minimum Temperature by Location Example

***

## Lecture 17 - [Activity]Running the Minimum Temperature Example, and Modifying it for Maximums
* File `min-temperatures.py`
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

lines = sc.textFile('/Users/marshad/Desktop/SparkCourse/data/1800.csv')
# passing parseLine function as a parameter
parsedLines = lines.map(parseLine)
maxTemps = parsedLines.filter(lambda x: "TMAX" in x[1])
stationTemps = maxTemps.map(lambda x: (x[0], x[2]))
maxTemps = stationTemps.reduceByKey(lambda x, y: max(x, y))
results = maxTemps.collect()

for result in results:
	print(result[0] + "\t{:.2f}F".format(result[1]))
```

***

## Lecture 18 - [Activity] Running the Maximum Temperature by Location Example

* File `max-temperatures.py`

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

lines = sc.textFile('/Users/marshad/Desktop/SparkCourse/data/1800.csv')
# passing parseLine function as a parameter
parsedLines = lines.map(parseLine)
maxTemps = parsedLines.filter(lambda x: "TMAX" in x[1])
stationTemps = maxTemps.map(lambda x: (x[0], x[2]))
maxTemps = stationTemps.reduceByKey(lambda x, y: max(x,y))
results = maxTemps.collect();

for result in results:
	print(result[0] + "\t{:.2f}F".format(result[1]))
```

***

## Lecture 19 - [Activity] Counting Word Occurrences using `flatMap()`

* `map()` versue (1-1 RDD) <--> 1-1 versus 1-many RDD

* File `word-count.py`
```python
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)
         
input = sc.textFile('/Users/marshad/Desktop/SparkCourse/data/Book')
words = input.flatMap(lambda x: x.split())
wordCounts = words.countByValue()

for word, count in wordCounts.items():
	cleanWord = word.encode('ascii', 'ignore')
   	if (cleanWord):
		print(cleanWord.decode() + " " + str(count))
```

***

## Lecture 20 - [Activity] Improving the Word Count Script with Regular Expressions

* File `word-count-better.py`
  
```python
import re
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

def normalizeWords(text):
	return re.compile(r'\W+', re.UNICODE).split(text.lower());

input = sc.textFile('/Users/marshad/Desktop/SparkCourse/data/Book')
# passing normalizeWords function as a parameter
words = input.flatMap(normalizeWords)

wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y : x + y)
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()
results = wordCountsSorted.collect()

for result in results:
	count = str(result[0])
	word = result[1].encode('ascii', 'ignore')
   	if (word):
	print(word.decode() + ":\t\t" + count)
```

***

## Lecture 21 - [Activity] Sorting the Word Count Results
* File ``
```python
import re
from pyspark import SparkConf, SparkContext

def normalizeWords(text):
	return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile('/Users/marshad/Desktop/SparkCourse/data/Book')
words = input.flatMap(normalizeWords)

wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()
results = wordCountsSorted.collect()

for result in results:
	count = str(result[0])
    	word = result[1].encode('ascii', 'ignore')
    	if (word):
        	print(word.decode() + ":\t\t" + count)
```

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
