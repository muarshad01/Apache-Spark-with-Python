# Section 01: Getting Started with Spark

## Lecture 01

### PATH 
* `/usr/local/Cellar/apache-spark/3.3.0/bin`

### Installation URL
* `https://sundog-education.com/spark-python/`

* `http://localhost:4040`

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

## Lecture 09

### Deprecate

* Deprecate Python2 --> Python3
* Deprecate old MLLib (based on rdd interface) -> Data Frame / Data Set based

### Faster & Better Performance

* Spark3 is 17 times faster than Spark2
	* i) Adaptive execution,
	* ii) dynamic-partition pruning

* Better kubernetes integration
	* Dynamic scaling
* Deep Learning
	* Take advantage of GPUs clusters MLSpark / TensorFlow
* SparkGraph
	* Cypher query language (property graph model)
* Data Lake ACID support Delta Lake
* Binary File support

***
