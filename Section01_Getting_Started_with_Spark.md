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
```python
$ cd /usr/local/Cellar/apache-spark/3.3.0/bin
$ pyspark
$ rdd = sc.textFile('README.md')
$ rdd.count()
$ quit()
$
```

***

## Lecture 06 - [Activity] Getting Set Up: Installing Python, a JDK, Spark, and its Dependencies.

* https://www.sundog-education.com/spark-python/

  ```python
  $ cd /opt/homebrew/Cellar/apache-spark/3.5.1
  $ pyspark
  >>> rdd = sc.textFile('README.md')
  >>> rdd.count()
  >>> quit()
  ```

***

## Lecture 07 - Alternate MovieLens download location

* http://media.sundog-soft.com/es/ml-100k.zip

***

## Lecture 08

* https://grouplens.org/
* https://movielens.org/
* 

***

## Lecture 09 - [Activity] Run your first Spark program! Ratings histogram example.

```python
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

### Deprecate

* Deprecate Python2 --> Python3
* Deprecate old MLLib (based on RDD interface)
    * DataFrame / DataSet based

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
