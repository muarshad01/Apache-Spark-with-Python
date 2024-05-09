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
### PATH SetUP
```
$ vim ~/.bash_profile
export SPARK_HOME=/opt/homebrew/Cellar/apache-spark/3.5.1/libexec
export PATH=$PATH:SPARK_HOME/bin
source ~/.bash_profile
```
***

## Lecture 07 - Alternate MovieLens download location

* http://media.sundog-soft.com/es/ml-100k.zip

***

## Lecture 08 - [Activity] Installing the MovieLens Movie Rating Dataset

* https://grouplens.org/
* https://movielens.org/

***

## Lecture 09 - [Activity] Run your first Spark program! Ratings histogram example.

### Run the code

```
$ /Users/marshad/Desktop/SparkCourse/code
$ spark-submit ratings-counter.py
```

```python
import collections
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('RatingHistogram')
sc = SparkContext(conf = conf)

lines = sc.textFile('/Users/marshad/Desktop/SparkCourse/data/ml-100k/u.data')
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
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
