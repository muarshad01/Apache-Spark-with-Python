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
