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
