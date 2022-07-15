import re
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

def normalizeWords(text):
   return re.compile(r'\W+', re.UNICODE).split(text.lower());

input = sc.textFile("/Users/marshad/Desktop/SparkCourse/data/Book")
words = input.flatMap(normalizeWords)
wordCounts = words.countByValue()

for word, count in wordCounts.items():
   cleanWord = word.encode('ascii', 'ignore')
   if (cleanWord):
      print(cleanWord.decode() + " " + str(count))
