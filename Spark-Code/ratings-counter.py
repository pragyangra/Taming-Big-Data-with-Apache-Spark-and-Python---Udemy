from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

lines = sc.textFile("file:///SparkCourse/ml-100k/u.data")
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue() #counts frequency of each object

sortedResults = collections.OrderedDict(sorted(result.items())) #it preserves the order of the dict items as they were added
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
