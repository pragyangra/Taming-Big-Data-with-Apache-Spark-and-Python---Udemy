from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("file:///SparkCourse/Book.txt")
words = input.flatMap(lambda x: x.split()) #one to many
wordCounts = words.countByValue()

for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore') #converts anything written in utf or unicode to ascii
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
