import re
from pyspark import SparkConf, SparkContext

#regular expressions are used for text processing, they define how to split up a string
#r'\W+' means break up this text based on words
#re.UNICODE specifies that it may have unicode info in it
def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("file:///SparkCourse/Book.txt")
words = input.flatMap(normalizeWords) #blow out each line into words and create a new row for each word
wordCounts = words.countByValue()

for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))