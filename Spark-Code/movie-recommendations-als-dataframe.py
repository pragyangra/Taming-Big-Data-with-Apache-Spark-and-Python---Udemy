from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
from pyspark.ml.recommendation import ALS
import sys
import codecs

def loadMovieNames():
    movieNames = {}
    # CHANGE THIS TO THE PATH TO YOUR u.ITEM FILE:
    #this opens the file locally where you are running the driver script to be kept in memory
    #this can be done because the file is small
    #with statement helps avoiding bugs and leaks by ensuring that a resource is properly 
    #released when the code using the resource is completely executed.
    with codecs.open("E:/SparkCourse/ml-100k/u.ITEM", "r", encoding='ISO-8859-1', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1] #builds a dictionary and assigns movie-id to movie-names
    return movieNames


spark = SparkSession.builder.appName("ALSExample").getOrCreate()
    
moviesSchema = StructType([ \
                     StructField("userID", IntegerType(), True), \
                     StructField("movieID", IntegerType(), True), \
                     StructField("rating", IntegerType(), True), \
                     StructField("timestamp", LongType(), True)])
    
names = loadMovieNames()
    
ratings = spark.read.option("sep", "\t").schema(moviesSchema) \
    .csv("file:///SparkCourse/ml-100k/u.data")
    
print("Training recommendation model...")

als = ALS().setMaxIter(5).setRegParam(0.01).setUserCol("userID").setItemCol("movieID") \
    .setRatingCol("rating")
    
model = als.fit(ratings)

# Manually construct a dataframe of the user ID's we want recs for
# command line argument given to the script using sys.argv
userID = int(sys.argv[1])
userSchema = StructType([StructField("userID", IntegerType(), True)])
users = spark.createDataFrame([[userID,]], userSchema)

recommendations = model.recommendForUserSubset(users, 10).collect()

print("Top 10 recommendations for user ID " + str(userID))

#iterate through each recommendation that comes back
for userRecs in recommendations:
    myRecs = userRecs[1]  #userRecs is (userID, [Row(movieId, rating), Row(movieID, rating)...])
    for rec in myRecs: #my Recs is just the column of recs for the user
        movie = rec[0] #For each rec in the list, extract the movie ID and rating
        rating = rec[1]
        movieName = names[movie]
        print(movieName + str(rating))
        

