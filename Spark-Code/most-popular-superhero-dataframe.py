from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

schema = StructType([ \
    StructField("id", IntegerType(), True), \
    StructField("name", StringType(), True)
])
names = spark.read.schema(schema).option("sep", " ").csv("file:///SparkCourse/Marvel-Names.txt")

lines = spark.read.text("file:///SparkCourse/Marvel-Graph.txt") #will take each line and throw it into a single column

connections = lines.withColumn("id", func.split(func.col("value"), " ")[0]) \
            .withColumn("connects", func.size(func.split(func.col("values"), " ")) - 1) \
            .groupBy("id").sum("connects").alias("connects")

mostpopular = connections.sort(func.col("connects").desc()).first()
mostpopularname = names.filter(func.col("id") == mostpopular[0]).select("name").first() #if there are multiple enteries for the name we select the first one
print(mostpopularname[0])