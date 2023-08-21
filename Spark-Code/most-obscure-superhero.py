from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostObscureHero").getOrCreate()

schema = StructType([ \
    StructField("id", IntegerType, True), \
    StructField("name", StringType, True)
])

names = spark.read.schema(schema).option("sep", " ").csv("file:///SparkCourse/Marvel-Names.txt")

lines = spark.read.text("file:///SparkCourse/Marvel-Graph.txt")

connects = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
            .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
            .groupBy("id").agg(func.sum("connections").alias("connections"))

least = connects.sort(func.col("connections").asc()).first()

joint = connects.join(names, "id")

leastpop = joint.filter(func.col("id") == least)

print(leastpop)