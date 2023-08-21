from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("SpendByCustomer")
sc = SparkContext(conf = conf)

def spend(line):
    fields = line.split(',')
    customerID = int(fields[0])
    amt = float(fields[2])
    return (customerID, amt)

input = sc.textFile("file:///SparkCourse/customer-orders.csv")
orders = input.map(spend)
total = orders.reduceByKey(lambda x, y: x+y)
sorted = total.map(lambda x: (x[1], x[0])).sortByKey()

results = sorted.collect() #converts into a python object for printing

for result in results:
    print(result)