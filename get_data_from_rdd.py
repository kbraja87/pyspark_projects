from pyspark import SparkContext
import logging

# Spark Context setup
sc = SparkContext("local", "RDD Application")

# Log Setup
logging.basicConfig(filename='app.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO)

# Get the file
flight_data_rdd = sc.textFile(
    'D://Professional//python_projects//pyspark_projects//airport-codes-na.txt',
    minPartitions=4,
    use_unicode=True
).map(lambda element: element.split('\t'))


# Printing for analysis
print(flight_data_rdd.getNumPartitions())  # get the number of partitions
print(flight_data_rdd.count())  # get the record count
print(flight_data_rdd.take(5))  # display sample records

# departure delay RDD
departure_delay_rdd = sc.textFile(
    'D://Professional//python_projects//pyspark_projects//departuredelays.csv',
    minPartitions=5,
    use_unicode=True
).map(lambda element: element.split(","))

# Get number of Partitions
print(departure_delay_rdd.getNumPartitions())
print(departure_delay_rdd.count())
print(departure_delay_rdd.take(5))
print(departure_delay_rdd.mapPartitionsWithIndex) # number of elements in each partition

# Experimenting map function
print(departure_delay_rdd. \
      map(lambda c: (c[0], c[1], c[2])).take(5))

# filter transformation

filter_distance = departure_delay_rdd \
    .map(lambda element: (element[0], element[2])) \
    .filter(lambda element: element[2] == '602')

print(filter_distance.take(5))

# distinct transformation

distinct_flight_data = departure_delay_rdd  \
    .map(lambda x: x[2])\
    .distinct()

print(distinct_flight_data.take(5))

# Zip with index

print(distinct_flight_data.zipWithIndex().take(5))


# Map Partitions with index : This helps to identify the data skew across the partitions
def partitionElementCount(idx, iterator):
    count = 0
    for _ in iterator:
        count += 1
    return idx, count


# Use mapPartitionsWithIndex to determine data skew
print(departure_delay_rdd.mapPartitionsWithIndex(partitionElementCount).collect())

# Save the data
departure_delay_rdd.saveAsTextFile('D://Professional//python_projects//pyspark_projects//outfiles//')

# Inference:
# Avoid RDDs especially when don't use JVM based languages for the performance reasons
