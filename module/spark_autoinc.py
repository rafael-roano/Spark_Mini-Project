#!/usr/bin/env python

from pyspark.sql import SparkSession
from operator import add

spark = SparkSession.builder.master("local").appName("My Application").getOrCreate()
sc = spark.sparkContext
# raw_rdd = sc.textFile("/home/jv/Python/SB/SB_Projects/Apache_Spark_Mini-Project/data.csv")
raw_rdd = sc.textFile("hdfs://sandbox-hdp.hortonworks.com:8020/user/admin/data.csv")


def extract_vin_key_value(row):
    '''
    Map RDD row as key-value, having the car vin_number as key and the incident type, car make and car year as value.

    Args:
        row (str): RDD row
    
    Returns:
        key, value (tuple)
    '''

    row = row.split(",")
    key = row[2]
    value = [row[1], row[3], row[5]]

    return (key, value)


def populate_make(values):
    '''
    Propagate car make and car year to all types of incidents. Filter out incident types that are not Accident.

    Args:
        values (str): Grouped values by vin_number
    
    Returns:
        propagated (list)
    '''
    
    
    master_make = ""
    master_year = ""
    propagated =[]
    
    def master_capture(values):
        # Capture car make and year from incident types that hold these values.
    
        for set in values:
            
            if set[0]:
                
                return set[1], set[2]
               
    master_make, master_year = master_capture(values)

    for set in values:
        if set[0] == "A":
        
            set = [set[0], master_make, master_year]
            propagated.append(set)

    
    return propagated

def extract_make_key_value(row):
    '''
    Map RDD row as key-value, having the car make and car year as key, and 1 as value.

    Args:
        row (str): RDD row
    
    Returns:
        key, 1 (tuple)
    '''

    key = row[1] + "-" + row[2]

    return (key, 1)


# Map RDD row as key-value, having the car vin_number as key and the incident type, car make and car year as value.
vin_kv = raw_rdd.map(lambda x: extract_vin_key_value(x))  

# Group records by key (vin_number), propagate car make and car year to all types of incidents and filter out incident types that are not an Accident.
enhance_make = vin_kv.groupByKey().flatMap(lambda kv: populate_make(kv[1]))

# Map RDD row as key-value, having the car make and car year as key, and 1 as value.
make_kv = enhance_make.map(lambda x: extract_make_key_value(x))

# Aggregate the key and count the number of records in total per key
reduced_kv = make_kv.reduceByKey(add)
# print(reduced_kv.collect())


# Save output to HDFS as CSV
df = spark.createDataFrame(reduced_kv)
# df.write.mode("overwrite").csv("/home/jv/Python/SB/SB_Projects/Apache_Spark_Mini-Project/output")
df.write.mode("overwrite").csv("hdfs://sandbox-hdp.hortonworks.com:8020/user/admin/output")