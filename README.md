# Spark Mini Project - Automobile Post-sales Report (Redesign)

This is a redesign of the Hadoop Automobile Post-sales Report mini-project, previously developed in MapReduce. Redesign consists in using Spark (Pyspark) as the new engine for the map-reduce operation.

The program assists an automobile tracking platform to generate a report of one of their business key metrics. Metric is Total Number of Accidents per make and year of the car.

## Description

The platform tracks the history of incidents after the initial sale of new vehicles. Incidents tracked include subsequent private sales, repairs, and accidents. The platform provides a reference for second-hand buyers to understand the vehicles they are interested in.

The Spark job produces a report of the total number of accidents per make and year of the car.

Spark job consists of:

1. Map Task 1 (extract_vin_key_value): Read history reports and generate intermediate key-value pairs (vin_number, [incident type, make, year])

2. Group by Key and Propagation (populate_make): Since "accident" incidents have no value in car make and year fields, this information needs to be extracted from other types of incidents. For this, key-value pairs are aggregated by key (vin_number) and car make and year values are propagated to "accident" records based on the information available from other types of incidents. Incidents that are not type "accident" are filtered out. Output are populated "accident" records.

3. Map Task 2 (extract_make_key_value): Read populated "accident" records and generate key-value pairs for counting purposes (make-year, 1)

4. Reduce Task: Aggregates "accident" records by sum based on key

## Getting Started

### Dependencies

* [Hortonworks Hadoop Sandbox][1] (HDP_3.0.1_virtualbox_181205)
* Pyspark version 2.3
* Windows 10

[1]: https://www.cloudera.com/downloads/hortonworks-sandbox/hdp.html

### Installing

**Hadoop Setup**

Using Hortonworks Hadoop Sandbox is recommended. This sandbox is a pre-configured virtual machine that has all necessary installation completed. Installation steps can be found [here][2]

[2]: https://www.youtube.com/watch?v=735yx2Eak48

**Files**

1. Download Data source and Python script from https://github.com/rafael-roano/Spark_Mini-Project.git. These are:
    * data.csv
    * spark_autoinc.py
    * spark_job_submission.sh

2. Save data.csv file in a HDFS location
3. Save spark_autoinc.py to local file system (within the sandbox environment)
4. Save spark_job_submission.sh to local file system (within the sandbox environment)


### Executing program

* Start Hortonworks Hadoop Sandbox
* Open HDP SSH (http://localhost:4200)
* Execute Shell script (spark_job_submission.sh)

Results: https://docs.google.com/presentation/d/1eYTpEI1H2H6rsTcG0y2d8LTXxe5VLp28aSJA2VDGW0s/edit?usp=sharing

## Authors

Rafael Roano

## Version History

* 0.1
    * Initial Release