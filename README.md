Spark to read/write a file using Python API
======================

# Introduction

This project enables Apache Spark's sparkSession API to read .csv file from a directory and writing the processed file to a single .csv file to a directory.
It is released under the Apache License v2.

# Reqiurements
1. Hadoop 2.7 and above 
2. Spark 2.2 and above 
3. Python 2.7 and above 

# spark createOrReplaceTempView vs createGlobalTempView
Spark Dataset 2.1 and above provides two functions createOrReplaceTempView and  createGlobalTempView.The basic difference between both functions are:

df.createOrReplaceTempView("tempViewName") // syntax 

createOrReplaceTempView() creates or replaces a local temporary view with dataframe df. Lifetime of this view is dependent to SparkSession class, is you want to drop this view,
spark.catalog.dropTempView("tempViewName") // syntax

df.createGlobalTempView("tempViewName") // syntax 

createGlobalTempView() creates a global temporary view with this dataframe df. life time of this view is dependent to spark application itself. If you want to drop,
spark.catalog.dropGlobalTempView("tempViewName") //syntax

Also, one can destory these views by invoking spark.stop() command as well.

# Basic difference of a Spark Application and a Spark Session
Spark application can be used:

1. Used for a single batch job
2. Can used to invoke an interactive session with multiple jobs
3. A long-lived server continually satisfying requests
4. A Spark job can consist of more than just a single map and reduce
5. A Spark Application can consist of more than one session

A SparkSession on the other hand is associated to a Spark Application:

1. Generally, a session is an interaction between two or more entities
2. In Spark 2.0 you can use SparkSession
3. A SparkSession can be created without creating SparkConf, SparkContext or SQLContext, (they’re encapsulated within the SparkSession)

Global temporary views are introduced in Spark 2.1.0 release. This feature is useful when you want to share data among different sessions and keep alive until your application ends.Please see a shot sample I wrote to illustrate the use for createTempView and createGlobalTempView
