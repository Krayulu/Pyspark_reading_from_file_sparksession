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
Spark Dataset 2.0+ provides two functions createOrReplaceTempView and  createGlobalTempView.The basic difference between both functions are:

df.createOrReplaceTempView("tempViewName") // syntax 
createOrReplaceTempView() creates or replaces a local temporary view with dataframe df. Lifetime of this view is dependent to SparkSession class, is you want to drop this view,
spark.catalog.dropTempView("tempViewName") // syntax

df.createGlobalTempView("tempViewName") // syntax 
createGlobalTempView() creates a global temporary view with this dataframe df. life time of this view is dependent to spark application itself. If you want to drop,
spark.catalog.dropGlobalTempView("tempViewName") //syntax

Also, one can destory these views by invoking spark.stop() command as well.
