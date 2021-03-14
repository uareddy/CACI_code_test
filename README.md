# CACI_code_test

**BI data Format choose:**

I have considered both Avro and Parquet formats to store in binary format and chosen Parquet based on below reasons.

1) AVRO is a row-based storage format whereas PARQUET is a columnar based storage format. PARQUET is much better for analytical querying i.e. reads and querying are much more efficient than writing. Write operations in AVRO are better than in PARQUET As BI Analysts required more reads Parquet will be preferred, Data storage space also for Parquet also less compared to Avro .
2) Parquet is good fit for Impala. (Impala is a Massive Parallel Processing (MPP) RDBM SQL-query engine which knows how to operate on data that resides in one or a few external storage engines.) good fit for end users while Avro is good fit for Spark processing.
3) Avro is a Row based format. If you want to retrieve the data as a whole you can use Avro.Parquet is a Column based format. If you are interested to do operations in a subset of columns then you can use Parquet

Note: We can persist fraud data set and we can also broadcast Fraud dataset at the time joining in cloud cluster as I run the program in local I havenot used those optimizations


Part2 of the Task:
Chosen Mysql and created table in Mysql shell and stored data in table.
Saved both variables and connection properties in a separate method so we can use those values in a common library and can be used whenever required.

Part 3 of the task:
Written a method to determine to card maker and applied on Data frames .
We can just change this method whenever we need to enter new card model or update in existing cards model.


Part 4 of the task:
Checked fraud transactions in both datasets by using  both columns credit card number and IP Address.
Filled null values in state name column from transaction-01 and transaction-02 datasets.
Masked credit card numbers last 9 digits and added a column with sum of size(in bytes) of all columns and saved in Json and Parquet formats.
  
.  
