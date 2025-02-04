# In PySpark, a SparkSession is the entry point to programming Spark with the Dataset and DataFrame API. Here's how to create a SparkSession:

# Creating a SparkSession
# You can create a SparkSession using the SparkSession.builder method:


from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("My Spark App") \
    .getOrCreate()


# In this example:

# - SparkSession.builder creates a new SparkSession builder.
# - appName("My Spark App") sets the name of the application.
# - getOrCreate() gets an existing SparkSession or creates a new one if none exists.

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Creating Pyspark DataFrame:

Here are the ways to create a PySpark DataFrame:

From a List of Rows

from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("Create DataFrame").getOrCreate()

# Create a list of rows
data = [
    ("John", 25),
    ("Mary", 31),
    ("David", 42)
]

# Create a DataFrame
df = spark.createDataFrame(data, ["Name", "Age"])

# Print the DataFrame
df.show()
********************************************************************************************************************************************************************************************************************


From a List of Dictionaries

from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("Create DataFrame").getOrCreate()

# Create a list of dictionaries
data = [
    {"Name": "John", "Age": 25},
    {"Name": "Mary", "Age": 31},
    {"Name": "David", "Age": 42}
]

# Create a DataFrame
df = spark.createDataFrame(data)

# Print the DataFrame
df.show()

********************************************************************************************************************************************************************************************************************

From a Pandas DataFrame

import pandas as pd
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("Create DataFrame").getOrCreate()

# Create a Pandas DataFrame
pdf = pd.DataFrame({
    "Name": ["John", "Mary", "David"],
    "Age": [25, 31, 42]
})

# Create a PySpark DataFrame
df = spark.createDataFrame(pdf)

# Print the DataFrame
df.show()

********************************************************************************************************************************************************************************************************************

From a CSV File

from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("Create DataFrame").getOrCreate()

# Create a DataFrame from a CSV file
df = spark.read.csv("data.csv", header=True, inferSchema=True)

# Print the DataFrame
df.show()

********************************************************************************************************************************************************************************************************************
From a JSON File

from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("Create DataFrame").getOrCreate()

# Create a DataFrame from a JSON file
df = spark.read.json("data.json")

# Print the DataFrame
df.show()

********************************************************************************************************************************************************************************************************************
Pyspark read modes:
# PySpark provides several read modes for reading data from files, including:

# 1. PERMISSIVE: This is the default mode. If a corrupt record is found, it will be ignored and the rest of the data will be processed.

# 2. DROPMALFORMED: This mode will drop the entire file if a corrupt record is found.

# 3. FAILFAST: This mode will throw an exception if a corrupt record is found.

# Here's how to specify the read mode when reading a CSV file in PySpark:


from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("Read Modes").getOrCreate()

# Read a CSV file with PERMISSIVE mode
df_permissive = spark.read.csv("data.csv", header=True, inferSchema=True, mode="PERMISSIVE")

# Read a CSV file with DROPMALFORMED mode
df_dropmalformed = spark.read.csv("data.csv", header=True, inferSchema=True, mode="DROPMALFORMED")

# Read a CSV file with FAILFAST mode
df_failfast = spark.read.csv("data.csv", header=True, inferSchema=True, mode="FAILFAST")

********************************************************************************************************************************************************************************************************************
# In PySpark, you can create a schema using the StructType and StructField classes. Here's an example:


from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create a schema
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])

# Print the schema
print(schema)


# This will create a schema with two fields: name (string type) and age (integer type).

# StructType
# StructType is the main class for creating a schema. It takes a list of StructField objects as arguments.

# StructField
# StructField is the class for creating individual fields in a schema. It takes three arguments:

# - name: The name of the field.
# - dataType: The data type of the field (e.g. StringType, IntegerType, etc.).
# - nullable: A boolean indicating whether the field can be null.

# Data Types
# PySpark provides a range of data types that can be used to define the schema, including:

# - StringType
# - IntegerType
# - LongType
# - DoubleType
# - FloatType
# - BooleanType
# - TimestampType
# - DateType
# - ArrayType
# - MapType
# - StructType

# Here's an example of creating a schema with multiple data types:


from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType

schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("salary", LongType(), True),
    StructField("height", DoubleType(), True)
])
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Here are some ways to display a PySpark DataFrame:

1. *show() Method*
The show() method is used to display the top 20 rows of a DataFrame.


df.show()


You can also specify the number of rows to display:


df.show(5)  # Display top 5 rows


2. *printSchema() Method*
The printSchema() method is used to display the schema of a DataFrame.


df.printSchema()


3. *head() Method*
The head() method is used to display the first few rows of a DataFrame.


df.head(5)  # Display first 5 rows


4. *take() Method*
The take() method is used to display a specified number of rows from a DataFrame.


df.take(5)  # Display first 5 rows


5. *toPandas() Method*
The toPandas() method is used to convert a PySpark DataFrame to a Pandas DataFrame, which can be displayed using Pandas' built-in display methods.


import pandas as pd

pdf = df.toPandas()
print(pdf)

6. display() method:

df.display()

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Filtering in Pyspark:

# In PySpark, you can filter a DataFrame using the filter() method or the where() method. Here are some examples:

# _Filtering using filter() method_

from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("Filtering").getOrCreate()

# Create a DataFrame
data = [("John", 25), ("Mary", 31), ("David", 42)]
df = spark.createDataFrame(data, ["Name", "Age"])

# Filter rows where Age is greater than 30
filtered_df = df.filter(df["Age"] > 30)

# Print the filtered DataFrame
filtered_df.show()


# _Filtering using where() method_

from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("Filtering").getOrCreate()

# Create a DataFrame
data = [("John", 25), ("Mary", 31), ("David", 42)]
df = spark.createDataFrame(data, ["Name", "Age"])

# Filter rows where Age is greater than 30
filtered_df = df.where(df["Age"] > 30)

# Print the filtered DataFrame
filtered_df.show()


# Filtering using SQL-like syntax

from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("Filtering").getOrCreate()

# Create a DataFrame
data = [("John", 25), ("Mary", 31), ("David", 42)]
df = spark.createDataFrame(data, ["Name", "Age"])

# Register the DataFrame as a temporary view
df.createOrReplaceTempView("people")

# Filter rows where Age is greater than 30 using SQL-like syntax
filtered_df = spark.sql("SELECT * FROM people WHERE Age > 30")

# Print the filtered DataFrame
filtered_df.show()


# Filtering using multiple conditions

from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("Filtering").getOrCreate()

# Create a DataFrame
data = [("John", 25), ("Mary", 31), ("David", 42)]
df = spark.createDataFrame(data, ["Name", "Age"])

# Filter rows where Age is greater than 30 and Name starts with "M"
filtered_df = df.filter((df["Age"] > 30) & (df["Name"].startswith("M")))
filtered_df = df.filter((df["Age"] > 30) | (df["Name"].startswith("M")))


# Print the filtered DataFrame
filtered_df.show()


Note that in PySpark, the filter() method and the where() method are equivalent and can be used interchangeably.

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# GROUPBY:

# In PySpark, you can use the groupBy() method to group a DataFrame by one or more columns, and then apply aggregation functions to each group.

# Here is the basic syntax:

df.groupBy("column1", "column2", ...).agg(agg_function1, agg_function2, ...)

# Where:

# - df is the DataFrame you want to group.
# - "column1", "column2", ... are the columns you want to group by.
# - agg_function1, agg_function2, ... are the aggregation functions you want to apply to each group.

# Some common aggregation functions include:

# - count(): Returns the number of rows in each group.
# - sum(): Returns the sum of a column in each group.
# - avg(): Returns the average of a column in each group.
# - max(): Returns the maximum value of a column in each group.
# - min(): Returns the minimum value of a column in each group.

# Here are some examples:

# Group by one column and count

from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("GroupBy").getOrCreate()

# Create a DataFrame
data = [("John", 25), ("Mary", 31), ("John", 25), ("David", 42)]
df = spark.createDataFrame(data, ["Name", "Age"])

# Group by Name and count
grouped_df = df.groupBy("Name").count()

# Print the result
grouped_df.show()

Output:

+-----+-----+
| Name|count|
+-----+-----+
|John |    2|
|Mary |    1|
|David|    1|
+-----+-----+

# Group by multiple columns and sum

from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("GroupBy").getOrCreate()

# Create a DataFrame
data = [("John", "Sales", 100), ("Mary", "Sales", 200), ("John", "Marketing", 50)]
df = spark.createDataFrame(data, ["Name", "Department", "Salary"])

# Group by Name and Department, and sum Salary
grouped_df = df.groupBy("Name", "Department").sum("Salary")

# Print the result
grouped_df.show()

Output:

+-----+----------+--------+
| Name|Department|sum(Salary)|
+-----+----------+--------+
|John |Marketing |       50|
|John |     Sales|      100|
|Mary |     Sales|      200|
+-----+----------+--------+

# Note that you can also use the agg() method to apply multiple aggregation functions to each group. For example:

grouped_df = df.groupBy("Name", "Department").agg(sum("Salary"), count("*"))

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# In PySpark, you can use the orderBy() method or the sort() method to sort a DataFrame by one or more columns.

# OrderBy

# The orderBy() method takes one or more column names as arguments and returns a new DataFrame sorted by those columns.


df.orderBy("column1", "column2", ...)


# For example:


from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("OrderBy").getOrCreate()

# Create a DataFrame
data = [("John", 25), ("Mary", 31), ("David", 42), ("John", 25)]
df = spark.createDataFrame(data, ["Name", "Age"])

# Sort by Age in ascending order
sorted_df = df.orderBy("Age")

# Print the result
sorted_df.show()


Output:


+-----+---+
| Name|Age|
+-----+---+
| John| 25|
| John| 25|
| Mary| 31|
|David| 42|
+-----+---+


# Sort

# The sort() method is similar to orderBy(), but it returns a new DataFrame sorted by the specified columns.


df.sort("column1", "column2", ...)


# For example:


from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("Sort").getOrCreate()

# Create a DataFrame
data = [("John", 25), ("Mary", 31), ("David", 42), ("John", 25)]
df = spark.createDataFrame(data, ["Name", "Age"])

# Sort by Age in descending order
sorted_df = df.sort(df["Age"].desc())

# Print the result
sorted_df.show()


Output:


+-----+---+
| Name|Age|
+-----+---+
|David| 42|
| Mary| 31|
| John| 25|
| John| 25|
+-----+---+


# Ascending and Descending Order

# To sort in ascending order, you can use the asc() method:


df.orderBy(df["Age"].asc())


# To sort in descending order, you can use the desc() method:


df.orderBy(df["Age"].desc())

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
WITHCOLUMN:
# In PySpark, you can add a new column to a DataFrame using the withColumn() method or the withColumnRenamed() method.

# WithColumn

# The withColumn() method takes two arguments: the name of the new column and the value of the new column.


df.withColumn("new_column", value)


# For example:


from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("Add Column").getOrCreate()

# Create a DataFrame
data = [("John", 25), ("Mary", 31), ("David", 42)]
df = spark.createDataFrame(data, ["Name", "Age"])

# Add a new column "Country" with value "USA"
new_df = df.withColumn("Country", "USA")

# Print the result
new_df.show()


Output:


+-----+---+-------+
| Name|Age|Country|
+-----+---+-------+
| John| 25|     USA|
| Mary| 31|     USA|
|David| 42|     USA|
+-----+---+-------+


# WithColumnRenamed

# The withColumnRenamed() method takes two arguments: the name of the existing column and the new name for the column.


df.withColumnRenamed("existing_column", "new_column")


# For example:


from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("Rename Column").getOrCreate()

# Create a DataFrame
data = [("John", 25), ("Mary", 31), ("David", 42)]
df = spark.createDataFrame(data, ["Name", "Age"])

# Rename the "Age" column to "Years"
new_df = df.withColumnRenamed("Age", "Years")

# Print the result
new_df.show()


Output:


+-----+-----+
| Name|Years|
+-----+-----+
| John|   25|
| Mary|   31|
|David|   42|
+-----+-----+


Using SelectExpr

You can also use selectExpr to add a new column.


from pyspark.sql.functions import lit

df.selectExpr("*", "lit('USA') as Country")


Using SQL

You can also use SQL to add a new column.


df.createOrReplaceTempView("df")
new_df = spark.sql("SELECT *, 'USA' as Country FROM df")

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
WHEN - OTHERWISE:

# In PySpark, when and otherwise are used in conjunction with each other to perform conditional logic on a DataFrame.

# when is used to specify a condition, and otherwise is used to specify the value to return when the condition is not met.

# Here's an example:


from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

# Create a SparkSession
spark = SparkSession.builder.appName("When Otherwise").getOrCreate()

# Create a DataFrame
data = [("John", 25), ("Mary", 31), ("David", 42)]
df = spark.createDataFrame(data, ["Name", "Age"])

# Use when and otherwise to create a new column "Status"
df = df.withColumn("Status", when(col("Age") > 30, "Older").otherwise("Younger"))

# Print the result
df.show()


Output:


+-----+---+-------+
| Name|Age| Status|
+-----+---+-------+
| John| 25|Younger|
| Mary| 31|  Older|
|David| 42|  Older|
+-----+---+-------+


# In this example, the when function checks if the value in the "Age" column is greater than 30. If it is, the value "Older" is returned. Otherwise, the value "Younger" is returned.

# You can also chain multiple when statements together to perform more complex conditional logic.


df = df.withColumn("Status", when(col("Age") > 60, "Senior").when(col("Age") > 30, "Older").otherwise("Younger"))
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# In PySpark, you can perform various types of joins on DataFrames using the join() method. Here are some examples:

# Inner Join

df1.join(df2, df1["id"] == df2["id"])

# This will return a new DataFrame containing only the rows where the "id" column matches between df1 and df2.

Left Outer Join

df1.join(df2, df1["id"] == df2["id"], "left_outer")

This will return a new DataFrame containing all the rows from df1 and the matching rows from df2. If there is no match, the result will contain null values.

Right Outer Join

df1.join(df2, df1["id"] == df2["id"], "right_outer")

This will return a new DataFrame containing all the rows from df2 and the matching rows from df1. If there is no match, the result will contain null values.

Full Outer Join

df1.join(df2, df1["id"] == df2["id"], "outer")

This will return a new DataFrame containing all the rows from both df1 and df2. If there is no match, the result will contain null values.

Cross Join

df1.crossJoin(df2)

This will return a new DataFrame containing the Cartesian product of df1 and df2.

Here's an example code snippet that demonstrates these joins:

from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("Joins").getOrCreate()

# Create two DataFrames
df1 = spark.createDataFrame([(1, "John"), (2, "Mary"), (3, "David")], ["id", "name"])
df2 = spark.createDataFrame([(1, "USA"), (2, "Canada"), (4, "Mexico")], ["id", "country"])

# Perform inner join
inner_join = df1.join(df2, df1["id"] == df2["id"])
print("Inner Join:")
inner_join.show()

# Perform left outer join
left_outer_join = df1.join(df2, df1["id"] == df2["id"], "left_outer")
print("Left Outer Join:")
left_outer_join.show()

# Perform right outer join
right_outer_join = df1.join(df2, df1["id"] == df2["id"], "right_outer")
print("Right Outer Join:")
right_outer_join.show()

# Perform full outer join
full_outer_join = df1.join(df2, df1["id"] == df2["id"], "outer")
print("Full Outer Join:")
full_outer_join.show()

# Perform cross join
cross_join = df1.crossJoin(df2)
print("Cross Join:")
cross_join.show()

Note that the join() method can also take additional arguments, such as how (which specifies the type of join) and on (which specifies the join condition).

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
WINDOWS FUNCTIONS

Window functions in PySpark allow you to perform calculations across a set of rows that are related to the current row, such as aggregating values or ranking rows.

Here are some common window functions in PySpark:

1. ROW_NUMBER(): Assigns a unique row number to each row within a partition.
2. RANK(): Assigns a rank to each row within a partition based on a specified column.
3. DENSE_RANK(): Similar to RANK(), but does not skip any rank values.
4. NTILE(): Divides a partition into a specified number of groups, based on a specified column.
5. LAG(): Returns the value of a column from a previous row within a partition.
6. LEAD(): Returns the value of a column from a next row within a partition.
7. SUM(), AVG(), MAX(), MIN(): Performs aggregations over a partition.

To use window functions in PySpark, you need to:

1. Import the necessary functions from pyspark.sql.functions.
2. Define a window specification using the Window class.
3. Apply the window function to a column in your DataFrame.

Here's an example:

from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number, rank, dense_rank, ntile, lag, lead, sum, avg, max, min
from pyspark.sql import Window

# Create a SparkSession
spark = SparkSession.builder.appName("Window Functions").getOrCreate()

# Create a sample DataFrame
data = [("John", 25, 100), ("Mary", 31, 200), ("David", 42, 300), ("John", 25, 400)]
df = spark.createDataFrame(data, ["Name", "Age", "Salary"])

# Define a window specification
window = Window.partitionBy("Name").orderBy("Age")

# Apply window functions
df_with_window_functions = df.withColumn("Row Number", row_number().over(window)) \
                                .withColumn("Rank", rank().over(window)) \
                                .withColumn("Dense Rank", dense_rank().over(window)) \
                                .withColumn("NTile", ntile(2).over(window)) \
                                .withColumn("Lag Salary", lag("Salary", 1).over(window)) \
                                .withColumn("Lead Salary", lead("Salary", 1).over(window)) \
                                .withColumn("Sum Salary", sum("Salary").over(window)) \
                                .withColumn("Avg Salary", avg("Salary").over(window)) \
                                .withColumn("Max Salary", max("Salary").over(window)) \
                                .withColumn("Min Salary", min("Salary").over(window))

# Show the results
df_with_window_functions.show()

This example demonstrates how to apply various window functions to a DataFrame, including row numbering, ranking, and aggregations.

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


REPARTITION AND COALESCE

In PySpark, repartition and coalesce are two methods that can be used to control the number of partitions in a DataFrame.

Repartition

repartition is used to increase or decrease the number of partitions in a DataFrame. When you call repartition, Spark will reshuffle the data across the new number of partitions.

Here's an example:

df = spark.createDataFrame([(1, 2), (3, 4), (5, 6)], ["col1", "col2"])
df = df.repartition(4)  # Increase the number of partitions to 4

Coalesce

coalesce is used to decrease the number of partitions in a DataFrame. Unlike repartition, coalesce does not reshuffle the data, but instead merges adjacent partitions together.

Here's an example:

df = spark.createDataFrame([(1, 2), (3, 4), (5, 6)], ["col1", "col2"])
df = df.coalesce(2)  # Decrease the number of partitions to 2

Key differences

Here are the key differences between repartition and coalesce:

- Reshuffling: repartition reshuffles the data across the new number of partitions, while coalesce does not.
- Increasing partitions: repartition can increase the number of partitions, while coalesce can only decrease the number of partitions.
- Performance: coalesce is generally faster than repartition because it does not require reshuffling the data.

When to use each

Here are some guidelines on when to use each method:

- Use repartition when:
    - You need to increase the number of partitions.
    - You need to reshuffle the data to ensure even distribution across partitions.
- Use coalesce when:
    - You need to decrease the number of partitions.
    - You want to minimize the amount of data reshuffling.
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

WRITES

In PySpark, when writing a DataFrame to a file or a database, you can specify the write mode to control how the data is written. Here are the different write modes available:

1. Append: This mode appends the new data to the existing data in the file or database.


df.write.mode("append").parquet("path/to/file")


2. Overwrite: This mode overwrites the existing data in the file or database with the new data.


df.write.mode("overwrite").parquet("path/to/file")


3. ErrorIfExists: This mode throws an error if the file or database already exists.


df.write.mode("errorIfExists").parquet("path/to/file")


4. Ignore: This mode ignores the write operation if the file or database already exists.


df.write.mode("ignore").parquet("path/to/file")


Note that the write mode only affects the write operation and does not affect the read operation.

Here's an example code snippet that demonstrates the different write modes:

from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("Write Modes").getOrCreate()

# Create a DataFrame
data = [("John", 25), ("Mary", 31), ("David", 42)]
df = spark.createDataFrame(data, ["Name", "Age"])

# Write the DataFrame to a file in append mode
df.write.mode("append").parquet("path/to/file")

# Write the DataFrame to a file in overwrite mode
df.write.mode("overwrite").parquet("path/to/file")

# Write the DataFrame to a file in errorIfExists mode
try:
    df.write.mode("errorIfExists").parquet("path/to/file")
except Exception as e:
    print(e)

# Write the DataFrame to a file in ignore mode
df.write.mode("ignore").parquet("path/to/file")

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

FILE FORMATS

PySpark supports various file formats for reading and writing data. Here are some of the most commonly used file formats:

1. CSV (Comma Separated Values): A plain text file format where each line represents a single record, and each field is separated by a comma.


df.write.csv("path/to/file")


2. JSON (JavaScript Object Notation): A lightweight, human-readable data interchange format.


df.write.json("path/to/file")


3. Parquet: A columnar storage format that is optimized for big data processing.


df.write.parquet("path/to/file")


4. Avro: A binary data serialization format that is widely used in big data processing.


df.write.format("avro").save("path/to/file")


5. ORC (Optimized Row Columnar): A columnar storage format that is optimized for big data processing.


df.write.format("orc").save("path/to/file")


6. Text: A plain text file format where each line represents a single record.


df.write.text("path/to/file")


7. JDBC: A database connectivity format that allows you to read and write data to relational databases.


df.write.jdbc("jdbc_url", "table_name", properties={"user": "username", "password": "password"})


When reading data from a file, you can specify the file format using the format() method.


df = spark.read.format("csv").option("header", "true").load("path/to/file")


Note that the file format and options may vary depending on the specific use case and requirements.

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
DELTA LAKE

Delta Lake is an open-source storage layer that provides a scalable and reliable way to store and manage large amounts of data. It is designed to work with Apache Spark and other big data processing engines.

Here are some key features of Delta Lake:

1. ACID Transactions: Delta Lake supports atomicity, consistency, isolation, and durability (ACID) transactions, ensuring that data is written and read in a consistent and reliable manner.
2. Schema Enforcement: Delta Lake enforces schema definitions, ensuring that data is written in a consistent format and preventing data corruption.
3. Data Versioning: Delta Lake provides data versioning, allowing you to track changes to your data over time and easily roll back to previous versions if needed.
4. Scalability: Delta Lake is designed to scale horizontally, allowing you to handle large amounts of data and scale your cluster as needed.
5. Integration with Apache Spark: Delta Lake is designed to work seamlessly with Apache Spark, allowing you to easily read and write data to Delta Lake tables using Spark DataFrames.

To use Delta Lake with PySpark, you need to add the Delta Lake library to your PySpark application. You can do this by adding the following line of code to your PySpark application:

from delta import *

Once you have added the Delta Lake library, you can create a Delta Lake table using the delta.create() method:

delta.create("path/to/table", schema="schema_definition")

You can then read and write data to the Delta Lake table using Spark DataFrames:

df = spark.read.format("delta").load("path/to/table")
df.write.format("delta").save("path/to/table")

Delta Lake also provides a number of other features and APIs, including:

- DeltaTable: A Delta Lake table that provides a convenient API for reading and writing data.
- DeltaLog: A log of all changes made to a Delta Lake table, allowing you to track changes and roll back to previous versions if needed.
- DeltaMerge: A API for merging data from one Delta Lake table into another.

Overall, Delta Lake provides a powerful and scalable way to store and manage large amounts of data, and is a great choice for big data applications that require high performance and reliability.

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

DELTA FORMAT

Delta format is a file format used by Delta Lake to store data in a scalable and efficient manner. Here are some key features of the Delta format:

1. Parquet-based: Delta format is built on top of the Parquet file format, which is a columnar storage format that provides efficient compression and encoding.
2. Delta-encoded: Delta format uses a delta-encoding scheme to store changes to the data, which allows for efficient storage and retrieval of changes.
3. Transaction logs: Delta format stores transaction logs that record all changes made to the data, which allows for efficient auditing and recovery.
4. Schema evolution: Delta format supports schema evolution, which allows you to modify the schema of your data without having to rewrite the entire dataset.
5. Data versioning: Delta format supports data versioning, which allows you to track changes to your data over time and easily roll back to previous versions if needed.

The Delta format consists of the following components:

1. _delta_log: A directory that contains the transaction logs for the dataset.
2. __delta_log/.json_*: Files that contain the transaction logs in JSON format.
3. _part-.parquet_*: Files that contain the data in Parquet format.
4. _metadata: A file that contains metadata about the dataset, such as the schema and partitioning information.

To read and write data in Delta format using PySpark, you can use the delta format specifier. For example:

df = spark.read.format("delta").load("path/to/delta/table")
df.write.format("delta").save("path/to/delta/table")

Note that the Delta format is specific to Delta Lake and is not compatible with other data storage systems.

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

TIME TRAVELLING WITH DELTA LAKE

Time traveling with Delta Lake allows you to query data as it existed at a specific point in time. This is useful for auditing, debugging, and analyzing data changes over time.

Here are the ways to time travel with Delta Lake:

1. Versioning: Delta Lake stores a version number for each commit. You can query data by specifying a version number.


df = spark.read.format("delta").option("versionAsOf", "2").load("path/to/delta/table")


2. Timestamp: Delta Lake stores a timestamp for each commit. You can query data by specifying a timestamp.


df = spark.read.format("delta").option("timestampAsOf", "2022-01-01 12:00:00").load("path/to/delta/table")


3. Date: You can query data by specifying a date.


df = spark.read.format("delta").option("dateAsOf", "2022-01-01").load("path/to/delta/table")


Note that the versionAsOf, timestampAsOf, and dateAsOf options can be used together to specify a specific point in time.

Here's an example code snippet that demonstrates time traveling with Delta Lake:

from delta import *

# Create a Delta Lake table
delta.create("path/to/delta/table", schema="schema_definition")

# Write data to the table
df.write.format("delta").save("path/to/delta/table")

# Time travel to version 2
df = spark.read.format("delta").option("versionAsOf", "2").load("path/to/delta/table")

# Time travel to a specific timestamp
df = spark.read.format("delta").option("timestampAsOf", "2022-01-01 12:00:00").load("path/to/delta/table")

# Time travel to a specific date
df = spark.read.format("delta").option("dateAsOf", "2022-01-01").load("path/to/delta/table")

Note that time traveling with Delta Lake requires that the _delta_log directory is present and contains the necessary transaction logs.

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
OPTIMIZATION TECHNIQUES

BROADCAST JOIN

A broadcast join is a type of join operation in Apache Spark that is optimized for joining large datasets with smaller datasets. In a broadcast join, the smaller dataset is broadcasted to all the nodes in the cluster, and then joined with the larger dataset.

Here are the key characteristics of a broadcast join:

1. Small dataset: One of the datasets being joined is small enough to fit in memory on each node.
2. Broadcasting: The small dataset is broadcasted to all the nodes in the cluster.
3. Join operation: The broadcasted dataset is then joined with the larger dataset on each node.

The benefits of broadcast joins are:

1. Improved performance: Broadcast joins can be faster than traditional joins because they avoid the overhead of shuffling data between nodes.
2. Reduced network traffic: By broadcasting the small dataset, we reduce the amount of data that needs to be transferred between nodes.

To perform a broadcast join in PySpark, you can use the broadcast method on the smaller dataset, like this:

from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("Broadcast Join").getOrCreate()

# Create two DataFrames
large_df = spark.createDataFrame([(1, "John"), (2, "Mary"), (3, "David")], ["id", "name"])
small_df = spark.createDataFrame([(1, "USA"), (2, "Canada")], ["id", "country"])

# Broadcast the small DataFrame
broadcast_small_df = spark.sparkContext.broadcast(small_df)

# Perform the join
joined_df = large_df.join(broadcast_small_df.value, on="id")

# Show the results
joined_df.show()

In this example, we create two DataFrames, large_df and small_df. We then broadcast the small_df using the broadcast method, and perform the join using the join method. The resulting DataFrame is stored in joined_df.

Note that the broadcast method returns a Broadcast object, which can be used to access the broadcasted data. In this example, we use the value attribute to access the broadcasted DataFrame.

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
PARTITION SKEW

Partition skew occurs when the data in a partitioned table or dataset is unevenly distributed across the partitions, leading to some partitions having much more data than others. This can cause performance issues, such as:

1. Slow query performance: Queries that access skewed partitions can take longer to execute.
2. Inefficient resource utilization: Skewed partitions can lead to inefficient use of resources, such as memory and CPU.
3. Increased risk of data loss: Skewed partitions can increase the risk of data loss due to node failures or other issues.

To minimize partition skew, you can use the following techniques:

1. Repartitioning: Repartitioning involves redistributing the data across the partitions to achieve a more even distribution.
2. Salting: Salting involves adding a random value to the partitioning key to distribute the data more evenly across the partitions.
3. Hash partitioning: Hash partitioning involves using a hash function to distribute the data across the partitions.
4. Range partitioning: Range partitioning involves dividing the data into ranges and assigning each range to a partition.
5. Composite partitioning: Composite partitioning involves using multiple partitioning schemes, such as hash and range partitioning, to distribute the data across the partitions.
6. Data sampling: Data sampling involves taking a random sample of the data to estimate the distribution of the data and adjust the partitioning scheme accordingly.
7. Dynamic partitioning: Dynamic partitioning involves adjusting the partitioning scheme based on the actual data distribution and query patterns.

In PySpark, you can use the repartition method to repartition a DataFrame, and the partitionBy method to partition a DataFrame using a specific partitioning scheme.

For example:

from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("Partition Skew Minimization").getOrCreate()

# Create a DataFrame
data = [("John", 25), ("Mary", 31), ("David", 42), ("John", 25), ("Mary", 31), ("David", 42)]
df = spark.createDataFrame(data, ["Name", "Age"])

# Repartition the DataFrame
df_repartitioned = df.repartition(4)

# Partition the DataFrame using hash partitioning
df_partitioned = df.repartitionByRange(4, "Age")

# Show the results
df_repartitioned.show()
df_partitioned.show()

In this example, we create a DataFrame and repartition it using the repartition method. We also partition the DataFrame using hash partitioning with the repartitionByRange method.

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CACHE VS PERSIST

In Apache Spark, caching and persistence are used to store data in memory or on disk to improve performance. Here are the different memory levels for caching and persistence:

Memory Levels:

1. MEMORY_ONLY: Stores data only in memory (RAM). If the data doesn't fit in memory, it will be evicted.
2. MEMORY_AND_DISK: Stores data in both memory (RAM) and disk. If the data doesn't fit in memory, it will be stored on disk.
3. MEMORY_ONLY_SER: Stores data in memory (RAM) in a serialized format. This is more compact than MEMORY_ONLY but slower to access.
4. MEMORY_AND_DISK_SER: Stores data in both memory (RAM) and disk in a serialized format.
5. DISK_ONLY: Stores data only on disk.

Cache and Persist:

1. cache(): Caches the data in memory (RAM) using the MEMORY_ONLY memory level.
2. persist(): Persists the data in memory (RAM) or disk using the specified memory level.

Example:

from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("Cache and Persist").getOrCreate()

# Create a DataFrame
data = [("John", 25), ("Mary", 31), ("David", 42)]
df = spark.createDataFrame(data, ["Name", "Age"])

# Cache the DataFrame in memory (RAM)
df.cache()

# Persist the DataFrame in memory (RAM) and disk
df.persist("MEMORY_AND_DISK")

# Show the results
df.show()

In this example, we create a DataFrame and cache it in memory (RAM) using the cache() method. We then persist the DataFrame in both memory (RAM) and disk using the persist() method with the MEMORY_AND_DISK memory level.

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
OPTIMIZE AND VACCUUM

In Apache Spark, OPTIMIZE and VACUUM are two commands used to optimize and manage the storage of Delta Lake tables.

OPTIMIZE:

The OPTIMIZE command is used to optimize the storage of a Delta Lake table by rewriting the table's data files to be more compact and efficient. This can improve query performance and reduce storage costs.

Here's an example of how to use the OPTIMIZE command:

from delta import *

# Create a Delta Lake table
delta.create("path/to/table", schema="schema_definition")

# Optimize the table
delta.optimize("path/to/table")

VACUUM:

The VACUUM command is used to remove files from a Delta Lake table that are no longer needed. This can help to reduce storage costs and improve query performance.

Here's an example of how to use the VACUUM command:

from delta import *

# Create a Delta Lake table
delta.create("path/to/table", schema="schema_definition")

# Vacuum the table
delta.vacuum("path/to/table")

Note that the VACUUM command can also be used with a retention period, which specifies the minimum age of files that can be deleted. For example:

delta.vacuum("path/to/table", retentionPeriod=7)

This would delete files that are older than 7 days.

Benefits:

Using OPTIMIZE and VACUUM can provide several benefits, including:

- Improved query performance
- Reduced storage costs
- Simplified data management
- Improved data reliability

Best practices:

Here are some best practices to keep in mind when using OPTIMIZE and VACUUM:

- Run OPTIMIZE regularly to keep your data compact and efficient
- Run VACUUM regularly to remove unnecessary files and reduce storage costs
- Use the retentionPeriod option with VACUUM to specify the minimum age of files that can be deleted
- Monitor your data and adjust your optimization and vacuuming schedule as needed

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

OUT OF MEMORY ISSUE

Out-of-memory (OOM) issues occur when an application runs out of memory to perform its operations. In Apache Spark, OOM issues can occur due to various reasons such as:

1. Insufficient memory allocation: If the Spark application is not allocated sufficient memory, it can lead to OOM issues.
2. Data skew: When data is skewed, it can lead to a few nodes processing a large amount of data, causing OOM issues.
3. Inefficient data structures: Using inefficient data structures, such as using a large number of small objects, can lead to OOM issues.
4. Caching: Caching large datasets can lead to OOM issues if the cache is not properly managed.

To resolve OOM issues in Apache Spark, follow these steps:

1. Increase memory allocation: Increase the memory allocation for the Spark application by setting the spark.driver.memory and spark.executor.memory properties.
2. Optimize data structures: Optimize data structures to reduce memory usage. For example, use Array instead of List for large collections.
3. Use caching wisely: Use caching wisely by caching only the necessary data and using the cache() method with caution.
4. *Use repartition*: Use the repartition method to redistribute data evenly across nodes, reducing data skew.
5. *Use broadcast*: Use the broadcast method to broadcast small datasets to all nodes, reducing data transfer and memory usage.
6. Monitor memory usage: Monitor memory usage using tools like Ganglia or Spark UI to identify memory-intensive operations and optimize them.
7. *Use spark.memory.fraction*: Adjust the spark.memory.fraction property to control the amount of memory used for caching and execution.
8. *Use spark.memory.storageFraction*: Adjust the spark.memory.storageFraction property to control the amount of memory used for caching.

Example code to increase memory allocation:

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("OOM Resolution") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

Example code to optimize data structures:

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("OOM Resolution").getOrCreate()

# Create a DataFrame with optimized data structures
data = [(1, "John"), (2, "Mary"), (3, "David")]
df = spark.createDataFrame(data, ["id", "name"]).cache()

# Use Array instead of List for large collections
df = df.withColumn("array_column", df["name"].apply(lambda x: [x]))

Note: The above code snippets are examples and may need to be modified based on the specific use case and requirements.

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


