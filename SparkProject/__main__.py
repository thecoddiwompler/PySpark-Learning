import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.functions import desc
from pyspark.sql.functions import lower, upper, substring, min, max, date_add, date_sub, expr
from pyspark.sql.functions import to_date, to_timestamp
import schema

# Build a Spark Session

spark = SparkSession.builder.getOrCreate()


# Read a CSV file and provide the schema, header (optional)
# If we do not provide schema it will infer on its own. Same for header, it will just take _c0, _c1 as headers and treat every row as datavalues

df = spark.read.csv("crime-data.csv", header=True, schema=schema.schema)


# Print the dataframe (using show command). Arguement is optional (default 20)

df.show(3)


# To print columns we can use select function
# The parameter of select can be string, columns, or list. So all 3 query below will result in same value:

df.select("IUCR","ID","case number").show(3) # Here, Column names are case insensitive (SQL like behaviour)
df.select(df.IUCR, df.ID).show(3) # This method does not support if column name has more than one word. Moreover, here Column names are case sensitive
df.select(["IUCR","ID"]).show(3)


# Adding a new column:

new_column = lit("Random Value")
df = df.withColumn("New Column", new_column) # Doesn't work for multiple columns

df.show(5)


# Dropping a column(s):

df = df.drop("New Column") # Does work for multiple columns
df.show(3)


# Renaming a column:

new_df = df.withColumnRenamed("Description", "New Description") # Only 2 mandatory params
new_df.show(3)


# Filtering for a particular condition
# filter is an alias for where. where is used more often than filter

new_df = df.where("ID = '10224738'") # or df.ID = '10224738'. For Columns with name more than one word, enclose them within backticks(``)
new_df.show()


# Selecting Distinct Rows:

distinct_df = df.select("District").distinct() # dropDuplicates() does the same thing
distinct_df.show()


# Order By (Sort):

sorted_df = df.orderBy("id", desc("Description")) # sort() also do the same thing
sorted_df.show()


# Union (Or Union all)

first_df = df.select("ID","Case Number", "Date", "Block", "Primary Type").where("id in (10000105, 10000108, 10000109)")
second_df = df.select("ID","Case Number", "Date", "Block", "Primary Type").where("id in (10000110, 10000111, 10000112)")

unioned_df = first_df.union(second_df)
unioned_df.show()


#####################################################################################################################################################

# Aggregation

#####################################################################################################################################################

# Group By

aggregated_df = df.groupBy("Primary Type").count()
aggregated_df.show()


# agg() function:

aggregated_df = df.groupBy("Primary Type").agg(expr("count(*) as counter"), expr("min(id) as min_id")).orderBy("counter")
# expr parse the expression into SQL like expressions. Used extensively with where clause. To give multiple condiions use multiple expr inside agg()
# if we are using expr or the column name contains more than one word, the column should be enclosed within backtick(``)

aggregated_df.show()

#####################################################################################################################################################

# Built-In Function

#####################################################################################################################################################

#####################################################################################################################################################
# String Functions

# Lowercase of a Column:

new_df = df.select(lower("description"))
new_df.show(1)


# Uppercase of a Column:

new_df = df.select(upper("description"))
new_df.show(1)


# Slice a part of a Column:

new_df = df.select(substring("description",2,4)) # Substring returns column type
new_df.show(3)


#####################################################################################################################################################
# Numeric Functions

# Min function:

new_df = df.select(min("id")) # Please note that this is aggregate function so in order to introduce other column we need to use groupBy/window function
new_df.show()


# Max function:

new_df = df.select(max("id"))
new_df.show()


#####################################################################################################################################################
# Date diff function

# date_add function

new_df = df.withColumn("Added Date Column", date_add(to_date("updated on", "MM/dd/yyyy hh:mm:ss a"),1)) 
new_df.select("Added Date Column", "Updated on").show(1)


#date_sub function

new_df = df.withColumn("Added Date Column", date_sub(to_date("updated on", "MM/dd/yyyy hh:mm:ss a"),1)) 
new_df.select("Added Date Column", "Updated on").show(1)


####################################################################################################################################################
# Working with dates:

# to_date and to_timestamp

new_df = spark.createDataFrame([('2019-12-25 13:30:00',)],['Christmas'])
new_df.show()

converted_df = new_df.select(to_date("Christmas", 'yyyy-MM-dd HH:mm:ss'),to_timestamp("Christmas", 'yyyy-MM-dd HH:mm:ss')) # to_date accepts column and
# date format of the provided column. There could be several formats like the one before, dd/MMM/yyyy HH:mm:ss a, and so on. Same for to_timestamp
converted_df.show()

df.select("Date",to_date("Date", "MM/dd/yyyy HH:mm:ss a"), to_timestamp("Date", "MM/dd/yyyy HH:mm:ss a")).show(3) # Showcasing with actual data


####################################################################################################################################################
# Joins:

left_df = spark.createDataFrame([(100,"Raj"),(200,"Rahul"),(300,"Raj"),(None,"Aditya")],['ID','Name'])
right_df = spark.createDataFrame([(400,"Akhil"),(200,"Rahul"),(300,"Raj"),(None, "Rakesh")],['ID','Name'])

joined_df = left_df.join(right_df,"id","inner") # So many joins are there- left, right, anti, semi (Like inner but Only return left df columns),cross, etc
joined_df.select(left_df.ID, left_df.Name).show()  # We can use <df_name>["<column_name>"] for the columns which have spaces in its name

# In PySpark, when you perform a join operation using DataFrame API, 
# the resulting DataFrame retains the information about its parent tables. 
# This allows you to reference the columns from the original tables using their table names. This feature is not in traditional SQL.