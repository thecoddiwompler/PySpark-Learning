from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BooleanType, DoubleType, IntegerType

schema = StructType([
    StructField('ID', IntegerType(), True),
    StructField('Case Number', StringType(), True),
    StructField('Date', StringType(), True),
    StructField('Block', StringType(), True),
    StructField('IUCR', StringType(), True),
    StructField('Primary Type', StringType(), True), 
    StructField('Description', StringType(), True),
    StructField('Location Description', StringType(), True),
    StructField('Arrest', StringType(), True),
    StructField('Domestic', BooleanType(), True),
    StructField('Beat', StringType(), True),
    StructField('District', StringType(), True),
    StructField('Ward', StringType(), True),
    StructField('Community Area', StringType(), True),
    StructField('FBI Code', StringType(), True),
    StructField('X Coordinate', StringType(), True),
    StructField('Y Coordinate', StringType(), True),
    StructField('Year', IntegerType(), True), 
    StructField('Updated On', StringType(), True),
    StructField('Latitude', DoubleType(), True),
    StructField('Longitude', DoubleType(), True),
    StructField('Location', StringType(), True)
])