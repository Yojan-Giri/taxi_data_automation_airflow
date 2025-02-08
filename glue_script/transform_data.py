import sys
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, hour, dayofweek, dayofmonth, 
    unix_timestamp, round, avg, sum, percentile_approx
)
from pyspark.sql.window import Window
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions

# Retrieve job arguments
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "DATABASE_NAME",
        "RAW_TABLE_NAME",
        "PROCESSED_TABLE_NAME",
        "OUTPUT_S3_PATH",
    ]
)

# Assign arguments to variables
job_name = args["JOB_NAME"]
database_name = args["DATABASE_NAME"]
raw_table_name = args["RAW_TABLE_NAME"]
processed_table_name = args["PROCESSED_TABLE_NAME"]
output_s3_path = args["OUTPUT_S3_PATH"]

# Initialize Glue and Spark Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(job_name, args)

# Read data from Glue Catalog
df = glueContext.create_dynamic_frame.from_catalog(
    database=database_name, table_name=raw_table_name
).toDF()

# Clean the data
df_cleaned = (
    df.dropDuplicates()
    .withColumn("pickup_datetime", col("tpep_pickup_datetime").cast("timestamp"))
    .withColumn("dropoff_datetime", col("tpep_dropoff_datetime").cast("timestamp"))
    .withColumn("trip_distance", col("trip_distance").cast("float"))
    .withColumn("fare_amount", col("fare_amount").cast("float"))
    .withColumn("tip_amount", col("tip_amount").cast("float"))
    .withColumn("passenger_count", col("passenger_count").cast("int"))
    .withColumn("payment_type", col("payment_type").cast("int"))
    .filter((col("fare_amount") >= 0) & (col("trip_distance") >= 0) & (col("passenger_count") >= 1))
)

# Calculate the 95th percentile for fare_amount to identify outliers
# We will use the percentile_approx function to approximate the 95th percentile
fare_percentile = df_cleaned.approxQuantile("fare_amount", [0.95], 0.01)[0]

# Filter out trips with fare_amount higher than the 95th percentile
df_cleaned = df_cleaned.filter(col("fare_amount") <= fare_percentile)

# Feature Engineering
# Adding time-based features
df_features = (
    df_cleaned
    .withColumn("pickup_hour", hour(col("pickup_datetime")))
    .withColumn("pickup_day_of_week", dayofweek(col("pickup_datetime")))
    .withColumn("pickup_day_of_month", dayofmonth(col("pickup_datetime")))  # Added day of month for more detailed analysis
    .withColumn("is_weekend", when(col("pickup_day_of_week").isin(1, 7), 1).otherwise(0))  # Flag weekends (1 = weekend, 0 = weekday)
)

# Geospatial Feature - Categorizing trips by distance
df_features = df_features.withColumn(
    "trip_category",
    when(col("trip_distance") < 2, "short")
    .when((col("trip_distance") >= 2) & (col("trip_distance") <= 10), "medium")
    .otherwise("long")
)

# Fare Engineering
df_features = df_features.withColumn(
    "tip_percentage",
    when(col("fare_amount") > 0, (col("tip_amount") / col("fare_amount")) * 100).otherwise(0)
)

# Trip Duration
df_features = df_features.withColumn(
    "trip_duration_minutes",
    (unix_timestamp(col("dropoff_datetime")) - unix_timestamp(col("pickup_datetime"))) / 60
)

# Trip Classification
df_features = df_features.withColumn(
    "trip_type",
    when((col("dropoff_latitude").between(40.63, 40.65)) & 
         (col("dropoff_longitude").between(-73.78, -73.76)), "JFK")
    .when((col("dropoff_latitude").between(40.77, 40.78)) & 
         (col("dropoff_longitude").between(-73.88, -73.86)), "LGA")
    .otherwise("Regular")
)

# Shared Ride Flag
df_features = df_features.withColumn(
    "is_shared_ride", when(col("passenger_count") > 1, 1).otherwise(0)
)

# Mapping Payment Types to Descriptions
df_features = df_features.withColumn(
    "payment_type_desc",
    when(col("payment_type") == 1, "Credit Card")
    .when(col("payment_type") == 2, "Cash")
    .when(col("payment_type") == 3, "No Charge")
    .when(col("payment_type") == 4, "Dispute")
    .when(col("payment_type") == 5, "Unknown")
    .when(col("payment_type") == 6, "Voided Trip")
    .otherwise("Other")  # If we encounter an unexpected payment type, label it as "Other"
)

# Cumulative Sum of Fare Amount
window_spec = Window.orderBy("pickup_day_of_month")
df_features = df_features.withColumn("cumulative_fare", sum("fare_amount").over(window_spec))

# Final Cleanup - Remove unnecessary columns
columns_to_keep = [
    "VendorID",  # Vendor ID, keep it for identification purposes
    "pickup_datetime",
    "dropoff_datetime",
    "pickup_day_of_month",  # Add day of month for better analysis by date
    "trip_distance",
    "trip_duration_minutes",  # Duration of the trip in minutes
    "fare_amount",
    "cumulative_fare",  # Cumulative fare amount, useful for revenue analysis
    "tip_percentage",
    "payment_type_desc",  # Descriptive payment type
    "pickup_hour",
    "pickup_day_of_week",
    "is_weekend",
    "trip_category",
    "trip_type",
    "is_shared_ride",
]

df_final = df_features.select(columns_to_keep)

# Convert DataFrame back to DynamicFrame for Glue
final_dynamic_frame = DynamicFrame.fromDF(df_final, glueContext)

#Saving the file in S3 and Glue Catalog
s3output = glueContext.getSink(
  path=output_s3_path,
  connection_type="s3",
  updateBehavior="UPDATE_IN_DATABASE",
  partitionKeys=[],
  compression="snappy",
  enableUpdateCatalog=True,
  transformation_ctx="s3output",
)
s3output.setCatalogInfo(
  catalogDatabase=database_name, catalogTableName=processed_table_name
)
s3output.setFormat("glueparquet")
s3output.writeFrame(final_dynamic_frame)
job.commit()