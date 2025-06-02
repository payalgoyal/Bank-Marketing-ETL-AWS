import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, trim, lower, when, regexp_replace

from pyspark.sql import functions as F

# Define a UDF for mapping
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Create a dictionary of known replacements
job_mapping = {
    "admin.": "admin",
    "self-employed": "selfemployed",
    "blue-collar": "bluecollar",
    "entrepreneur": "business",
    "retired": "retired",
    "unemployed": "unemployed",
    "services": "service",
    "housemaid": "homemaker",
    # Add more variations if needed
}

# UDF to replace values based on the mapping
def standardize_job(job):
    if job:
        cleaned = job.strip().lower().replace("-", "").replace(".", "").replace(" ", "")
        return job_mapping.get(cleaned, cleaned)
    return None

standardize_job_udf = udf(standardize_job, StringType())

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load silver_cleaned data (from S3, not catalog)
input_path = "s3://bank-marketing-bronze/silver_cleaned/"
df = spark.read.json(input_path)

# ---------------------------------------------
# Advanced Transformations
# ---------------------------------------------

# Clean and normalize fields
df_cleaned = df \
    .withColumn("job", trim(lower(col("job")))) \
    .withColumn("marital", trim(lower(col("marital")))) \
    .withColumn("education", trim(lower(col("education")))) \
    .withColumn("contact", trim(lower(col("contact")))) \
    .withColumn("month", trim(lower(col("month")))) \
    .withColumn("housing", when(col("housing") == "yes", True).otherwise(False)) \
    .withColumn("loan", when(col("loan") == "yes", True).otherwise(False)) \
    .withColumn("deposit", when(col("deposit") == "yes", True).otherwise(False)) \
    .withColumn("age", col("age").cast("int")) \
    .withColumn("balance", col("balance").cast("double")) \
    .withColumn("duration", col("duration").cast("int")) \
    .withColumn("campaign", col("campaign").cast("int"))
    
# Drop unnecessary fields
columns_to_drop = ["pdays", "previous", "contact"]
df_cleaned = df_cleaned.drop(*columns_to_drop)

# Apply this to your Spark DataFrame
df_cleaned = df_cleaned.withColumn("job", standardize_job_udf(col("job")))

# Rename columns for clarity
df_cleaned = df_cleaned \
    .withColumnRenamed("job", "job_title") \
    .withColumnRenamed("marital", "marital_status") \
    .withColumnRenamed("education", "education_level")
    
# ---------------------------------------------
# Write to Silver Final Layer
# ---------------------------------------------
df_final = df_cleaned.coalesce(1)  # compact to 1 file

# Convert to DynamicFrame
dyf_final = DynamicFrame.fromDF(df_final, glueContext, "dyf_final")

glueContext.write_dynamic_frame.from_options(
    frame=dyf_final,
    connection_type="s3",
    connection_options={"path": "s3://bank-marketing-bronze/silver_final/"},
    format="json"
)
job.commit()