import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.transforms import DropNullFields
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# Setup
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Replace with your actual database and table names
bronze_db = "bank-project"
bronze_table = "bronze_bronze"

# Load data from Glue Catalog
bronze_df = glueContext.create_dynamic_frame.from_catalog(
    database=bronze_db,
    table_name=bronze_table
)

# Clean data: Drop rows with nulls
bronze_cleaned = DropNullFields.apply(bronze_df)

# Convert to DataFrame to coalesce and compact the output
df = bronze_cleaned.toDF()

# Coalesce to fewer partitions (e.g., 1 file)
df_compacted = df.coalesce(1)  # or use .coalesce(5) for medium size

# Convert back to DynamicFrame
df_final = DynamicFrame.fromDF(df_compacted, glueContext, "df_final")

# Write to Silver folder
glueContext.write_dynamic_frame.from_options(
    frame=df_final,
    connection_type="s3",
    connection_options={
        "path": "s3://bank-marketing-bronze/silver_cleaned/"
    },
    format="json"
)

job.commit()
