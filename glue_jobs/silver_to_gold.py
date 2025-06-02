import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col,when,round,sum,count,avg

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load from silver_final data
df = spark.read.json("s3://bank-marketing-bronze/silver_final/")

#-------------------------------------
# 1. Job Title vs Deposit count
#-------------------------------------

job_deposit = df.groupBy("job_title").agg( 
    count("*").alias("total_customers"),
    sum(when(col("deposit") == True, 1).otherwise(0)).alias("deposits")
)
job_deposit = job_deposit.coalesce(1)
job_deposit_out = DynamicFrame.fromDF(job_deposit, glueContext, "job_deposit_out")
glueContext.write_dynamic_frame.from_options(
    frame=job_deposit_out,
    connection_type="s3",
    connection_options={"path": "s3://bank-marketing-bronze/gold/job_vs_deposit/"},
    format="json"
)

# --------------------------------------
# 2. Monthly Conversion Rate
# --------------------------------------
month_conv = df.groupBy("month").agg(
    count("*").alias("contacts"),
    sum(when(col("deposit") == True, 1).otherwise(0)).alias("deposits"),
    round(
        (sum(when(col("deposit") == True, 1).otherwise(0)) * 100.0) / count("*"), 2
    ).alias("conversion_rate")
)
month_conv = month_conv.coalesce(1)
month_conv_out = DynamicFrame.fromDF(month_conv, glueContext, "month_conv_out")
glueContext.write_dynamic_frame.from_options(
    frame=month_conv_out,
    connection_type="s3",
    connection_options={"path": "s3://bank-marketing-bronze/gold/month_conversion/"},
    format="json"
)

# --------------------------------------------------
# 3. Age Group Performance
# --------------------------------------------------
age_group = df.withColumn(
    "age_group",
    when(col("age") < 30, "Under 30")
    .when((col("age") >= 30) & (col("age") <= 50), "30–50")
    .otherwise("Above 50")
)

age_perf = age_group.groupBy("age_group").agg(
    count("*").alias("total_customers"),
    round(avg("campaign"), 2).alias("avg_campaign_attempts"),
    sum(when(col("deposit") == True, 1).otherwise(0)).alias("deposits")
)
age_perf = age_perf.coalesce(1)
age_perf_out = DynamicFrame.fromDF(age_perf, glueContext, "age_perf_out")
glueContext.write_dynamic_frame.from_options(
    frame=age_perf_out,
    connection_type="s3",
    connection_options={"path": "s3://bank-marketing-bronze/gold/age_group_perf/"},
    format="json"
)

# --------------------------------------------------
# 4. Education vs Loan Distribution
# --------------------------------------------------
edu_loan = df.groupBy("education_level").agg(
    sum(when(col("loan") == True, 1).otherwise(0)).alias("loans"),
    count("*").alias("total"),
    round(
        (sum(when(col("loan") == True, 1).otherwise(0)) * 100.0) / count("*"), 2
    ).alias("loan_percentage")
)
edu_loan = edu_loan.coalesce(1)
edu_loan_out = DynamicFrame.fromDF(edu_loan, glueContext, "edu_loan_out")
glueContext.write_dynamic_frame.from_options(
    frame=edu_loan_out,
    connection_type="s3",
    connection_options={"path": "s3://bank-marketing-bronze/gold/edu_loan_stats/"},
    format="json"
)   

job.commit()