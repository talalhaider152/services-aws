import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Initialize contexts and job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Sample data frame
data = [("Alice", 34), ("Bob", 28), ("Charlie", 45)]
columns = ["name", "age"]
df = spark.createDataFrame(data, columns)

# Write to S3 in CSV format (replace with your bucket)
df.write.mode("overwrite").csv("s3://your-output-bucket/output/")

job.commit()
