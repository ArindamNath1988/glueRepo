import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME','S3_Source_Path','S3_Destination_Path'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

S3_Source_Path = args['S3_Source_Path']
S3_Destination_Path = args['S3_Destination_Path']

# Script generated for node External-Data-Source
ExternalDataSource = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": "\"", "withHeader": True, "separator": ","}, 
    connection_type="s3", 
    format="csv", 
    connection_options={"paths": [S3_Source_Path]}, 
    transformation_ctx="ExternalDataSource")

#Purge before writting
glueContext.purge_s3_path(S3_Destination_Path, {"retentionPeriod": 0})

# Script generated for node External-Raw-Layer
ExternalRawLayer = glueContext.write_dynamic_frame.from_options(
    frame=ExternalDataSource, 
    connection_type="s3", 
    format="parquet", 
    connection_options={"path": S3_Destination_Path, "partitionKeys": []}, 
    format_options={"compression": "snappy"}, 
    transformation_ctx="ExternalRawLayer")

job.commit()

# Delete the source file after successful processing
glueContext.purge_s3_path(S3_Source_Path, {"retentionPeriod": 0})
print(f"Successfully processed and deleted: {S3_Source_Path}")