import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME','entityname','loadtype','watermark','watermarkcolname'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

Table_Name = args['entityname']
Table_Load_Type = args['loadtype'].strip().upper()
Incremental_Key = args['watermarkcolname']

# Script generated for node PostgreSQL
if Table_Load_Type == "INCREMENTAL":

    Incremental_Key_Value = int(args['watermark'])
    Query_To_Execute = f"SELECT * FROM {Table_Name} WHERE {Incremental_Key} > {Incremental_Key_Value}"

    PostgreSQL_node = glueContext.create_dynamic_frame.from_options(
        connection_type = "postgresql",
        connection_options = {
            "useConnectionProperties": "true",
            "dbtable": Table_Name,
            "connectionName": "Postgresql connection",
            "sampleQuery": Query_To_Execute
        },
        transformation_ctx = f"PostgreSQL_node{Table_Name}"
    )
else:
    PostgreSQL_node = glueContext.create_dynamic_frame.from_options(
        connection_type = "postgresql",
        connection_options = {
            "useConnectionProperties": "true",
            "dbtable": Table_Name,
            "connectionName": "Postgresql connection",
        },
        transformation_ctx = f"PostgreSQL_node{Table_Name}"
    )

# Script generated for node Raw Layer
RawLayer_node1772908919259 = glueContext.write_dynamic_frame.from_options(frame=PostgreSQL_node, connection_type="s3", format="parquet", connection_options={"path": f"s3://arindam-s3-bucket-all-purpose/postgresql-data-raw-layer/{Table_Name}/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="RawLayer_node1772908919259")

job.commit()