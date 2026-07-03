import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Parámetros del Job
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

# Contextos de Glue y Spark
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# 1. Leer tabla del Data Catalog (MySQL ya mapeado por un Crawler)
products = glueContext.create_dynamic_frame.from_catalog(
    database="db-rds1",
    table_name="glue_customers",
    transformation_ctx="datasource"
)

# 5. Guardar el resultado en Amazon S3 en formato Parquet
output = glueContext.write_dynamic_frame.from_options(  
    frame=products,
    connection_type="s3",
    connection_options={"path": "s3://danieljara-s3-glue-primer-job-input/data-scripts/"},
    format="csv",
    transformation_ctx="output"
)

job.commit()