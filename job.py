import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1755133287680 = glueContext.create_dynamic_frame.from_catalog(database="weather_db", table_name="weather_raw_data2", transformation_ctx="AWSGlueDataCatalog_node1755133287680")

# Script generated for node Change Schema
ChangeSchema_node1755133361940 = ApplyMapping.apply(frame=AWSGlueDataCatalog_node1755133287680, mappings=[("coord.lon", "double", "coord.lon", "double"), ("coord.lat", "double", "coord.lat", "double"), ("weather", "array", "weather", "array"), ("base", "string", "base", "string"), ("main.temp", "double", "main.temp", "double"), ("main.feels_like", "double", "main.feels_like", "double"), ("main.temp_min", "int", "main.temp_min", "int"), ("main.temp_max", "double", "main.temp_max", "double"), ("main.pressure", "int", "main.pressure", "int"), ("main.humidity", "int", "main.humidity", "int"), ("main.sea_level", "int", "main.sea_level", "int"), ("main.grnd_level", "int", "main.grnd_level", "int"), ("visibility", "int", "visibility", "int"), ("wind.speed", "double", "wind.speed", "double"), ("wind.deg", "int", "wind.deg", "int"), ("clouds.all", "int", "clouds.all", "int"), ("dt", "int", "dt", "int"), ("sys.type", "int", "sys.type", "int"), ("sys.id", "int", "sys.id", "int"), ("sys.country", "string", "sys.country", "string"), ("sys.sunrise", "int", "sys.sunrise", "int"), ("sys.sunset", "int", "sys.sunset", "int"), ("timezone", "int", "timezone", "int"), ("id", "int", "id", "int"), ("name", "string", "name", "string"), ("cod", "int", "cod", "int"), ("timestamp", "string", "timestamp", "string"), ("partition_0", "string", "partition_0", "string"), ("partition_1", "string", "partition_1", "string")], transformation_ctx="ChangeSchema_node1755133361940")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=ChangeSchema_node1755133361940, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1755133161301", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1755133412307 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchema_node1755133361940, connection_type="s3", format="glueparquet", connection_options={"path": "s3://weather-processed-data-2", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1755133412307")

job.commit()