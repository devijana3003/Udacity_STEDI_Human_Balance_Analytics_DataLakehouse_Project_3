import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1700525291747 = glueContext.create_dynamic_frame.from_catalog(
    database="landing_zone",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_trusted_node1700525291747",
)

# Script generated for node step trainer trusted
steptrainertrusted_node1700525306485 = glueContext.create_dynamic_frame.from_catalog(
    database="landing_zone",
    table_name="step_trainer_trusted",
    transformation_ctx="steptrainertrusted_node1700525306485",
)

# Script generated for node SQL Query
SqlQuery241 = """
select * from accelerometer_trusted inner join step_trainer_trusted on accelerometer_trusted.timestamp = step_trainer_trusted.sensorreadingtime
"""
SQLQuery_node1700525309898 = sparkSqlQuery(
    glueContext,
    query=SqlQuery241,
    mapping={
        "step_trainer_trusted": steptrainertrusted_node1700525306485,
        "accelerometer_trusted": accelerometer_trusted_node1700525291747,
    },
    transformation_ctx="SQLQuery_node1700525309898",
)

# Script generated for node machine_learning curated
machine_learningcurated_node1700525316691 = glueContext.getSink(
    path="s3://stediproject/machinelearning/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="machine_learningcurated_node1700525316691",
)
machine_learningcurated_node1700525316691.setCatalogInfo(
    catalogDatabase="landing_zone", catalogTableName="machine_learning_curated"
)
machine_learningcurated_node1700525316691.setFormat("json")
machine_learningcurated_node1700525316691.writeFrame(SQLQuery_node1700525309898)
job.commit()
