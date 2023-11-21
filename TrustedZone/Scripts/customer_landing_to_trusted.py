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

# Script generated for node Customer Landing
CustomerLanding_node1700497909121 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stediproject/customer_landing/"],
        "recurse": True,
    },
    transformation_ctx="CustomerLanding_node1700497909121",
)

# Script generated for node SQL Query
SqlQuery199 = """
select * from customer_landing where sharewithresearchasofdate IS NOT NULL
"""
SQLQuery_node1700499208976 = sparkSqlQuery(
    glueContext,
    query=SqlQuery199,
    mapping={"customer_landing": CustomerLanding_node1700497909121},
    transformation_ctx="SQLQuery_node1700499208976",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1700498491507 = glueContext.getSink(
    path="s3://stediproject/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerTrusted_node1700498491507",
)
CustomerTrusted_node1700498491507.setCatalogInfo(
    catalogDatabase="landing_zone", catalogTableName="customer_trusted"
)
CustomerTrusted_node1700498491507.setFormat("json")
CustomerTrusted_node1700498491507.writeFrame(SQLQuery_node1700499208976)
job.commit()

