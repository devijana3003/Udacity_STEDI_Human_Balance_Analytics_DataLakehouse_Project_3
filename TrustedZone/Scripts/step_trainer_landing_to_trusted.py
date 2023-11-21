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

# Script generated for node step_trainer
step_trainer_node1700518860172 = glueContext.create_dynamic_frame.from_catalog(
    database="landing_zone",
    table_name="step_trainer",
    transformation_ctx="step_trainer_node1700518860172",
)

# Script generated for node customer_curated
customer_curated_node1700518755975 = glueContext.create_dynamic_frame.from_catalog(
    database="landing_zone",
    table_name="customer_curated",
    transformation_ctx="customer_curated_node1700518755975",
)

# Script generated for node SQL Query
SqlQuery271 = """
select * from customer_curated
inner join step_trainer on customer_curated.serialnumber=step_trainer.serialnumber
"""
SQLQuery_node1700524067990 = sparkSqlQuery(
    glueContext,
    query=SqlQuery271,
    mapping={
        "step_trainer": step_trainer_node1700518860172,
        "customer_curated": customer_curated_node1700518755975,
    },
    transformation_ctx="SQLQuery_node1700524067990",
)

# Script generated for node Drop Fields
DropFields_node1700524290881 = DropFields.apply(
    frame=SQLQuery_node1700524067990,
    paths=[
        "birthday",
        "sharewithpublicasofdate",
        "sharewithresearchasofdate",
        "registrationdate",
        "customername",
        "sharewithfriendsasofdate",
        "email",
        "lastupdatedate",
        "phone",
    ],
    transformation_ctx="DropFields_node1700524290881",
)

# Script generated for node Step_trainer_trusted
Step_trainer_trusted_node1700519223903 = glueContext.getSink(
    path="s3://stediproject/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="Step_trainer_trusted_node1700519223903",
)
Step_trainer_trusted_node1700519223903.setCatalogInfo(
    catalogDatabase="landing_zone", catalogTableName="step_trainer_trusted"
)
Step_trainer_trusted_node1700519223903.setFormat("json")
Step_trainer_trusted_node1700519223903.writeFrame(DropFields_node1700524290881)
job.commit()
