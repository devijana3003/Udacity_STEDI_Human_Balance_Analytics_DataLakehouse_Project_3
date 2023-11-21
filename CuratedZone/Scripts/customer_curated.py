import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node CustomerTrusted
CustomerTrusted_node1700517172989 = glueContext.create_dynamic_frame.from_catalog(
    database="landing_zone",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1700517172989",
)

# Script generated for node Accelerometer_Landing
Accelerometer_Landing_node1700517201597 = glueContext.create_dynamic_frame.from_catalog(
    database="landing_zone",
    table_name="accelerometer_landing",
    transformation_ctx="Accelerometer_Landing_node1700517201597",
)

# Script generated for node Join
Join_node1700517232751 = Join.apply(
    frame1=Accelerometer_Landing_node1700517201597,
    frame2=CustomerTrusted_node1700517172989,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1700517232751",
)

# Script generated for node Drop Fields
DropFields_node1700517262876 = DropFields.apply(
    frame=Join_node1700517232751,
    paths=["user", "timestamp", "x", "y", "z"],
    transformation_ctx="DropFields_node1700517262876",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1700518175434 = DynamicFrame.fromDF(
    DropFields_node1700517262876.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1700518175434",
)

# Script generated for node Customer Curated
CustomerCurated_node1700517333019 = glueContext.getSink(
    path="s3://stediproject/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerCurated_node1700517333019",
)
CustomerCurated_node1700517333019.setCatalogInfo(
    catalogDatabase="landing_zone", catalogTableName="customer_curated"
)
CustomerCurated_node1700517333019.setFormat("json")
CustomerCurated_node1700517333019.writeFrame(DropDuplicates_node1700518175434)
job.commit()
