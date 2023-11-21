import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1700504704570 = glueContext.create_dynamic_frame.from_catalog(
    database="landing_zone",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLanding_node1700504704570",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1700504794048 = glueContext.create_dynamic_frame.from_catalog(
    database="landing_zone",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1700504794048",
)

# Script generated for node Customer Privacy Filter
CustomerPrivacyFilter_node1700504920059 = Join.apply(
    frame1=AccelerometerLanding_node1700504704570,
    frame2=CustomerTrusted_node1700504794048,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="CustomerPrivacyFilter_node1700504920059",
)

# Script generated for node Drop Fields
DropFields_node1700505537206 = DropFields.apply(
    frame=CustomerPrivacyFilter_node1700504920059,
    paths=[
        "customername",
        "email",
        "phone",
        "birthday",
        "serialnumber",
        "registrationdate",
        "lastupdatedate",
        "sharewithpublicasofdate",
        "sharewithresearchasofdate",
        "sharewithfriendsasofdate",
    ],
    transformation_ctx="DropFields_node1700505537206",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1700505219547 = glueContext.getSink(
    path="s3://stediproject/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AccelerometerTrusted_node1700505219547",
)
AccelerometerTrusted_node1700505219547.setCatalogInfo(
    catalogDatabase="landing_zone", catalogTableName="accelerometer_trusted"
)
AccelerometerTrusted_node1700505219547.setFormat("json")
AccelerometerTrusted_node1700505219547.writeFrame(DropFields_node1700505537206)
job.commit()
