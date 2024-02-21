import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import re
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1706204472650 = glueContext.create_dynamic_frame.from_catalog(
    database="bgg-database",
    table_name="bgg_transformed_classification",
    transformation_ctx="AWSGlueDataCatalog_node1706204472650",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1706200220224 = glueContext.create_dynamic_frame.from_catalog(
    database="bgg-database",
    table_name="bgg_transformed",
    transformation_ctx="AWSGlueDataCatalog_node1706200220224",
)

# Script generated for node Filter
Filter_node1706206011410 = Filter.apply(
    frame=AWSGlueDataCatalog_node1706204472650,
    f=lambda row: (
        bool(re.match("boardgamemechanic", row["classification"]))
        or bool(re.match("boardgamecategory", row["classification"]))
    ),
    transformation_ctx="Filter_node1706206011410",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1706200291867 = DynamicFrame.fromDF(
    AWSGlueDataCatalog_node1706200220224.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1706200291867",
)

# Script generated for node Filter
Filter_node1706200414982 = Filter.apply(
    frame=DropDuplicates_node1706200291867,
    f=lambda row: (
        bool(re.match("boardgame", row["bgg_type"]))
        and row["year_published"] < 2024
        and row["bgg_rank"] > 0
        and row["bgg_rank"] <= 5000
    ),
    transformation_ctx="Filter_node1706200414982",
)

# Script generated for node Drop Fields
DropFields_node1706200578067 = DropFields.apply(
    frame=Filter_node1706200414982,
    paths=[
        "day",
        "month",
        "year",
        "subdomain_2_rank",
        "subdomain_2",
        "subdomain_1_rank",
        "subdomain_1",
        "num_weights",
        "bayes_average",
        "bgg_type",
        "img_src",
        "description",
        "playing_time",
        "min_age",
    ],
    transformation_ctx="DropFields_node1706200578067",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1706206276647 = ApplyMapping.apply(
    frame=DropFields_node1706200578067,
    mappings=[
        ("bgg_id", "string", "bgg_id", "string"),
        ("name", "string", "name", "string"),
    ],
    transformation_ctx="RenamedkeysforJoin_node1706206276647",
)

# Script generated for node Join
Join_node1706206084800 = Join.apply(
    frame1=RenamedkeysforJoin_node1706206276647,
    frame2=Filter_node1706206011410,
    keys1=["bgg_id"],
    keys2=["bgg_id"],
    transformation_ctx="Join_node1706206084800",
)

# Script generated for node Drop Fields
DropFields_node1706206238063 = DropFields.apply(
    frame=Join_node1706206084800,
    paths=["`.bgg_id`"],
    transformation_ctx="DropFields_node1706206238063",
)

# Script generated for node Amazon S3
AmazonS3_node1706200889119 = glueContext.getSink(
    path="s3://bgg-analytics-apsoutheast1-dev",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["date", "type"],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1706200889119",
)
AmazonS3_node1706200889119.setCatalogInfo(
    catalogDatabase="bgg-database", catalogTableName="bgg_analytics"
)
AmazonS3_node1706200889119.setFormat("glueparquet", compression="snappy")
AmazonS3_node1706200889119.writeFrame(DropFields_node1706200578067)
# Script generated for node Amazon S3
AmazonS3_node1706206439647 = glueContext.getSink(
    path="s3://bgg-analytics-apsoutheast1-dev",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["date", "type", "classification"],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1706206439647",
)
AmazonS3_node1706206439647.setCatalogInfo(
    catalogDatabase="bgg-database", catalogTableName="bgg_analytics_classification"
)
AmazonS3_node1706206439647.setFormat("glueparquet", compression="snappy")
AmazonS3_node1706206439647.writeFrame(DropFields_node1706206238063)
job.commit()
