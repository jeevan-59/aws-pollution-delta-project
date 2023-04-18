import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from delta.tables import DeltaTable
from pyspark.sql.functions import *

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'src_database', 'src_bucket_name', 'tgt_bucket_name', 'area'])
p_src_database    = args['src_database']     # "plltndelta-land-db"
p_src_bucket_name = args['src_bucket_name']  # "plltndelta-landing-layer"
p_tgt_bucket_name = args['tgt_bucket_name']  # "plltndelta-bronze-layer"
p_area            = args['area']             # "sofia_area"

temp_dir = f"s3://{p_src_bucket_name}/temp/"
s3_path  = f"s3://{p_tgt_bucket_name}/{p_area}/"



sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#input
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = p_src_database, table_name = f"land_{p_area}", transformation_ctx = "datasource0")

# transform JSON files to flatten structure
dfc = datasource0.relationalize("root", temp_dir)

# Join flatten structure  
dyf_out   = dfc.select("root").join("`data.attributions`", 'id_del', dfc.select("root_data.attributions"       ).rename_field("id", "id_del")).drop_fields(["id_del"])
dyf_final = dyf_out.join("`data.city.geo`"               , 'id_del', dfc.select("root_data.city.geo"           ).rename_field("id", "id_del")).drop_fields(["id_del", "`data.forecast.daily.o3`", "`data.forecast.daily.uvi`", "`data.forecast.daily.pm10`", "`data.forecast.daily.pm25`"])

# Avoiding dots column names 
df = dyf_final.toDF()
clean_df = df.toDF(*(c.replace('.', '_') for c in df.columns))

# adding partition date column
clean_df = clean_df.withColumn("part_date",to_date(col("data_time_iso")))

# Loading data
if DeltaTable.isDeltaTable(spark, s3_path):
    # Upsert
    deltaTable = DeltaTable.forPath(spark, s3_path)
    deltaTable.alias("old") \
      .merge( clean_df.alias("new"), "old.data_idx = new.data_idx AND old.rootroot_data_attributions_index = new.rootroot_data_attributions_index AND old.index = new.index AND old.part_date = new.part_date" ) \
      .whenMatchedUpdateAll() \
      .whenNotMatchedInsertAll() \
      .execute()
      
    # Generate MANIFEST file for Athena/Catalog
    deltaTable.generate("symlink_format_manifest")
else:
    # initial load
    clean_df.write.format("delta").partitionBy("part_date").mode("overwrite").save(s3_path)
    
    # Generate MANIFEST file for Athena/Catalog
    deltaTable = DeltaTable.forPath(spark, s3_path)
    deltaTable.generate("symlink_format_manifest")

#moving ingested files to archive
s3 = boto3.resource('s3')
bucket = s3.Bucket(p_src_bucket_name)

for objects in bucket.objects.filter(Prefix=f"{p_area}/"):
    copy_source = {
          'Bucket': p_src_bucket_name,
          'Key': objects.key
        }

    s3.Object(p_src_bucket_name,f"archive/{objects.key}").copy_from(CopySource=copy_source)
    s3.Object(p_src_bucket_name,objects.key).delete()

#purge temp_dir locations older than 48 hour
glueContext.purge_s3_path(s3_path = temp_dir, options={"retentionPeriod": 48} )

job.commit()
