import pyspark.sql.functions as F
df = spark.table("default.nyctaxi_trips_clone")
df = df.filter(F.col("trip_distance") > 10)
df.printSchema()
df.show(10)

## add for data output in cloud storage of s3
#added via vs code
df_to_write = df.limit(100)

s3_bucket_path = 's3://mydbricks-bucket/dbricks-output'
try:
    df_to_write.write.parquet(s3_bucket_path, mode='overwrite')
    print(f"Save in s3 successful")
except Exception as e:
    print(f'str{e}')