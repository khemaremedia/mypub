import pyspark.sql.functions as F
df = spark.table("default.nyctaxi_trips_clone")
df = df.filter(F.col("trip_distance") > 10)
df.printSchema()
df.show(10)