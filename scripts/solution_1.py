import json
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *

corrupted_json_file = open("src/files/corrupted-file.json")
corrupted_json_str = corrupted_json_file.read()

f = open("src/files/uncorrupted.json", "w")
f.write(json.loads(json.dumps(f"[{corrupted_json_str}]")))
f.close()


json_file_path = 'src/files/uncorrupted.json'

spark = SparkSession \
    .builder \
    .appName("Ntt Data Case Solution") \
    .config("spark.jars", 'src/jars/postgresql-42.6.0.jar') \
    .getOrCreate()

df = spark \
  .read \
  .option("multiline", "true") \
  .json(json_file_path)
# corrupted-file.json
df.printSchema()
df.show()

sensor_names = df \
    .select(explode("measurements")) \
    .select("col.series.*") \
    .schema \
    .fieldNames()[1:]

full_data_arr = df \
  .select(
      "content-spec",
      "device.deviceID",
      explode("device.metaData.cloudGateway.awsTarget").alias("awsTarget"),
      "device.metaData.cloudGateway.hostName",
      "device.metaData.cloudGateway.splitMeasurements",
      "device.metaData.cloudGateway.subscriptionTopic",
      "measurements"
    ) \
  .withColumn(
      "explode_measurements", explode("measurements")
  ) \
  .select(
      "content-spec",
      "deviceID",
      "awsTarget",
      "hostName",
      "splitMeasurements",
      "subscriptionTopic",
      "explode_measurements.ts",
      "explode_measurements.series"
  ) \
  .collect()
full_data_arr[:2]

manupilated_arr = []

for row in full_data_arr:
  content_spec = row["content-spec"]
  deviceID = row["deviceID"]
  awsTarget = row["awsTarget"]
  hostName = row["hostName"]
  splitMeasurements = row["splitMeasurements"]
  subscriptionTopic = row["subscriptionTopic"]
  timestamp = row["ts"]

  dollar_times = row["series"]["$_time"]

  sensor_name = ""
  sensor_values = ""

  for col_name in sensor_names:
    if row["series"][col_name] is not None:
      sensor_name = col_name
      sensor_values = row["series"][col_name]
  manupilated_arr.append([
    content_spec,
    deviceID,
    awsTarget,
    hostName,
    splitMeasurements,
    subscriptionTopic,
    timestamp, 
    dollar_times, 
    sensor_name, 
    sensor_values
  ])

schema = StructType([
    StructField('content-spec', StringType()),
    StructField('deviceID', StringType()),
    StructField('awsTarget', StringType()),
    StructField('hostName', StringType()),
    StructField('splitMeasurements', BooleanType()),
    StructField('subscriptionTopic', StringType()),
    StructField('str_timestamp', StringType()),
    StructField('dollar_times', ArrayType(IntegerType())),
    StructField('sensor_name', StringType()),
    StructField('sensor_values', ArrayType(StringType())),
])

manupilated_df = spark \
    .createDataFrame(manupilated_arr, schema) \
    .withColumn("timestamp", to_timestamp("str_timestamp")) \
    .withColumn("sensor_name", regexp_replace("sensor_name", ".ab", "")) \
    .withColumn("explode_zipped_col", explode(arrays_zip("dollar_times", "sensor_values"))) \
    .drop("dollar_times", "sensor_values", "str_timestamp") \
    .select(
        "content-spec",
        "deviceID",
        "awsTarget",
        "hostName",
        "splitMeasurements",
        "subscriptionTopic",
        "timestamp",
        col("explode_zipped_col.dollar_times").alias("dollar_time"),
        "sensor_name",
        col("explode_zipped_col.sensor_values").alias("sensor_value")
    )
    
manupilated_df.printSchema()
manupilated_df.show(10, False)

format_ = 'jdbc'
url = 'jdbc:postgresql://localhost:5432/ntt'
table_name = 'table1'
user = 'ntt' 
password = 'ntt'
driver = 'org.postgresql.Driver'

manupilated_df \
    .write \
    .format(format_) \
    .option("url", url) \
    .option("dbtable", table_name) \
    .option("user", user) \
    .option("password", password) \
    .option("driver", driver) \
    .mode('overwrite') \
    .save()
