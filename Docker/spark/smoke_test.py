"""Smoke test: Spark can read from Kafka AND write/read MySQL through JDBC.
Run it with the command in docs/environment_test_guide.md (section 7)."""
import os
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("smoke_test").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 1) Read the test topic from Kafka (batch mode)
df = (spark.read.format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9092")
      .option("subscribe", "smoke.test")
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest")
      .load())
n = df.count()
print(f"KAFKA rows read: {n}")

# 2) Write that number to MySQL, then read it back
url = (f"jdbc:mysql://db:3306/{os.environ['DB_NAME']}"
       "?connectionTimeZone=UTC&allowPublicKeyRetrieval=true&useSSL=false")
props = {"user": os.environ["DB_USER"], "password": os.environ["DB_PASSWORD"],
         "driver": "com.mysql.cj.jdbc.Driver"}
spark.createDataFrame([(n,)], ["kafka_rows"]).write.jdbc(url, "smoke_test", "overwrite", props)
back = spark.read.jdbc(url, "smoke_test", properties=props).collect()[0]["kafka_rows"]
print(f"MYSQL value read back: {back}")

assert n > 0 and back == n, "SMOKE TEST FAILED"
print("SMOKE TEST PASSED")
spark.stop()
