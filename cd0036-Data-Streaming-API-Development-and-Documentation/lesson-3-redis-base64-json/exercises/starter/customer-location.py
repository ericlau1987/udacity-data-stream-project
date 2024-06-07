from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType

# this is a manually created schema - before Spark 3.0.0, schema inference is not automatic

redisMessageSchema = StructType(
    [
        StructField("key", StringType()),
        StructField("value", StringType()),
        StructField("expiredType", StringType()),
        StructField("expiredValue",StringType()),
        StructField("existType", StringType()),
        StructField("ch", StringType()),
        StructField("incr",BooleanType()),
        StructField("zSetEntries", ArrayType( \
            StructType([
                StructField("element", StringType()),\
                StructField("score", StringType())   \
            ]))                                      \
        )
    ]
)


# this is a manually created schema - before Spark 3.0.0, schema inference is not automatic
# since we are not using the date or the amount in sql calculations, we are going
# to cast them as strings
# {"accountNumber":"703934969","amount":625.8,"dateAndTime":"Sep 29, 2020, 10:06:23 AM","transactionId":1601395583682}
customerLocationSchema = StructType (
    [
        StructField("accountNumber", StringType()),
        StructField("location", StringType()),
    ]   
)


# the source for this data pipeline is a kafka topic, defined below
spark = SparkSession.builder.appName("customer-location").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

redisServerRawStreamingDF = spark                          \
    .readStream                                          \
    .format("kafka")                                     \
    .option("kafka.bootstrap.servers", "kafka:19092") \
    .option("subscribe","redis-server")                  \
    .option("startingOffsets","earliest")\
    .load()                                     

#it is necessary for Kafka Data Frame to be readable, to cast each field from a binary to a string
redisServerStreamingDF = redisServerRawStreamingDF.selectExpr("cast(key as string) key", "cast(value as string) value")

# this creates a temporary streaming view based on the streaming dataframe
# it can later be queried with spark.sql, we will cover that in the next section 
redisServerStreamingDF.withColumn("value",from_json("value",redisMessageSchema))\
        .select(col('value.*')) \
        .createOrReplaceTempView("RedisData")

# Using spark.sql we can select any valid select statement from the spark view
zSetEntriesEncodedStreamingDF=spark.sql("select key, zSetEntries[0].element as customerLocation from RedisData")

zSetDecodedEntriesStreamingDF= zSetEntriesEncodedStreamingDF.withColumn("customerLocation", unbase64(zSetEntriesEncodedStreamingDF.customerLocation).cast("string"))

zSetDecodedEntriesStreamingDF\
    .withColumn("customerLocation", from_json("customerLocation", customerLocationSchema))\
    .select(col('customerLocation.*'))\
    .createOrReplaceTempView("CustomerLocation")\

customerLocationStreamingDF = spark.sql("select * from CustomerLocation where location is not null")

# this takes the stream and "sinks" it to the console as it is updated one message at a time (null means the JSON parsing didn't match the fields in the schema):

# +-------------+---------+
# |accountNumber| location|
# +-------------+---------+
# |         null|     null|
# |     93618942|  Nigeria|
# |     55324832|   Canada|
# |     81128888|    Ghana|
# |    440861314|  Alabama|
# |    287931751|  Georgia|
# |    413752943|     Togo|
# |     93618942|Argentina|
# +-------------+---------+

customerLocationStreamingDF.writeStream.outputMode("append").format("console").start().awaitTermination()

