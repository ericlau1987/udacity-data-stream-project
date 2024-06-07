from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType, IntegerType

# TO-DO: create bank withdrawals kafka message schema StructType including the following JSON elements:
#  {"accountNumber":"703934969","amount":625.8,"dateAndTime":"Sep 29, 2020, 10:06:23 AM","transactionId":1601395583682}
bankWithdrawalsSchema = StructType(
    [
        StructField("accountNumber", StringType()),
        StructField("amount", IntegerType()),
        StructField("dateAndTime", StringType()),
        StructField("transactionId", StringType())
    ]
)
# TO-DO: create an atm withdrawals kafka message schema StructType including the following JSON elements:
# {"transactionDate":"Sep 29, 2020, 10:06:23 AM","transactionId":1601395583682,"atmLocation":"Thailand"}
atmWithdrawalsSchema = StructType(
    [
        StructField("transactionDate", StringType()),
        StructField("transactionId", StringType()),
        StructField("atmLocation", StringType())
    ]
)
# TO-DO: create a spark session, with an appropriately named application name
spark = SparkSession.builder.appName("atm-bank-withdrawals").getOrCreate()
#TO-DO: set the log level to WARN
spark.sparkContext.setLogLevel("WARN")
#TO-DO: read the bank-withdrawals kafka topic as a source into a streaming dataframe with the bootstrap server kafka:19092, configuring the stream to read the earliest messages possible                                    
sparkBankWithdrawalsRawStreamDF = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:19092") \
    .option("subscribe","bank-withdrawals")  \
    .option("startingOffsets", "earliest") \
    .load()
#TO-DO: using a select expression on the streaming dataframe, cast the key and the value columns from kafka as strings, and then select them
sparkBankWithdrawalsStreamDF = sparkBankWithdrawalsRawStreamDF.selectExpr("cast(key as string) as key", "cast(value as string) as value")
#TO-DO: using the kafka message StructType, deserialize the JSON from the streaming dataframe 
sparkBankWithdrawalsStreamDF.withColumn("value", from_json("value", bankWithdrawalsSchema)) \
    .select(col("value.*")) \
    .createOrReplaceTempView("BankWithdrawals")
# TO-DO: create a temporary streaming view called "BankWithdrawals" 
# it can later be queried with spark.sql

#TO-DO: using spark.sql, select * from BankWithdrawals into a dataframe
sparkBankWithdrawalsStarStreamDF = spark.sql("select accountNumber, amount, dateAndTime, transactionId as bankTransactionId from BankWithdrawals")
#TO-DO: read the atm-withdrawals kafka topic as a source into a streaming dataframe with the bootstrap server kafka:19092, configuring the stream to read the earliest messages possible                                    
sparkATMWithdrawalsRawStreamDF = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:19092") \
    .option("subscribe","atm-withdrawals")  \
    .option("startingOffsets", "earliest") \
    .load()
#TO-DO: using a select expression on the streaming dataframe, cast the key and the value columns from kafka as strings, and then select them
sparkATMWithdrawalsStreamDF = sparkATMWithdrawalsRawStreamDF.selectExpr("cast(key as string) as key", "cast(value as string) as value")
#TO-DO: using the kafka message StructType, deserialize the JSON from the streaming dataframe 
sparkATMWithdrawalsStreamDF.withColumn("value", from_json("value", atmWithdrawalsSchema)) \
    .select(col("value.*")) \
    .createOrReplaceTempView("ATMWithDrawals")
# TO-DO: create a temporary streaming view called "AtmWithdrawals" 
# it can later be queried with spark.sql

#TO-DO: using spark.sql, select * from AtmWithdrawals into a dataframe
sparkATMWithdrawalsStarStreamDF = spark.sql("select transactionDate, transactionId as atmTransactionId, atmLocation from ATMWithDrawals")
#TO-DO: join the atm withdrawals dataframe with the bank withdrawals dataframe
atmBankWithdrawalsDF = sparkBankWithdrawalsStarStreamDF.join(sparkATMWithdrawalsStarStreamDF, 
    expr("""
         atmTransactionId = bankTransactionId
         """)
    )
# TO-DO: write the stream to the kafka in a topic called withdrawals-location, and configure it to run indefinitely, the console will not output anything. You will want to attach to the topic using the kafka-console-consumer inside another terminal
# TO-DO: for the "checkpointLocation" option in the writeStream, be sure to use a unique file path to avoid conflicts with other spark scripts
atmBankWithdrawalsDF.selectExpr("cast(atmTransactionId as string) as key", 
"to_json(struct(*)) as value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:19092") \
    .option("topic", "withdrawals-location") \
    .option("checkpointLocation", "/tmp/kafkacheckpoint/v2") \
    .start() \
    .awaitTermination()    