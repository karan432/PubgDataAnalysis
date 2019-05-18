from pyspark.sql import SQLContext, Row, SparkSession

spark = SparkSession\
    .builder\
    .appName("streamingPubg")\
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/pubg") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/pubg") \
    .getOrCreate()

csv = spark.read.csv("C:/opt/spark/kafka_2.12-2.2.0/trimmed.csv", header='true')

spark.sparkContext.setLogLevel("WARN")

def writeToMongo(df):
    df.write.format("com.mongodb.spark.sql.DefaultSource")\
        .mode("append")\
        .option("database","pubg")\
        .option("collection", "players")\
        .save()

writeToMongo(csv)